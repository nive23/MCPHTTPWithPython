"""
Azure App Service optimized Salesforce MCP Server
This version includes both get_accounts and create_quote_from_opportunity tools
Optimized for Azure App Service deployment with proper error handling and logging
"""
import time
import sys
import socket
import dns.resolver
import jwt
import requests
from simple_salesforce import Salesforce, SFType
from mcp.server.fastmcp import FastMCP
from typing import List, Dict, Any, Optional
import os

# -------------------------------------------------
# DNS Fix: Use Google DNS (must be FIRST)
# Azure App Service may have DNS resolution issues, this helps
# -------------------------------------------------
_dns_resolver = dns.resolver.Resolver()
_dns_resolver.nameservers = ['8.8.8.8', '8.8.4.4']
_dns_cache = {}

def _resolve_dns(hostname):
    """Resolve DNS using Google DNS for better reliability on Azure"""
    if hostname not in _dns_cache:
        try:
            _dns_cache[hostname] = str(_dns_resolver.resolve(hostname, 'A')[0])
            print(f"[DNS] {hostname} -> {_dns_cache[hostname]}", file=sys.stderr)
        except Exception as e:
            print(f"[DNS ERROR] {hostname}: {e}", file=sys.stderr)
            return None
    return _dns_cache[hostname]

_original_getaddrinfo = socket.getaddrinfo

def _patched_getaddrinfo(host, port, *args, **kwargs):
    """Patch socket.getaddrinfo to use custom DNS resolver for Salesforce"""
    if 'salesforce.com' in str(host):
        ip = _resolve_dns(host)
        if ip:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (ip, port))]
    return _original_getaddrinfo(host, port, *args, **kwargs)

socket.getaddrinfo = _patched_getaddrinfo

# -------------------------------------------------
# Salesforce Configuration (from environment variables)
# Azure App Service sets these via Application Settings
# -------------------------------------------------
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_LOGIN_URL = os.getenv("SF_LOGIN_URL", "https://login.salesforce.com")
SF_PRIVATE_KEY = os.getenv("SF_PRIVATE_KEY")

# Validate required environment variables
if not SF_CLIENT_ID:
    raise ValueError("SF_CLIENT_ID environment variable is required")
if not SF_USERNAME:
    raise ValueError("SF_USERNAME environment variable is required")
if not SF_PRIVATE_KEY:
    raise ValueError("SF_PRIVATE_KEY environment variable is required")

# -------------------------------------------------
# Azure App Service Configuration
# Azure automatically sets PORT environment variable
# -------------------------------------------------
# Get port from environment (Azure App Service sets this automatically)
port = int(os.getenv("PORT", 8000))
host = os.getenv("HOST", "0.0.0.0")

# Initialize FastMCP with host and port upfront
# This is required for streamable-http transport to work properly on Azure
mcp = FastMCP("salesforce-azure", host=host, port=port)

# -------------------------------------------------
# Salesforce Client (Cached - Auth Once)
# Azure App Service keeps the process running, so caching helps
# -------------------------------------------------
_sf_client = None
_auth_time = None

def get_salesforce():
    """Get or create Salesforce client (authenticates only once, re-auths if expired)"""
    global _sf_client, _auth_time
    
    # Re-auth if token is older than 1 hour
    if _sf_client and _auth_time and (time.time() - _auth_time) > 3600:
        print("[SF] Token expired, re-authenticating...", file=sys.stderr)
        _sf_client = None
    
    if _sf_client is None:
        print("[SF] Authenticating with JWT...", file=sys.stderr)
        t = time.time()
        
        try:
            # Create JWT assertion
            payload = {
                "iss": SF_CLIENT_ID,
                "sub": SF_USERNAME,
                "aud": SF_LOGIN_URL,
                "exp": int(time.time()) + 300,
            }
            assertion = jwt.encode(payload, SF_PRIVATE_KEY, algorithm="RS256")
            
            # Get access token
            resp = requests.post(
                f"{SF_LOGIN_URL}/services/oauth2/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
                timeout=15,
            )
            
            if resp.status_code != 200:
                error_msg = f"Auth failed: {resp.status_code} - {resp.text}"
                print(f"[SF ERROR] {error_msg}", file=sys.stderr)
                raise RuntimeError(error_msg)
            
            data = resp.json()
            _sf_client = Salesforce(
                instance_url=data["instance_url"],
                session_id=data["access_token"],
            )
            _auth_time = time.time()
            print(f"[SF] Authenticated successfully in {time.time()-t:.1f}s", file=sys.stderr)
            print(f"[SF] Instance URL: {data['instance_url']}", file=sys.stderr)
            
        except Exception as e:
            print(f"[SF ERROR] Authentication failed: {e}", file=sys.stderr)
            raise
    
    return _sf_client

# -------------------------------------------------
# MCP Server Tools
# -------------------------------------------------

@mcp.tool()
def get_accounts(limit: int = 5) -> List[Dict[str, Any]]:
    """
    Fetch Salesforce Accounts. Returns list of account names and IDs.
    
    Args:
        limit: Maximum number of accounts to return (default: 5, max: 100)
    
    Returns:
        List of dictionaries containing account Id and Name
    """
    try:
        if limit < 1 or limit > 100:
            limit = 5
            print(f"[Accounts] Invalid limit, using default: 5", file=sys.stderr)
        
        print(f"[Accounts] Fetching {limit} accounts...", file=sys.stderr)
        sf = get_salesforce()
        result = sf.query(f"SELECT Id, Name FROM Account LIMIT {limit}")
        
        accounts = result.get("records", [])
        print(f"[Accounts] Found {len(accounts)} accounts", file=sys.stderr)
        
        return accounts
        
    except Exception as e:
        error_msg = f"Error fetching accounts: {str(e)}"
        print(f"[Accounts ERROR] {error_msg}", file=sys.stderr)
        return [{"error": error_msg}]

@mcp.tool()
def create_quote_from_opportunity(opportunity_id: str) -> Dict[str, Any]:
    """
    Create a Standard Quote and Quote Line Items from an Opportunity.
    
    This tool:
    1. Fetches the Opportunity with Account details
    2. Creates a Standard Quote linked to the Opportunity
    3. Creates Quote Line Items from Opportunity Line Items
    4. Returns detailed information about the created quote, opportunity, account, and quote lines
    
    Args:
        opportunity_id: The Salesforce Opportunity ID (required, format: 006XXXXXXXXXXXXXXX)
    
    Returns:
        Dictionary containing:
        - quoteId: The created Quote ID
        - opportunityId: The Opportunity ID
        - opportunityName: The Opportunity Name
        - accountId: The Account ID
        - accountName: The Account Name
        - accountPhone: The Account Phone
        - accountIndustry: The Account Industry
        - quoteLineCount: Number of quote lines created
        - quoteLines: List of quote line details (SKU, list price, sales price, quantity)
        - errorMessage: Error message if any error occurred
    """
    result = {
        "quoteId": None,
        "opportunityId": None,
        "opportunityName": None,
        "accountId": None,
        "accountName": None,
        "accountPhone": None,
        "accountIndustry": None,
        "quoteLineCount": 0,
        "quoteLines": [],
        "errorMessage": None
    }
    
    try:
        if not opportunity_id:
            raise ValueError("Opportunity Id is required")
        
        if not opportunity_id.startswith("006"):
            raise ValueError(f"Invalid Opportunity ID format: {opportunity_id}. Must start with '006'")
        
        sf = get_salesforce()
        
        # ---------------------------------
        # Fetch Opportunity + Account
        # ---------------------------------
        print(f"[Quote] Fetching Opportunity {opportunity_id}...", file=sys.stderr)
        opp_query = f"""
            SELECT Id, Name, AccountId, Account.Name, Account.Phone, Account.Industry, Pricebook2Id
            FROM Opportunity
            WHERE Id = '{opportunity_id}'
            LIMIT 1
        """
        opp_result = sf.query(opp_query)
        
        if not opp_result.get("records"):
            raise ValueError(f"Opportunity with Id {opportunity_id} not found")
        
        opp = opp_result["records"][0]
        
        if not opp.get("Pricebook2Id"):
            raise ValueError("Opportunity must have a Pricebook assigned")
        
        # ---------------------------------
        # Create Standard Quote
        # ---------------------------------
        print(f"[Quote] Creating Quote for Opportunity {opp['Name']}...", file=sys.stderr)
        quote_name = f"{opp['Name']} - Quote"
        
        quote_data = {
            "Name": quote_name,
            "OpportunityId": opp["Id"],
            "Pricebook2Id": opp["Pricebook2Id"]
        }
        
        # Use simple-salesforce SFType to create Quote
        quote_sf = SFType('Quote', sf.session_id, sf.sf_instance)
        quote_result = quote_sf.create(quote_data)
        quote_id = quote_result["id"]
        print(f"[Quote] Created Quote {quote_id}", file=sys.stderr)
        
        # ---------------------------------
        # Populate response (Quote / Opp / Account)
        # ---------------------------------
        result["quoteId"] = quote_id
        result["opportunityId"] = opp["Id"]
        result["opportunityName"] = opp["Name"]
        
        # Handle Account relationship (can be dict or just AccountId)
        account = opp.get("Account")
        if account and isinstance(account, dict):
            result["accountId"] = account.get("Id") or opp.get("AccountId")
            result["accountName"] = account.get("Name")
            result["accountPhone"] = account.get("Phone")
            result["accountIndustry"] = account.get("Industry")
        else:
            result["accountId"] = opp.get("AccountId")
            # Try to fetch account details if AccountId exists
            if result["accountId"]:
                try:
                    account_query = f"SELECT Id, Name, Phone, Industry FROM Account WHERE Id = '{result['accountId']}' LIMIT 1"
                    account_result = sf.query(account_query)
                    if account_result.get("records"):
                        acc = account_result["records"][0]
                        result["accountName"] = acc.get("Name")
                        result["accountPhone"] = acc.get("Phone")
                        result["accountIndustry"] = acc.get("Industry")
                except Exception as e:
                    print(f"[Quote] Warning: Could not fetch Account details: {e}", file=sys.stderr)
        
        # ---------------------------------
        # Fetch Opportunity Line Items
        # ---------------------------------
        print(f"[Quote] Fetching Opportunity Line Items...", file=sys.stderr)
        oli_query = f"""
            SELECT Id, Quantity, UnitPrice, PricebookEntryId, 
                   PricebookEntry.UnitPrice, Product2.SKU__c
            FROM OpportunityLineItem
            WHERE OpportunityId = '{opp["Id"]}'
        """
        oli_result = sf.query(oli_query)
        
        if oli_result.get("records"):
            print(f"[Quote] Found {len(oli_result['records'])} Opportunity Line Items", file=sys.stderr)
            
            quote_lines = []
            line_responses = []
            
            for oli in oli_result["records"]:
                # Get list price from PricebookEntry
                list_price = None
                if oli.get("PricebookEntry") and isinstance(oli["PricebookEntry"], dict):
                    list_price = oli["PricebookEntry"].get("UnitPrice")
                
                # Sales price is the UnitPrice on the OLI
                sales_price = oli.get("UnitPrice", 0)
                quantity = oli.get("Quantity", 0)
                pricebook_entry_id = oli.get("PricebookEntryId")
                
                if not pricebook_entry_id:
                    print(f"[Quote] Warning: OLI {oli.get('Id')} has no PricebookEntryId, skipping", file=sys.stderr)
                    continue
                
                # Create Quote Line Item
                qli_data = {
                    "QuoteId": quote_id,
                    "PricebookEntryId": pricebook_entry_id,
                    "Quantity": quantity,
                    "UnitPrice": sales_price
                }
                
                # Use simple-salesforce SFType to create QuoteLineItem
                qli_sf = SFType('QuoteLineItem', sf.session_id, sf.sf_instance)
                qli_result = qli_sf.create(qli_data)
                quote_lines.append(qli_result["id"])
                
                # Quote Line response
                line_response = {
                    "skuId": None,
                    "listPrice": float(list_price) if list_price else None,
                    "salesPrice": float(sales_price) if sales_price else None,
                    "quantity": float(quantity) if quantity else None
                }
                
                # Get SKU from Product2 if available
                if oli.get("Product2") and isinstance(oli["Product2"], dict):
                    line_response["skuId"] = oli["Product2"].get("SKU__c")
                
                line_responses.append(line_response)
            
            result["quoteLineCount"] = len(quote_lines)
            result["quoteLines"] = line_responses
            print(f"[Quote] Created {len(quote_lines)} Quote Line Items", file=sys.stderr)
        else:
            print(f"[Quote] No Opportunity Line Items found", file=sys.stderr)
        
        print(f"[Quote] Successfully created Quote {quote_id}", file=sys.stderr)
        
    except Exception as e:
        error_msg = str(e)
        result["errorMessage"] = error_msg
        print(f"[Quote ERROR] {error_msg}", file=sys.stderr)
    
    return result

# -------------------------------------------------
# Entry Point - HTTP/SSE Server for Azure App Service
# -------------------------------------------------
if __name__ == "__main__":
    print("=" * 60, file=sys.stderr)
    print("Salesforce MCP Server - Azure App Service", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Starting server on {host}:{port}...", file=sys.stderr)
    print(f"Server URL: http://{host}:{port}", file=sys.stderr)
    print(f"Environment: Azure App Service", file=sys.stderr)
    print(f"Transport: streamable-http", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    # Validate environment variables on startup
    print("[Config] Validating environment variables...", file=sys.stderr)
    print(f"[Config] SF_CLIENT_ID: {'✓ Set' if SF_CLIENT_ID else '✗ Missing'}", file=sys.stderr)
    print(f"[Config] SF_USERNAME: {'✓ Set' if SF_USERNAME else '✗ Missing'}", file=sys.stderr)
    print(f"[Config] SF_PRIVATE_KEY: {'✓ Set' if SF_PRIVATE_KEY else '✗ Missing'}", file=sys.stderr)
    print(f"[Config] SF_LOGIN_URL: {SF_LOGIN_URL}", file=sys.stderr)
    print(f"[Config] PORT: {port}", file=sys.stderr)
    print(f"[Config] HOST: {host}", file=sys.stderr)
    
    # Run with streamable-http transport
    # This is the recommended transport for Azure App Service
    # Host and port are set during FastMCP initialization above
    try:
        mcp.run(transport="streamable-http")
    except Exception as e:
        print(f"[FATAL ERROR] Failed to start server: {e}", file=sys.stderr)
        sys.exit(1)

