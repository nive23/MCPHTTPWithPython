"""
HTTP/SSE version of Salesforce MCP Server (Combined)
This version includes both get_accounts and create_quote_from_opportunity tools
Can be hosted on Render and accessed via HTTP
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
# -------------------------------------------------
_dns_resolver = dns.resolver.Resolver()
_dns_resolver.nameservers = ['8.8.8.8', '8.8.4.4']
_dns_cache = {}

def _resolve_dns(hostname):
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
    if 'salesforce.com' in str(host):
        ip = _resolve_dns(host)
        if ip:
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, '', (ip, port))]
    return _original_getaddrinfo(host, port, *args, **kwargs)

socket.getaddrinfo = _patched_getaddrinfo

# -------------------------------------------------
# Salesforce Configuration (from environment or defaults)
# -------------------------------------------------
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID", "3MVG9kb26yEQGZW2QFfTr6bIpqQAwUwBLo.8X_y7DMe2eLZPFM2e0A37ygvPKF.HgnnKsFdrtRU3hiYtx.B2Y")
SF_USERNAME = os.getenv("SF_USERNAME", "anandbts79@gmail.com2025/sdragent.demo")
SF_LOGIN_URL = os.getenv("SF_LOGIN_URL", "https://login.salesforce.com")
SF_PRIVATE_KEY = os.getenv("SF_PRIVATE_KEY", """-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDUuq8n0BvKbeRBU1IjtYiNNw+GBxGYW8eZfnf6vuwCX5zaTW443pjG+/zTkKbMYk2nUZUl/70+JfelK+350PVzKSD3f4ieeeVwuvY7lEFN6mjUXWAUk26aWGsIgwWwzisJWXuXVaWyVaKh7lbpEDJM63DfnprvBsoylixM85WqKvaARkT5tFwkW4UdTCpZTU+w5BRFPlWhqKnkEhMxhNJNXwVI56sH3NvVH1M4fU4pvE/bJLe21VI2E+E+KivihuZrECGsiqu8HYyeAv+s7Dy3Rc3NIKLZGobvwcaj8UIKkvtqyRFo+KCjEI73sLrPO0nd0pKn8N4Ax7AnykWKxdD/AgMBAAECggEAF7v8buS5NQytutwg/frzEU5jXQDM5cLXsjh6Cm/ixSEFzxV0hsPPJ+RVgQVSltsg7tobWfH+m0CBMNyF4Kl/uSmNlA+WnpfbEYjKkDsqThcpOwhv/9uzK2VSC9ESgXd/NbvWHjxgEFWdn5OH/tjfw9VA8rIvdxu604oNx0gmGqYcT/vd6F58q7HYgNkWTvklh35yYAxRO/SX1UngkktyVnwYlt3JuuqrGZajhvgaUx3HpsaaUdKm8SjTgngn9+7DkqTwTkQ1YGiUa+BEg9YMkvuBUGohJnjla+duRV1r/PrJWy9nG7wb87HvD49UZL6Km+FJ5JIs1+07uUXdWNwsAQKBgQD3seG60WlWaPVmKZPlapEBevt9ktfbs5/ZJIul/DrSFYuLZmWI5UeGEImZZIQE6AEYKtwiyiBjZvOjdKFu+0tCe7MRCh/IinN1okOg/d3DnSNY17Fn1T04vsAP5K8J//bRRyeGddYIXsoWFpkCUnZoFfVW+pCudFvDwwwTIDJgnwKBgQDb3KvBd2dqNQJDROO2XatXtgynjgjh1S7GUavAzuqb4Q3ESMwOOWWCbtE0lxFUEUXDkqfF7NATFka63vj3t/FLYMf0g3MpnUoheICd1wTviwHQ+GUXs/fHBWxZ3sd+QmPs3vumkY1RsQX0oCswOOKYCG2OYMT3vM3ThJ4JAgzToQKBgQCRh0Cxy6HRNB+iY6FSdDc4IHKsR99tZO7w3Ijz/+rrTd9MCuBy+wr32LWPmz/5xfoGof/urMU0weM50SecFicUq6r8wu8Dm1zU635Ck4V9DdEbvLat49pxgZlEfT7eaDYypVSyqn8TMeYX7jT96UdKRkR8UwE4joLv1KQ+hHc0zwKBgDwwI+DZpDjh/BWYRVJGQELJtpsj+fCA4MYv03n76yPzL0aduybltZFIbwMbnAXMmLGac01ur+OZxJEhuzxtYaQGAZdBaQRqZ8HT8DnFhDdjcYLYjSw4+0rDhE7x+uDoodxVisSlse88K+o08r3HxNhj7kH84c8EI8CU3IKQyhwhAoGBAJhbQiGxHqbuqhWbut2pcHBXnuYvQ4OKB+7Y3enTlSjVARSVPz8l6QQZ2ZYChC0MEwaVnUzE01pGSbtQKRWY+jWcO+6BjR70cOgJ0+CkJ7kM2ILtYKo5Zn+beEj/J09zOQDHKthIDD71OGYGDXuWXAE90WYYhk8e8lwZN1ISj/MK
-----END PRIVATE KEY-----""")

# -------------------------------------------------
# Salesforce Client (Cached - Auth Once)
# -------------------------------------------------
_sf_client = None
_auth_time = None

def get_salesforce():
    """Get or create Salesforce client (authenticates only once)"""
    global _sf_client, _auth_time
    
    # Re-auth if token is older than 1 hour
    if _sf_client and _auth_time and (time.time() - _auth_time) > 3600:
        print("[SF] Token expired, re-authenticating...", file=sys.stderr)
        _sf_client = None
    
    if _sf_client is None:
        print("[SF] Authenticating with JWT...", file=sys.stderr)
        t = time.time()
        
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
            raise RuntimeError(f"Auth failed: {resp.text}")
        
        data = resp.json()
        _sf_client = Salesforce(
            instance_url=data["instance_url"],
            session_id=data["access_token"],
        )
        _auth_time = time.time()
        print(f"[SF] Authenticated in {time.time()-t:.1f}s", file=sys.stderr)
    
    return _sf_client

# -------------------------------------------------
# MCP Server & Tools
# -------------------------------------------------
# Get host and port from environment (Render sets PORT automatically)
port = int(os.getenv("PORT", 8000))
host = os.getenv("HOST", "0.0.0.0")

# Initialize FastMCP - host and port must be set during initialization
# for streamable-http transport to work
mcp = FastMCP("salesforce", host=host, port=port)

@mcp.tool()
def get_accounts(limit: int = 5):
    """Fetch Salesforce Accounts. Returns list of account names and IDs."""
    sf = get_salesforce()
    result = sf.query(f"SELECT Id, Name FROM Account LIMIT {limit}")
    return result["records"]

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
        opportunity_id: The Salesforce Opportunity ID (required)
    
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
        
        if not opp_result["records"]:
            raise ValueError(f"Opportunity with Id {opportunity_id} not found")
        
        opp = opp_result["records"][0]
        
        if not opp.get("Pricebook2Id"):
            raise ValueError("Opportunity must have a Pricebook")
        
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
                    if account_result["records"]:
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
        
        if oli_result["records"]:
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
        print(f"[Quote] Error: {error_msg}", file=sys.stderr)
    
    return result

# -------------------------------------------------
# Entry Point - HTTP/SSE Server
# -------------------------------------------------
if __name__ == "__main__":
    print(f"Starting Salesforce MCP server on {host}:{port}...", file=sys.stderr)
    print(f"Access the server at: http://{host}:{port}", file=sys.stderr)
    
    # Run with streamable-http transport
    # Host and port are set during FastMCP initialization above
    mcp.run(transport="streamable-http")

