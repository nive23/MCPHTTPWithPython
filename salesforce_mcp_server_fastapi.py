"""
FastAPI-based Salesforce MCP Server for Azure App Service
Uses FastAPI to properly implement MCP protocol endpoints
This should work reliably with Claude.ai
"""
import time
import sys
import socket
import dns.resolver
import jwt
import requests
from simple_salesforce import Salesforce, SFType
from typing import List, Dict, Any, Optional
import os
import json
from fastapi import FastAPI, Request, Response
from fastapi.responses import StreamingResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
import asyncio

# -------------------------------------------------
# DNS Fix: Use Google DNS
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
# Salesforce Configuration
# -------------------------------------------------
SF_CLIENT_ID = os.getenv("SF_CLIENT_ID")
SF_USERNAME = os.getenv("SF_USERNAME")
SF_LOGIN_URL = os.getenv("SF_LOGIN_URL", "https://login.salesforce.com")
SF_PRIVATE_KEY = os.getenv("SF_PRIVATE_KEY")

if not SF_CLIENT_ID:
    raise ValueError("SF_CLIENT_ID environment variable is required")
if not SF_USERNAME:
    raise ValueError("SF_USERNAME environment variable is required")
if not SF_PRIVATE_KEY:
    raise ValueError("SF_PRIVATE_KEY environment variable is required")

# -------------------------------------------------
# Azure Configuration
# -------------------------------------------------
port = int(os.getenv("PORT", 8000))
host = os.getenv("HOST", "0.0.0.0")

# -------------------------------------------------
# FastAPI App
# -------------------------------------------------
app = FastAPI(title="Salesforce MCP Server")

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    print(f"[REQUEST] {request.method} {request.url.path}", file=sys.stderr)
    response = await call_next(request)
    print(f"[RESPONSE] {request.method} {request.url.path} -> {response.status_code}", file=sys.stderr)
    return response

# -------------------------------------------------
# Salesforce Client
# -------------------------------------------------
_sf_client = None
_auth_time = None

def get_salesforce():
    """Get or create Salesforce client"""
    global _sf_client, _auth_time
    
    if _sf_client and _auth_time and (time.time() - _auth_time) > 3600:
        print("[SF] Token expired, re-authenticating...", file=sys.stderr)
        _sf_client = None
    
    if _sf_client is None:
        print("[SF] Authenticating with JWT...", file=sys.stderr)
        t = time.time()
        
        try:
            payload = {
                "iss": SF_CLIENT_ID,
                "sub": SF_USERNAME,
                "aud": SF_LOGIN_URL,
                "exp": int(time.time()) + 300,
            }
            assertion = jwt.encode(payload, SF_PRIVATE_KEY, algorithm="RS256")
            
            resp = requests.post(
                f"{SF_LOGIN_URL}/services/oauth2/token",
                data={
                    "grant_type": "urn:ietf:params:oauth:grant-type:jwt-bearer",
                    "assertion": assertion,
                },
                timeout=15,
            )
            
            if resp.status_code != 200:
                raise RuntimeError(f"Auth failed: {resp.status_code} - {resp.text}")
            
            data = resp.json()
            _sf_client = Salesforce(
                instance_url=data["instance_url"],
                session_id=data["access_token"],
            )
            _auth_time = time.time()
            print(f"[SF] Authenticated in {time.time()-t:.1f}s", file=sys.stderr)
        except Exception as e:
            print(f"[SF ERROR] Authentication failed: {e}", file=sys.stderr)
            raise
    
    return _sf_client

# -------------------------------------------------
# MCP Protocol Endpoints
# -------------------------------------------------

@app.get("/")
async def root():
    """Root endpoint - Health check"""
    return {
        "status": "ok",
        "server": "salesforce-azure",
        "version": "1.0.0",
        "protocol": "MCP"
    }

@app.get("/sse")
async def sse_endpoint(request: Request):
    """SSE endpoint for MCP protocol"""
    async def event_stream():
        # Send initial connection message
        yield f"data: {json.dumps({'type': 'connection', 'status': 'connected'})}\n\n"
        
        # Keep connection alive
        while True:
            await asyncio.sleep(30)
            yield f"data: {json.dumps({'type': 'ping'})}\n\n"
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no"
        }
    )

@app.post("/register")
async def register():
    """MCP server registration endpoint"""
    print("[MCP] Register endpoint called", file=sys.stderr)
    return {
        "status": "registered",
        "server": "salesforce-azure",
        "version": "1.0.0"
    }

@app.get("/.well-known/oauth-protected-resource")
async def oauth_protected_resource():
    """OAuth protected resource discovery"""
    print("[MCP] OAuth protected resource endpoint called", file=sys.stderr)
    return {
        "resource": "salesforce-azure",
        "scopes_supported": []
    }

@app.get("/.well-known/oauth-authorization-server")
async def oauth_authorization_server():
    """OAuth authorization server discovery"""
    print("[MCP] OAuth authorization server endpoint called", file=sys.stderr)
    return {
        "issuer": "salesforce-azure",
        "authorization_endpoint": None,
        "token_endpoint": None
    }

@app.post("/")
@app.post("")
async def mcp_request(request: Request):
    """Handle MCP protocol requests"""
    print(f"[MCP] POST / received", file=sys.stderr)
    try:
        # Try to get JSON body
        try:
            body = await request.json()
        except Exception as json_error:
            print(f"[MCP ERROR] Failed to parse JSON: {json_error}", file=sys.stderr)
            return JSONResponse(
                status_code=400,
                content={
                    "jsonrpc": "2.0",
                    "error": {
                        "code": -32700,
                        "message": "Parse error"
                    }
                }
            )
        
        method = body.get("method")
        params = body.get("params", {})
        request_id = body.get("id")
        
        print(f"[MCP] Received request: {method} (id: {request_id})", file=sys.stderr)
        
        if method == "initialize":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "protocolVersion": "2024-11-05",
                    "capabilities": {
                        "tools": {}
                    },
                    "serverInfo": {
                        "name": "salesforce-azure",
                        "version": "1.0.0"
                    }
                }
            }
        
        elif method == "tools/list":
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "result": {
                    "tools": [
                        {
                            "name": "get_accounts",
                            "description": "Fetch Salesforce Accounts. Returns list of account names and IDs.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "limit": {
                                        "type": "integer",
                                        "description": "Maximum number of accounts to return (default: 5, max: 100)",
                                        "default": 5
                                    }
                                }
                            }
                        },
                        {
                            "name": "create_quote_from_opportunity",
                            "description": "Create a Standard Quote and Quote Line Items from an Opportunity.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "opportunity_id": {
                                        "type": "string",
                                        "description": "The Salesforce Opportunity ID (required)"
                                    }
                                },
                                "required": ["opportunity_id"]
                            }
                        }
                    ]
                }
            }
        
        elif method == "tools/call":
            tool_name = params.get("name")
            arguments = params.get("arguments", {})
            
            if tool_name == "get_accounts":
                limit = arguments.get("limit", 5)
                if limit < 1 or limit > 100:
                    limit = 5
                
                print(f"[Accounts] Fetching {limit} accounts...", file=sys.stderr)
                sf = get_salesforce()
                result = sf.query(f"SELECT Id, Name FROM Account LIMIT {limit}")
                accounts = result.get("records", [])
                
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(accounts, indent=2)
                            }
                        ]
                    }
                }
            
            elif tool_name == "create_quote_from_opportunity":
                opportunity_id = arguments.get("opportunity_id")
                
                if not opportunity_id:
                    raise ValueError("Opportunity Id is required")
                
                # Use the same quote creation logic from the original file
                result = create_quote_logic(opportunity_id)
                
                return {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "result": {
                        "content": [
                            {
                                "type": "text",
                                "text": json.dumps(result, indent=2)
                            }
                        ]
                    }
                }
            
            else:
                raise ValueError(f"Unknown tool: {tool_name}")
        
        else:
            return {
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32601,
                    "message": f"Method not found: {method}"
                }
            }
    
    except Exception as e:
        error_msg = str(e)
        import traceback
        print(f"[MCP ERROR] {error_msg}", file=sys.stderr)
        traceback.print_exc(file=sys.stderr)
        request_id = None
        if 'body' in locals():
            request_id = body.get("id")
        return JSONResponse(
            status_code=200,
            content={
                "jsonrpc": "2.0",
                "id": request_id,
                "error": {
                    "code": -32603,
                    "message": error_msg
                }
            }
        )

def create_quote_logic(opportunity_id: str) -> Dict[str, Any]:
    """Create quote logic (same as original)"""
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
        if not opportunity_id.startswith("006"):
            raise ValueError(f"Invalid Opportunity ID format: {opportunity_id}")
        
        sf = get_salesforce()
        
        # Fetch Opportunity
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
        
        # Create Quote
        quote_name = f"{opp['Name']} - Quote"
        quote_data = {
            "Name": quote_name,
            "OpportunityId": opp["Id"],
            "Pricebook2Id": opp["Pricebook2Id"]
        }
        
        quote_sf = SFType('Quote', sf.session_id, sf.sf_instance)
        quote_result = quote_sf.create(quote_data)
        quote_id = quote_result["id"]
        
        result["quoteId"] = quote_id
        result["opportunityId"] = opp["Id"]
        result["opportunityName"] = opp["Name"]
        
        # Handle Account
        account = opp.get("Account")
        if account and isinstance(account, dict):
            result["accountId"] = account.get("Id") or opp.get("AccountId")
            result["accountName"] = account.get("Name")
            result["accountPhone"] = account.get("Phone")
            result["accountIndustry"] = account.get("Industry")
        else:
            result["accountId"] = opp.get("AccountId")
        
        # Fetch and create Quote Line Items
        oli_query = f"""
            SELECT Id, Quantity, UnitPrice, PricebookEntryId, 
                   PricebookEntry.UnitPrice, Product2.SKU__c
            FROM OpportunityLineItem
            WHERE OpportunityId = '{opp["Id"]}'
        """
        oli_result = sf.query(oli_query)
        
        if oli_result.get("records"):
            for oli in oli_result["records"]:
                pricebook_entry_id = oli.get("PricebookEntryId")
                if not pricebook_entry_id:
                    continue
                
                qli_data = {
                    "QuoteId": quote_id,
                    "PricebookEntryId": pricebook_entry_id,
                    "Quantity": oli.get("Quantity", 0),
                    "UnitPrice": oli.get("UnitPrice", 0)
                }
                
                qli_sf = SFType('QuoteLineItem', sf.session_id, sf.sf_instance)
                qli_sf.create(qli_data)
                
                result["quoteLineCount"] += 1
        
    except Exception as e:
        result["errorMessage"] = str(e)
        print(f"[Quote ERROR] {e}", file=sys.stderr)
    
    return result

# -------------------------------------------------
# Entry Point
# -------------------------------------------------
if __name__ == "__main__":
    print("=" * 60, file=sys.stderr)
    print("Salesforce MCP Server - FastAPI (Azure)", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    print(f"Starting server on {host}:{port}...", file=sys.stderr)
    print(f"Server URL: http://{host}:{port}", file=sys.stderr)
    print(f"Environment: Azure App Service", file=sys.stderr)
    print(f"Framework: FastAPI with MCP protocol", file=sys.stderr)
    print("=" * 60, file=sys.stderr)
    
    print("[Config] Validating environment variables...", file=sys.stderr)
    print(f"[Config] SF_CLIENT_ID: {'✓ Set' if SF_CLIENT_ID else '✗ Missing'}", file=sys.stderr)
    print(f"[Config] SF_USERNAME: {'✓ Set' if SF_USERNAME else '✗ Missing'}", file=sys.stderr)
    print(f"[Config] SF_PRIVATE_KEY: {'✓ Set' if SF_PRIVATE_KEY else '✗ Missing'}", file=sys.stderr)
    print(f"[Config] SF_LOGIN_URL: {SF_LOGIN_URL}", file=sys.stderr)
    print(f"[Config] PORT: {port}", file=sys.stderr)
    print(f"[Config] HOST: {host}", file=sys.stderr)
    
    uvicorn.run(app, host=host, port=port, log_level="info")

