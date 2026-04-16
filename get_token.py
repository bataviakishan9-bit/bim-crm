"""
BIM CRM — Zoho Refresh Token Generator
Run this whenever you need a new refresh token (e.g. after re-authorising in Zoho API Console).

Steps:
  1. Go to https://api-console.zoho.in/
  2. Open your Self Client app
  3. Click "Generate Code"
  4. Scopes:  ZohoMail.messages.ALL,ZohoMail.accounts.READ
  5. Expiry:  10 minutes
  6. Copy the authorization code shown
  7. Run this script and paste it when prompted
  8. The new refresh token is saved to .env automatically
"""

import os
import sys
import requests
from dotenv import load_dotenv, set_key

load_dotenv()

CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID",     "1000.V1GB0ZULJ3A8J68N57IQSOPU5P6N0P")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET", "74371811950b55ccbf5ab82fa31bdfd75168b6c183")
DC            = os.getenv("ZOHO_DC",            "in")

ENV_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

print("\n" + "=" * 60)
print("  BIM CRM — Zoho Token Refresh")
print("=" * 60)
print(f"\n  Client ID : {CLIENT_ID[:20]}...")
print(f"  DC        : {DC}")
print()

# ── Step 1: Test if current refresh token still works ────────────────────────
current_rt = os.getenv("ZOHO_REFRESH_TOKEN", "")
if current_rt:
    print("Checking current refresh token...")
    r = requests.post(
        f"https://accounts.zoho.{DC}/oauth/v2/token",
        params={
            "grant_type"   : "refresh_token",
            "client_id"    : CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": current_rt,
        },
        timeout=10,
    )
    data = r.json()
    if "access_token" in data:
        print(f"\n  [OK]  Current refresh token is VALID")
        print(f"     Access token: {data['access_token'][:40]}...")
        print()
        proceed = input("  Current token works. Re-generate anyway? [y/N]: ").strip().lower()
        if proceed != "y":
            print("\n  Nothing changed. Your existing token is fine.\n")
            sys.exit(0)
    else:
        print(f"\n  [FAIL]  Current token FAILED: {data}")
        print("     Proceeding to get a new one...\n")

# ── Step 2: Guide user to get auth code ─────────────────────────────────────
print()
print("─" * 60)
print("  FOLLOW THESE STEPS IN YOUR BROWSER:")
print("─" * 60)
print()
print("  1. Open: https://api-console.zoho.in/")
print("  2. Click your Self Client app")
print("  3. Click 'Generate Code'")
print("  4. Scopes: ZohoMail.messages.ALL,ZohoMail.accounts.READ")
print("  5. Expiry: 10 minutes")
print("  6. Copy the code shown")
print()

auth_code = input("  Paste the authorization code here: ").strip()
if not auth_code:
    print("\n  No code entered. Exiting.\n")
    sys.exit(1)

# ── Step 3: Exchange code for tokens ────────────────────────────────────────
print("\n  Exchanging code for tokens...")
r = requests.post(
    f"https://accounts.zoho.{DC}/oauth/v2/token",
    params={
        "grant_type"  : "authorization_code",
        "client_id"   : CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "code"        : auth_code,
    },
    timeout=10,
)
data = r.json()

if "refresh_token" not in data:
    print(f"\n  ❌  Token exchange failed:")
    import json
    print("  ", json.dumps(data, indent=4))
    print()
    print("  Common causes:")
    print("    - Code already used (each code works once)")
    print("    - Code expired (use within 10 minutes)")
    print("    - Wrong scopes — check the Zoho console")
    sys.exit(1)

new_refresh_token = data["refresh_token"]
new_access_token  = data.get("access_token", "")

print(f"\n  [OK]  New refresh token obtained!")
print(f"     Refresh: {new_refresh_token[:40]}...")
if new_access_token:
    print(f"     Access : {new_access_token[:40]}...")

# ── Step 4: Update .env file ─────────────────────────────────────────────────
print(f"\n  Saving to {ENV_PATH}...")
set_key(ENV_PATH, "ZOHO_REFRESH_TOKEN", new_refresh_token)
print("  [OK]  .env updated")

# ── Step 5: Print Render instructions ───────────────────────────────────────
print()
print("─" * 60)
print("  IMPORTANT — UPDATE RENDER ENVIRONMENT VARIABLE:")
print("─" * 60)
print()
print("  1. Go to https://dashboard.render.com/")
print("  2. Open your bim-crm service")
print("  3. Go to  Environment  tab")
print("  4. Find  ZOHO_REFRESH_TOKEN  and update it to:")
print()
print(f"     {new_refresh_token}")
print()
print("  5. Click Save → service will auto-redeploy")
print()
print("=" * 60)
print("  Done! Token is ready.")
print("=" * 60)
print()
