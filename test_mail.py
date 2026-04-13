"""
BIM Infra Solutions — Zoho Mail Connection Test
Run: py test_mail.py
"""
import os
import sys
import json
import requests
from dotenv import load_dotenv

load_dotenv(override=True)

CLIENT_ID     = os.getenv("ZOHO_CLIENT_ID")
CLIENT_SECRET = os.getenv("ZOHO_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("ZOHO_REFRESH_TOKEN")
DC            = os.getenv("ZOHO_DC", "in")
ACCOUNT_ID    = os.getenv("ZOHO_MAIL_ACCOUNT_ID")
SENDER_EMAIL  = "kishan.batavia@biminfrasolutions.in"

print("=" * 55)
print("  BIM Infra Solutions — Zoho Mail Test")
print("=" * 55)

# ── TEST 1: Check .env values ──────────────────────────────
print("\n[TEST 1] Checking .env values...")
missing = []
if not CLIENT_ID:     missing.append("ZOHO_CLIENT_ID")
if not CLIENT_SECRET: missing.append("ZOHO_CLIENT_SECRET")
if not REFRESH_TOKEN: missing.append("ZOHO_REFRESH_TOKEN")
if not ACCOUNT_ID:    missing.append("ZOHO_MAIL_ACCOUNT_ID")

if missing:
    print(f"  FAIL — Missing: {', '.join(missing)}")
    sys.exit(1)

print(f"  PASS — CLIENT_ID      : {CLIENT_ID[:12]}...")
print(f"  PASS — CLIENT_SECRET  : {CLIENT_SECRET[:6]}...")
print(f"  PASS — REFRESH_TOKEN  : {REFRESH_TOKEN[:20]}...")
print(f"  PASS — ACCOUNT_ID     : {ACCOUNT_ID}")
print(f"  PASS — DC             : {DC}")

# ── TEST 2: Refresh Access Token ───────────────────────────
print("\n[TEST 2] Refreshing access token...")
try:
    r = requests.post(
        f"https://accounts.zoho.{DC}/oauth/v2/token",
        params={
            "grant_type"   : "refresh_token",
            "client_id"    : CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "refresh_token": REFRESH_TOKEN,
        },
        timeout=10
    )
    data = r.json()
    if "access_token" not in data:
        print(f"  FAIL — Zoho response: {data}")
        print("\n  FIX: Generate a new Grant Code at https://api-console.zoho.in")
        print("       Then run: py C:\\Users\\Kishan\\get_token.py")
        sys.exit(1)

    access_token = data["access_token"]
    print(f"  PASS — Access token  : {access_token[:20]}...")
    print(f"  PASS — Expires in    : {data.get('expires_in', '?')} seconds")

except Exception as e:
    print(f"  FAIL — {e}")
    sys.exit(1)

# ── TEST 3: Verify Mail Account ────────────────────────────
print("\n[TEST 3] Verifying Zoho Mail account...")
try:
    r = requests.get(
        f"https://mail.zoho.{DC}/api/accounts",
        headers={"Authorization": f"Zoho-oauthtoken {access_token}"},
        timeout=10
    )
    accounts = r.json().get("data", [])
    if not accounts:
        print(f"  FAIL — No mail accounts found. Response: {r.json()}")
        sys.exit(1)

    found = None
    for acc in accounts:
        print(f"  Found account: {acc.get('emailAddress')} (ID: {acc.get('accountId')})")
        if str(acc.get("accountId")) == str(ACCOUNT_ID):
            found = acc

    if found:
        print(f"  PASS — Account ID {ACCOUNT_ID} matched!")
    else:
        print(f"  WARN — Account ID {ACCOUNT_ID} not in list above.")
        print(f"         Update ZOHO_MAIL_ACCOUNT_ID in .env with correct ID.")

except Exception as e:
    print(f"  FAIL — {e}")
    sys.exit(1)

# ── TEST 4: Send Test Email ────────────────────────────────
print(f"\n[TEST 4] Sending test email to {SENDER_EMAIL}...")
try:
    payload = {
        "fromAddress": SENDER_EMAIL,
        "toAddress"  : SENDER_EMAIL,
        "subject"    : "BIM CRM — Mail Test Successful",
        "content"    : """
        <div style="font-family:Arial;padding:20px;max-width:500px;">
            <div style="background:#1B3A6B;padding:15px;text-align:center;border-radius:6px;">
                <h2 style="color:white;margin:0;">BIM Infra Solutions</h2>
            </div>
            <div style="padding:20px;">
                <h3 style="color:#1B3A6B;">Zoho Mail is working!</h3>
                <p>Your CRM email system is connected and ready to send outreach emails.</p>
                <p style="color:#888;font-size:12px;">Sent from BIM Infra CRM — test_mail.py</p>
            </div>
        </div>
        """,
        "mailFormat" : "html",
    }

    r = requests.post(
        f"https://mail.zoho.{DC}/api/accounts/{ACCOUNT_ID}/messages",
        headers={
            "Authorization": f"Zoho-oauthtoken {access_token}",
            "Content-Type" : "application/json",
        },
        data=json.dumps(payload),
        timeout=15
    )

    if r.status_code in (200, 201):
        print(f"  PASS — Email sent! Check {SENDER_EMAIL} inbox.")
    else:
        print(f"  FAIL — HTTP {r.status_code}: {r.text}")
        sys.exit(1)

except Exception as e:
    print(f"  FAIL — {e}")
    sys.exit(1)

# ── RESULT ─────────────────────────────────────────────────
print("\n" + "=" * 55)
print("  ALL TESTS PASSED — Zoho Mail is fully working!")
print("=" * 55)
print(f"\n  Check your inbox: {SENDER_EMAIL}")
print("  You can now send emails from the CRM.\n")
