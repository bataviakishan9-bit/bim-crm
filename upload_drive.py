"""
BIM Infra Solutions — Upload Portfolio PDF to Google Drive
=========================================================
Run: py upload_drive.py
Requires: google_credentials.json in the same folder
"""
import os
import json

CREDENTIALS_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "google_credentials.json")
PDF_PATH         = os.path.join(os.path.dirname(os.path.abspath(__file__)), "BIMINFRA_PORTFOLIO.pdf")
ENV_PATH         = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env")

print("=" * 55)
print("  BIM Infra — Google Drive PDF Upload")
print("=" * 55)

# Check files exist
if not os.path.exists(CREDENTIALS_FILE):
    print("\n  ERROR: google_credentials.json not found!")
    print("  Steps:")
    print("  1. Go to https://console.cloud.google.com")
    print("  2. APIs & Services → Credentials → Create OAuth 2.0 Client ID")
    print("  3. Type: Desktop App → Download JSON")
    print(f"  4. Save as: {CREDENTIALS_FILE}")
    input("\nPress Enter to exit.")
    exit(1)

if not os.path.exists(PDF_PATH):
    print(f"\n  ERROR: PDF not found at {PDF_PATH}")
    input("\nPress Enter to exit.")
    exit(1)

# Install google-auth if needed
try:
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload
except ImportError:
    print("\nInstalling Google Drive libraries...")
    os.system("py -m pip install google-auth google-auth-oauthlib google-auth-httplib2 google-api-python-client")
    from google.oauth2.credentials import Credentials
    from google_auth_oauthlib.flow import InstalledAppFlow
    from google.auth.transport.requests import Request
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaFileUpload

SCOPES      = ["https://www.googleapis.com/auth/drive.file"]
TOKEN_FILE  = os.path.join(os.path.dirname(os.path.abspath(__file__)), "google_token.json")

# Auth flow
creds = None
if os.path.exists(TOKEN_FILE):
    creds = Credentials.from_authorized_user_file(TOKEN_FILE, SCOPES)

if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        print("\nOpening browser for Google login...")
        flow  = InstalledAppFlow.from_client_secrets_file(CREDENTIALS_FILE, SCOPES)
        creds = flow.run_local_server(port=0)
    with open(TOKEN_FILE, "w") as f:
        f.write(creds.to_json())

print("\n  Google Auth: OK")

# Upload PDF
service    = build("drive", "v3", credentials=creds)
pdf_size   = os.path.getsize(PDF_PATH) / 1024 / 1024

print(f"  Uploading PDF ({pdf_size:.1f} MB)...")

file_metadata = {
    "name"    : "BIM_InfraSolutions_Portfolio_2025.pdf",
    "mimeType": "application/pdf",
}

media = MediaFileUpload(PDF_PATH, mimetype="application/pdf", resumable=True)
file  = service.files().create(
    body=file_metadata,
    media_body=media,
    fields="id, name, webViewLink, webContentLink"
).execute()

file_id = file.get("id")
print(f"  Uploaded: {file.get('name')} (ID: {file_id})")

# Make it public (anyone with link can view)
print("  Setting public permissions...")
service.permissions().create(
    fileId=file_id,
    body={"type": "anyone", "role": "reader"},
).execute()

# Get shareable links
view_link     = f"https://drive.google.com/file/d/{file_id}/view"
download_link = f"https://drive.google.com/uc?export=download&id={file_id}"

print(f"\n  View link     : {view_link}")
print(f"  Download link : {download_link}")

# Auto-update .env with the drive link
if os.path.exists(ENV_PATH):
    with open(ENV_PATH, "r") as f:
        content = f.read()

    if "PORTFOLIO_DRIVE_LINK" in content:
        lines = content.splitlines()
        lines = [f"PORTFOLIO_DRIVE_LINK={view_link}" if l.startswith("PORTFOLIO_DRIVE_LINK") else l for l in lines]
        content = "\n".join(lines)
    else:
        content += f"\n# Google Drive Portfolio Link\nPORTFOLIO_DRIVE_LINK={view_link}\n"

    with open(ENV_PATH, "w") as f:
        f.write(content)
    print(f"\n  .env updated with PORTFOLIO_DRIVE_LINK")

print("\n" + "=" * 55)
print("  UPLOAD COMPLETE!")
print("=" * 55)
print(f"\n  Share this link with leads:")
print(f"  {view_link}")
print("\n  The link is now embedded in all email templates.")
print("  Restart app.py to apply.\n")

input("Press Enter to exit.")
