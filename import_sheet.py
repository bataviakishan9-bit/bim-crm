"""
Import Survey of India Geospatial leads from Google Sheet into BIM CRM
Run: py import_sheet.py
"""
import requests, csv, io, sys, os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from dotenv import load_dotenv
load_dotenv(override=True)
import database as db

db.init_db()

sheet_id = "1buAYF8WykRsNToMw8UjAHct_EOX8TyH8"
url      = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
r        = requests.get(url, timeout=15)
content  = r.content.decode("utf-8", errors="replace")
rows     = list(csv.reader(io.StringIO(content)))

# Header row is row index 2
headers = rows[2]  # Sl No, Company Name, Class, Key Person, Designation, Phone, Email, Address, City, Website, Services, Status

imported, skipped = 0, 0

for row in rows[3:]:
    # Skip section header rows and empty rows
    if not row or not row[0].strip() or not row[0].strip().isdigit():
        continue

    email   = row[6].strip() if len(row) > 6 else ""
    company = row[1].strip() if len(row) > 1 else ""
    name    = row[3].strip() if len(row) > 3 else ""
    parts   = name.split() if name else []

    # Skip if no email
    if not email or email.lower() in ("", "nan"):
        skipped += 1
        continue

    # Priority based on class
    cls = row[2].strip() if len(row) > 2 else ""
    priority = 90 if cls == "A+" else 75 if cls == "A" else 60 if cls == "B" else 45

    data = {
        "first_name"            : parts[0] if parts else company.split()[0],
        "last_name"             : " ".join(parts[1:]) if len(parts) > 1 else "",
        "email"                 : email,
        "company"               : company,
        "title"                 : row[4].strip() if len(row) > 4 else "",
        "phone"                 : row[5].strip() if len(row) > 5 else "",
        "website"               : row[9].strip() if len(row) > 9 else "",
        "city"                  : row[8].strip() if len(row) > 8 else "",
        "country"               : "India",
        "industry"              : "Geospatial / Drone / Survey",
        "status"                : "New",
        "priority_score"        : priority,
        "services_needed"       : row[10].strip() if len(row) > 10 else "",
        "outsourcing_likelihood": "High" if cls in ("A+", "A") else "Medium",
        "pitch_angle"           : "INFRA X Drone Progress Monitoring",
        "email_template"        : "D",
        "linkedin_url"          : "",
        "follow_up_stage"       : "",
        "description"           : f"Survey of India Empanelled Firm — Class {cls} | {row[10].strip() if len(row) > 10 else ''}",
    }

    try:
        db.create_lead(data)
        imported += 1
        print(f"  Imported: {name} — {company} ({email})")
    except Exception as e:
        if "UNIQUE constraint" in str(e):
            skipped += 1
        else:
            print(f"  Error: {company} — {e}")
            skipped += 1

print(f"\n{'='*50}")
print(f"  Import complete: {imported} imported, {skipped} skipped")
print(f"{'='*50}")
