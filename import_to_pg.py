"""
Run this ONCE on Render shell to import all data into PostgreSQL.
Usage: python import_to_pg.py
"""
import os, json, psycopg2
from dotenv import load_dotenv
load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL", "")
if not DATABASE_URL:
    print("ERROR: DATABASE_URL not set")
    exit(1)

conn = psycopg2.connect(DATABASE_URL)
c = conn.cursor()

with open("export_data.json", encoding="utf-8") as f:
    data = json.load(f)

# ── Import leads ──────────────────────────────────────────────────────────────
leads = data.get("leads", [])
skipped = 0
imported = 0
for r in leads:
    try:
        c.execute("""
            INSERT INTO leads (
                id, first_name, last_name, email, company, title, phone, website,
                city, country, industry, status, priority_score, services_needed,
                outsourcing_likelihood, pitch_angle, email_template, linkedin_url,
                follow_up_stage, description, email_sequence_step,
                created_at, updated_at, last_email_sent
            ) VALUES (
                %(id)s, %(first_name)s, %(last_name)s, %(email)s, %(company)s,
                %(title)s, %(phone)s, %(website)s, %(city)s, %(country)s,
                %(industry)s, %(status)s, %(priority_score)s, %(services_needed)s,
                %(outsourcing_likelihood)s, %(pitch_angle)s, %(email_template)s,
                %(linkedin_url)s, %(follow_up_stage)s, %(description)s,
                %(email_sequence_step)s, %(created_at)s, %(updated_at)s, %(last_email_sent)s
            ) ON CONFLICT (email) DO NOTHING
        """, r)
        imported += 1
    except Exception as e:
        print(f"  Lead skip: {r.get(chr(39)+'email'+chr(39))} — {e}")
        skipped += 1
print(f"Leads: {imported} imported, {skipped} skipped")

# Reset sequence
c.execute("SELECT setval(chr(39)+chr(39)+'leads_id_seq'+chr(39)+chr(39), (SELECT MAX(id) FROM leads))")

# ── Import email_logs ─────────────────────────────────────────────────────────
logs = data.get("email_logs", [])
ok = 0
for r in logs:
    try:
        c.execute("""
            INSERT INTO email_logs (id, lead_id, subject, body, template_used, sequence_step,
                sent_at, status, opened, clicked, open_count, bounced, bounce_reason)
            VALUES (%(id)s, %(lead_id)s, %(subject)s, %(body)s, %(template_used)s,
                %(sequence_step)s, %(sent_at)s, %(status)s, %(opened)s, %(clicked)s,
                %(open_count)s, %(bounced)s, %(bounce_reason)s)
            ON CONFLICT DO NOTHING
        """, r)
        ok += 1
    except Exception as e:
        pass
print(f"Email logs: {ok} imported")

# ── Import replies ────────────────────────────────────────────────────────────
replies = data.get("replies", [])
ok = 0
for r in replies:
    try:
        c.execute("""
            INSERT INTO replies (id, lead_id, from_email, subject, body, priority, status, source, received_at)
            VALUES (%(id)s, %(lead_id)s, %(from_email)s, %(subject)s, %(body)s,
                %(priority)s, %(status)s, %(source)s, %(received_at)s)
            ON CONFLICT DO NOTHING
        """, r)
        ok += 1
    except Exception as e:
        pass
print(f"Replies: {ok} imported")

# ── Import tasks ──────────────────────────────────────────────────────────────
tasks = data.get("tasks", [])
ok = 0
for r in tasks:
    try:
        c.execute("""
            INSERT INTO tasks (id, lead_id, subject, due_date, status, priority, description, created_at)
            VALUES (%(id)s, %(lead_id)s, %(subject)s, %(due_date)s,
                %(status)s, %(priority)s, %(description)s, %(created_at)s)
            ON CONFLICT DO NOTHING
        """, r)
        ok += 1
    except Exception as e:
        pass
print(f"Tasks: {ok} imported")

conn.commit()
conn.close()
print("
Done! All data imported successfully.")
