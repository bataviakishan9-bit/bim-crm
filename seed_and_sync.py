"""
BIM Infra Solutions — DB Seed + Live Excel Sync
================================================
Place: C:\\Users\\Kishan\\BIM_CRM\\

HOW TO USE
----------
Run ONCE to load all 306 leads into your DB and generate Excel:
    python seed_and_sync.py

Other commands:
    python seed_and_sync.py export       → DB → refresh BIM_CRM_Sync.xlsx
    python seed_and_sync.py import       → BIM_CRM_Sync.xlsx Lead Database → DB (new leads only)
    python seed_and_sync.py push-status  → Edit statuses in Excel → push to DB
    python seed_and_sync.py mail-sync    → Scan Zoho inbox → detect replies → update DB
    python seed_and_sync.py full-sync    → export + mail-sync together
    python seed_and_sync.py status       → Show live DB stats
    python seed_and_sync.py watch [N]    → Auto full-sync every N minutes (default 5)

TWO-WAY SYNC FLOW:
  Your CRM App  ──→  bim_crm.db  ──→  [export]  ──→  BIM_CRM_Sync.xlsx
  BIM_CRM_Sync.xlsx  ──→  [import / push-status]  ──→  bim_crm.db  ──→  Your CRM App

ZOHO MAIL SYNC:
  Lead replies their email  →  [mail-sync]  →  status=Replied  →  appears in CRM + Excel
"""

import os, sys, csv, time, sqlite3
from pathlib import Path
from datetime import datetime

BASE_DIR   = Path(__file__).parent
DB_PATH    = BASE_DIR / "bim_crm.db"
EXCEL_PATH = BASE_DIR / "BIM_CRM_Sync.xlsx"
SEED_CSV   = BASE_DIR / "leads_seed.csv"


# ── DB connection ────────────────────────────────────────────────────────────
def get_db():
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    return conn


# ── Seed from CSV ────────────────────────────────────────────────────────────
def seed_database():
    """
    Load all leads from leads_seed.csv into bim_crm.db.
    Safe to run multiple times — skips duplicates via UNIQUE email constraint.
    """
    if not SEED_CSV.exists():
        print(f"❌ Seed file not found: {SEED_CSV}")
        print("   Make sure leads_seed.csv is in the same folder as this script.")
        return 0

    conn    = get_db()
    now     = datetime.utcnow().isoformat()
    inserted = skipped = 0

    with open(SEED_CSV, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            email = row.get('email', '').strip()
            if not email or '@' not in email:
                skipped += 1
                continue

            tmpl = str(row.get('email_template', 'A')).strip()
            if tmpl not in ('A', 'B', 'C'):
                tmpl = 'A'

            try:
                priority = int(float(row.get('priority_score', 0) or 0))
            except:
                priority = 0

            try:
                conn.execute("""
                    INSERT OR IGNORE INTO leads (
                        first_name, last_name, email, company, title, phone, website,
                        city, country, industry, status, priority_score, services_needed,
                        outsourcing_likelihood, pitch_angle, email_template, linkedin_url,
                        follow_up_stage, description, email_sequence_step, created_at, updated_at
                    ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,0,?,?)
                """, (
                    row.get('first_name', '').strip(),
                    row.get('last_name', '').strip(),
                    email,
                    row.get('company', '').strip(),
                    row.get('title', '').strip(),
                    '',
                    row.get('website', '').strip(),
                    row.get('city', '').strip(),
                    row.get('country', '').strip(),
                    row.get('industry', '').strip(),
                    'New',
                    priority,
                    row.get('services_needed', '').strip(),
                    row.get('outsourcing_likelihood', '').strip(),
                    row.get('pitch_angle', '').strip(),
                    tmpl,
                    row.get('linkedin_url', '').strip(),
                    '',
                    row.get('description', '').strip(),
                    now, now
                ))
                inserted += 1
            except sqlite3.IntegrityError:
                skipped += 1
            except Exception as e:
                print(f"  ⚠  Skip {email}: {e}")
                skipped += 1

    conn.commit()
    total = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    conn.close()

    print(f"✅ Seed complete: {inserted} new leads | {skipped} skipped (duplicates/invalid)")
    print(f"   Total leads in DB now: {total}")
    return total


# ── Export DB → Excel ────────────────────────────────────────────────────────
def export_excel():
    """
    Run crm_excel_sync.py's build_excel() against the live local DB.
    Generates BIM_CRM_Sync.xlsx with 6 sheets.
    """
    import importlib.util
    spec = importlib.util.spec_from_file_location("sync", BASE_DIR / "crm_excel_sync.py")
    mod  = importlib.util.module_from_spec(spec)

    # Load env vars first
    env_path = BASE_DIR / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                k, v = line.split('=', 1)
                os.environ.setdefault(k.strip(), v.strip())

    spec.loader.exec_module(mod)

    # Override paths so it uses our local DB + output path
    mod.DB_PATH    = DB_PATH
    mod.EXCEL_PATH = EXCEL_PATH
    mod.get_db     = get_db

    mod.build_excel()
    if EXCEL_PATH.exists():
        size = EXCEL_PATH.stat().st_size
        print(f"✅ Excel refreshed → {EXCEL_PATH.name}  ({size // 1024} KB)")
    else:
        print("❌ Excel generation failed — check logs above.")


# ── Live status ──────────────────────────────────────────────────────────────
def show_status():
    conn    = get_db()
    total   = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    hot     = conn.execute("SELECT COUNT(*) FROM leads WHERE status='Hot'").fetchone()[0]
    new     = conn.execute("SELECT COUNT(*) FROM leads WHERE status='New'").fetchone()[0]
    cont    = conn.execute("SELECT COUNT(*) FROM leads WHERE status='Contacted'").fetchone()[0]
    replied = conn.execute("SELECT COUNT(*) FROM leads WHERE status IN ('Replied','Engaged — Reply Received')").fetchone()[0]
    sent    = conn.execute("SELECT COUNT(*) FROM email_logs").fetchone()[0]
    opens   = conn.execute("SELECT COUNT(*) FROM email_logs WHERE opened=1").fetchone()[0]
    tasks   = conn.execute("SELECT COUNT(*) FROM tasks WHERE status!='Completed'").fetchone()[0]
    excel_state = "EXISTS  ✅" if EXCEL_PATH.exists() else "NOT YET GENERATED"
    conn.close()

    print(f"""
╔══════════════════════════════════════════╗
║   BIM INFRA CRM — LIVE DATABASE STATUS   ║
╠══════════════════════════════════════════╣
║  Total Leads    : {total:<24}║
║  New            : {new:<24}║
║  Contacted      : {cont:<24}║
║  Hot 🔥         : {hot:<24}║
║  Replied ✅     : {replied:<24}║
╠══════════════════════════════════════════╣
║  Emails Sent    : {sent:<24}║
║  Emails Opened  : {opens:<24}║
║  Pending Tasks  : {tasks:<24}║
╠══════════════════════════════════════════╣
║  BIM_CRM_Sync.xlsx : {excel_state:<21}║
╚══════════════════════════════════════════╝""")


# ── CLI entry point ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    cmd = sys.argv[1].lower() if len(sys.argv) > 1 else "seed"

    print(f"\n{'='*50}")
    print(f"  BIM CRM Sync — {cmd.upper()}  [{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]")
    print(f"{'='*50}\n")

    if cmd == "seed":
        # ── STEP 1: Seed DB
        db_count = get_db().execute("SELECT COUNT(*) FROM leads").fetchone()[0]
        if db_count == 0:
            print("📥 Database is empty — loading 306 leads from CSV seed file...")
            total = seed_database()
        else:
            print(f"ℹ  Database already has {db_count} leads. Checking for new entries...")
            total = seed_database()

        # ── STEP 2: Export Excel
        if total > 0:
            print(f"\n📊 Generating Excel from {total} live DB leads...")
            export_excel()
            print(f"\n✅ ALL DONE!")
            print(f"   Open BIM_CRM_Sync.xlsx to see all leads + live stats.")
            print(f"\nNEXT STEPS:")
            print(f"  1. Open your CRM at http://127.0.0.1:5000")
            print(f"  2. Go to Import Leads → upload BIM_CRM_Sync.xlsx → sheet 'Lead Database'")
            print(f"  3. Run 'python seed_and_sync.py watch' for auto-sync every 5 min")

    elif cmd == "export":
        print("📊 Exporting live DB → BIM_CRM_Sync.xlsx ...")
        export_excel()

    elif cmd == "import":
        print("📥 Importing from BIM_CRM_Sync.xlsx → DB ...")
        os.system(f'python "{BASE_DIR / "crm_excel_sync.py"}" import')

    elif cmd == "push-status":
        print("⬆  Pushing status edits → DB ...")
        os.system(f'python "{BASE_DIR / "crm_excel_sync.py"}" push-status')

    elif cmd == "mail-sync":
        print("📬 Scanning Zoho inbox for replies ...")
        os.system(f'python "{BASE_DIR / "crm_excel_sync.py"}" mail-sync')

    elif cmd == "full-sync":
        print("🔄 Full sync: export + mail-sync ...")
        export_excel()
        os.system(f'python "{BASE_DIR / "crm_excel_sync.py"}" mail-sync')
        print("\n✅ Full sync complete.")

    elif cmd == "status":
        show_status()

    elif cmd == "watch":
        interval = int(sys.argv[2]) if len(sys.argv) > 2 else 5
        print(f"👁  Auto-sync every {interval} min. Press Ctrl+C to stop.\n")
        # First sync immediately
        export_excel()
        os.system(f'python "{BASE_DIR / "crm_excel_sync.py"}" mail-sync')
        while True:
            time.sleep(interval * 60)
            ts = datetime.now().strftime('%H:%M:%S')
            print(f"\n[{ts}] Auto-syncing...")
            try:
                export_excel()
                os.system(f'python "{BASE_DIR / "crm_excel_sync.py"}" mail-sync')
                print(f"[{ts}] ✅ Sync done.")
            except Exception as e:
                print(f"[{ts}] ⚠ Error: {e}")

    else:
        print(f"Unknown command: {cmd}")
        print("\nUsage: python seed_and_sync.py [seed|export|import|push-status|mail-sync|full-sync|status|watch [N]]")
