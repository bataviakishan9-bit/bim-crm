"""
BIM Infra Solutions — CRM Database
Uses PostgreSQL on Render (DATABASE_URL set) or SQLite locally.
"""
import os
import sqlite3
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL", "")

# ── Connection helper ──────────────────────────────────────────────────────────

def _is_pg():
    return bool(os.getenv("DATABASE_URL", DATABASE_URL))

def get_db():
    if _is_pg():
        import psycopg2
        import psycopg2.extras
        url = os.getenv("DATABASE_URL", DATABASE_URL)
        conn = psycopg2.connect(url, cursor_factory=psycopg2.extras.RealDictCursor)
        return conn
    else:
        DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bim_crm.db")
        conn = sqlite3.connect(DB_PATH, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA busy_timeout=10000")
        return conn

def _q(sql: str) -> str:
    """Convert SQLite ? placeholders to %s for PostgreSQL."""
    if _is_pg():
        return sql.replace("?", "%s")
    return sql

def _named(sql: str) -> str:
    """Convert :name placeholders to %(name)s for PostgreSQL."""
    if _is_pg():
        import re
        return re.sub(r':([a-zA-Z_][a-zA-Z0-9_]*)', r'%(\1)s', sql)
    return sql

def _lastrow(cursor, table: str) -> int:
    """Get last inserted row ID (cross-DB)."""
    if _is_pg():
        return cursor.fetchone()["id"]
    return cursor.lastrowid

def _fetchall(rows) -> list:
    if _is_pg():
        return [dict(r) for r in rows]
    return [dict(r) for r in rows]

def _fetchone(row) -> dict | None:
    if row is None:
        return None
    return dict(row)


# ── Schema ─────────────────────────────────────────────────────────────────────

def init_db():
    try:
        conn = get_db()
    except Exception as e:
        import logging
        logging.warning("DB connection failed, falling back to SQLite: %s", e)
        global DATABASE_URL
        DATABASE_URL = ""
        conn = get_db()
    c = conn.cursor()

    if _is_pg():
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id                      SERIAL PRIMARY KEY,
                first_name              TEXT NOT NULL,
                last_name               TEXT,
                email                   TEXT UNIQUE NOT NULL,
                company                 TEXT,
                title                   TEXT,
                phone                   TEXT,
                website                 TEXT,
                city                    TEXT,
                country                 TEXT DEFAULT 'USA',
                industry                TEXT,
                status                  TEXT DEFAULT 'New',
                priority_score          INTEGER DEFAULT 0,
                services_needed         TEXT,
                outsourcing_likelihood  TEXT,
                pitch_angle             TEXT,
                email_template          TEXT DEFAULT 'A',
                linkedin_url            TEXT,
                follow_up_stage         TEXT,
                description             TEXT,
                email_sequence_step     INTEGER DEFAULT 0,
                created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_email_sent         TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS email_logs (
                id              SERIAL PRIMARY KEY,
                lead_id         INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
                subject         TEXT,
                body            TEXT,
                template_used   TEXT,
                sequence_step   INTEGER DEFAULT 0,
                sent_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status          TEXT DEFAULT 'sent',
                opened          INTEGER DEFAULT 0,
                clicked         INTEGER DEFAULT 0,
                open_count      INTEGER DEFAULT 0,
                bounced         INTEGER DEFAULT 0,
                bounce_reason   TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS replies (
                id           SERIAL PRIMARY KEY,
                lead_id      INTEGER REFERENCES leads(id) ON DELETE SET NULL,
                from_email   TEXT,
                subject      TEXT,
                body         TEXT,
                priority     TEXT DEFAULT 'Medium',
                status       TEXT DEFAULT 'Unread',
                source       TEXT DEFAULT 'Manual',
                received_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id          SERIAL PRIMARY KEY,
                lead_id     INTEGER NOT NULL REFERENCES leads(id) ON DELETE CASCADE,
                subject     TEXT NOT NULL,
                due_date    TIMESTAMP,
                status      TEXT DEFAULT 'Not Started',
                priority    TEXT DEFAULT 'Medium',
                description TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS leads (
                id                      INTEGER PRIMARY KEY AUTOINCREMENT,
                first_name              TEXT NOT NULL,
                last_name               TEXT,
                email                   TEXT UNIQUE NOT NULL,
                company                 TEXT,
                title                   TEXT,
                phone                   TEXT,
                website                 TEXT,
                city                    TEXT,
                country                 TEXT DEFAULT 'USA',
                industry                TEXT,
                status                  TEXT DEFAULT 'New',
                priority_score          INTEGER DEFAULT 0,
                services_needed         TEXT,
                outsourcing_likelihood  TEXT,
                pitch_angle             TEXT,
                email_template          TEXT DEFAULT 'A',
                linkedin_url            TEXT,
                follow_up_stage         TEXT,
                description             TEXT,
                email_sequence_step     INTEGER DEFAULT 0,
                created_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at              TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                last_email_sent         TIMESTAMP
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS email_logs (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id         INTEGER NOT NULL,
                subject         TEXT,
                body            TEXT,
                template_used   TEXT,
                sequence_step   INTEGER DEFAULT 0,
                sent_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                status          TEXT DEFAULT 'sent',
                opened          INTEGER DEFAULT 0,
                clicked         INTEGER DEFAULT 0,
                open_count      INTEGER DEFAULT 0,
                bounced         INTEGER DEFAULT 0,
                bounce_reason   TEXT,
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
            )
        """)
        for col in ["bounced INTEGER DEFAULT 0", "bounce_reason TEXT"]:
            try:
                c.execute(f"ALTER TABLE email_logs ADD COLUMN {col}")
            except Exception:
                pass
        c.execute("""
            CREATE TABLE IF NOT EXISTS replies (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id      INTEGER,
                from_email   TEXT,
                subject      TEXT,
                body         TEXT,
                priority     TEXT DEFAULT 'Medium',
                status       TEXT DEFAULT 'Unread',
                source       TEXT DEFAULT 'Manual',
                received_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE SET NULL
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS tasks (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id     INTEGER NOT NULL,
                subject     TEXT NOT NULL,
                due_date    TIMESTAMP,
                status      TEXT DEFAULT 'Not Started',
                priority    TEXT DEFAULT 'Medium',
                description TEXT,
                created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
            )
        """)

    # user_settings table (PostgreSQL)
    if _is_pg():
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                username        TEXT PRIMARY KEY,
                sender_email    TEXT,
                sender_name     TEXT,
                zoho_client_id     TEXT,
                zoho_client_secret TEXT,
                zoho_refresh_token TEXT,
                zoho_dc            TEXT DEFAULT 'in',
                zoho_account_id    TEXT,
                is_locked          INTEGER DEFAULT 0,
                wa_phone           TEXT,
                callmebot_api_key  TEXT,
                updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Migrate existing tables — add columns if missing (IF NOT EXISTS avoids transaction abort)
        for col, coldef in [("wa_phone", "TEXT"), ("callmebot_api_key", "TEXT")]:
            c.execute(f"ALTER TABLE user_settings ADD COLUMN IF NOT EXISTS {col} {coldef}")
        conn.commit()
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS user_settings (
                username        TEXT PRIMARY KEY,
                sender_email    TEXT,
                sender_name     TEXT,
                zoho_client_id     TEXT,
                zoho_client_secret TEXT,
                zoho_refresh_token TEXT,
                zoho_dc            TEXT DEFAULT 'in',
                zoho_account_id    TEXT,
                is_locked          INTEGER DEFAULT 0,
                wa_phone           TEXT,
                callmebot_api_key  TEXT,
                updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        # Migrate existing tables — add columns if missing (SQLite doesn't support IF NOT EXISTS on ALTER)
        for col, coldef in [("wa_phone", "TEXT"), ("callmebot_api_key", "TEXT")]:
            try:
                c.execute(f"ALTER TABLE user_settings ADD COLUMN {col} {coldef}")
                conn.commit()
            except Exception:
                pass  # SQLite: swallow the error (no rollback needed — SQLite auto-resets)

    # whatsapp_logs table
    if _is_pg():
        c.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_logs (
                id          SERIAL PRIMARY KEY,
                lead_id     INTEGER REFERENCES leads(id) ON DELETE CASCADE,
                phone       TEXT,
                message     TEXT,
                sent_by     TEXT,
                method      TEXT DEFAULT 'manual',
                status      TEXT DEFAULT 'sent',
                sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS whatsapp_logs (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                lead_id     INTEGER,
                phone       TEXT,
                message     TEXT,
                sent_by     TEXT,
                method      TEXT DEFAULT 'manual',
                status      TEXT DEFAULT 'sent',
                sent_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (lead_id) REFERENCES leads(id) ON DELETE CASCADE
            )
        """)

    # email_templates table
    if _is_pg():
        c.execute("""
            CREATE TABLE IF NOT EXISTS email_templates (
                id           SERIAL PRIMARY KEY,
                template_key TEXT NOT NULL,
                step         INTEGER NOT NULL,
                subject      TEXT NOT NULL,
                body         TEXT NOT NULL,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(template_key, step)
            )
        """)
    else:
        c.execute("""
            CREATE TABLE IF NOT EXISTS email_templates (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                template_key TEXT NOT NULL,
                step         INTEGER NOT NULL,
                subject      TEXT NOT NULL,
                body         TEXT NOT NULL,
                updated_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                UNIQUE(template_key, step)
            )
        """)

    # internal_notes table
    tbl_serial = "SERIAL" if _is_pg() else "INTEGER"
    pk = "PRIMARY KEY" if not _is_pg() else "PRIMARY KEY"
    auto = "" if _is_pg() else "AUTOINCREMENT"
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS internal_notes (
            id         {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            title      TEXT NOT NULL,
            body       TEXT NOT NULL,
            created_by TEXT NOT NULL,
            is_pinned  INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # responsibilities table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS responsibilities (
            id          {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            title       TEXT NOT NULL,
            description TEXT,
            assigned_to TEXT NOT NULL,
            assigned_by TEXT NOT NULL,
            category    TEXT DEFAULT 'General',
            status      TEXT DEFAULT 'Active',
            created_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # team_tasks table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS team_tasks (
            id            {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            title         TEXT NOT NULL,
            description   TEXT,
            assigned_to   TEXT NOT NULL,
            assigned_by   TEXT NOT NULL,
            lead_id       INTEGER,
            due_date      TIMESTAMP,
            priority      TEXT DEFAULT 'Medium',
            status        TEXT DEFAULT 'Pending',
            reminder_at   TIMESTAMP,
            reminder_sent INTEGER DEFAULT 0,
            email_sent    INTEGER DEFAULT 0,
            created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # expenses table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS expenses (
            id           {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            date         DATE NOT NULL,
            month_year   TEXT NOT NULL,
            partner      TEXT NOT NULL,
            expense_type TEXT NOT NULL,
            project_name TEXT,
            category     TEXT NOT NULL,
            description  TEXT,
            amount       NUMERIC(12,2) NOT NULL DEFAULT 0,
            created_by   TEXT,
            created_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # income_entries table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS income_entries (
            id               {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            date             DATE NOT NULL,
            month_year       TEXT NOT NULL,
            client_source    TEXT NOT NULL,
            income_category  TEXT NOT NULL,
            description      TEXT,
            amount           NUMERIC(12,2) NOT NULL DEFAULT 0,
            created_by       TEXT,
            created_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # projects table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS projects (
            id              {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            name            TEXT NOT NULL,
            client_name     TEXT NOT NULL,
            client_address  TEXT,
            client_gstin    TEXT,
            start_date      DATE,
            end_date        DATE,
            status          TEXT DEFAULT 'Active',
            total_value     NUMERIC(14,2) DEFAULT 0,
            description     TEXT,
            lead_id         INTEGER,
            created_by      TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # invoices table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS invoices (
            id              {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            invoice_no      TEXT UNIQUE NOT NULL,
            date            DATE NOT NULL,
            project_id      INTEGER,
            client_name     TEXT NOT NULL,
            client_address  TEXT,
            client_gstin    TEXT,
            lut_number      TEXT,
            gst_rate        NUMERIC(5,2) DEFAULT 18,
            subtotal        NUMERIC(14,2) DEFAULT 0,
            gst_amount      NUMERIC(14,2) DEFAULT 0,
            total           NUMERIC(14,2) DEFAULT 0,
            notes           TEXT,
            status          TEXT DEFAULT 'Draft',
            created_by      TEXT,
            created_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at      TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # invoice_items table
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS invoice_items (
            id              {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            invoice_id      INTEGER NOT NULL,
            description     TEXT NOT NULL,
            sac_code        TEXT,
            unit            INTEGER DEFAULT 1,
            rate            NUMERIC(14,2) DEFAULT 0,
            amount          NUMERIC(14,2) DEFAULT 0,
            sort_order      INTEGER DEFAULT 0
        )
    """)

    # zoho_synced_messages: tracks which Zoho message IDs have already been processed
    # so sync-delivery never creates duplicate bounce/reply entries
    c.execute(f"""
        CREATE TABLE IF NOT EXISTS zoho_synced_messages (
            id          {"SERIAL PRIMARY KEY" if _is_pg() else "INTEGER PRIMARY KEY AUTOINCREMENT"},
            message_id  TEXT NOT NULL UNIQUE,
            processed_at TEXT NOT NULL,
            msg_type    TEXT DEFAULT 'unknown'
        )
    """)

    # app_config: generic key-value store for runtime settings (e.g. Zoho refresh token)
    # Using TEXT PRIMARY KEY so it works on both PostgreSQL and SQLite without SERIAL
    c.execute("""
        CREATE TABLE IF NOT EXISTS app_config (
            key        TEXT PRIMARY KEY,
            value      TEXT,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)

    conn.commit()
    conn.close()


# ── ZOHO SYNC TRACKING ─────────────────────────────────────────────────────────

def get_synced_message_ids() -> set:
    """Return the set of already-processed Zoho message IDs."""
    conn = get_db()
    c = conn.cursor()
    try:
        c.execute("SELECT message_id FROM zoho_synced_messages")
        rows = c.fetchall()
        return {r[0] if not _is_pg() else r["message_id"] for r in rows}
    except Exception:
        return set()
    finally:
        conn.close()


def mark_messages_synced(message_ids: list, msg_type: str = "unknown"):
    """Record a batch of Zoho message IDs as processed."""
    if not message_ids:
        return
    conn = get_db()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    try:
        for mid in message_ids:
            if _is_pg():
                c.execute(
                    "INSERT INTO zoho_synced_messages (message_id, processed_at, msg_type) "
                    "VALUES (%s, %s, %s) ON CONFLICT (message_id) DO NOTHING",
                    (str(mid), now, msg_type)
                )
            else:
                c.execute(
                    "INSERT OR IGNORE INTO zoho_synced_messages (message_id, processed_at, msg_type) "
                    "VALUES (?, ?, ?)",
                    (str(mid), now, msg_type)
                )
        conn.commit()
    except Exception as e:
        import logging
        logging.warning("mark_messages_synced failed: %s", e)
    finally:
        conn.close()


# ── USER SETTINGS ──────────────────────────────────────────────────────────────

def get_user_settings(username: str) -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM user_settings WHERE username = ?"), (username,))
    row = c.fetchone()
    conn.close()
    if row:
        return _fetchone(row)
    return {}


def save_user_settings(username: str, data: dict):
    conn = get_db()
    c = conn.cursor()
    existing = get_user_settings(username)
    if existing:
        sql = _named("""
            UPDATE user_settings SET
                sender_email       = :sender_email,
                sender_name        = :sender_name,
                zoho_client_id     = :zoho_client_id,
                zoho_client_secret = :zoho_client_secret,
                zoho_refresh_token = :zoho_refresh_token,
                zoho_dc            = :zoho_dc,
                zoho_account_id    = :zoho_account_id,
                is_locked          = :is_locked,
                wa_phone           = :wa_phone,
                callmebot_api_key  = :callmebot_api_key,
                updated_at         = :updated_at
            WHERE username = :username
        """)
    else:
        sql = _named("""
            INSERT INTO user_settings (
                username, sender_email, sender_name,
                zoho_client_id, zoho_client_secret, zoho_refresh_token,
                zoho_dc, zoho_account_id, is_locked,
                wa_phone, callmebot_api_key, updated_at
            ) VALUES (
                :username, :sender_email, :sender_name,
                :zoho_client_id, :zoho_client_secret, :zoho_refresh_token,
                :zoho_dc, :zoho_account_id, :is_locked,
                :wa_phone, :callmebot_api_key, :updated_at
            )
        """)
    data["username"] = username
    data["updated_at"] = datetime.utcnow().isoformat()
    c.execute(sql, data)
    conn.commit()
    conn.close()


# ── EMAIL TEMPLATES ───────────────────────────────────────────────────────────

def get_all_email_templates() -> dict:
    """Returns {(key, step): {subject, body}} for all saved custom templates."""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT template_key, step, subject, body FROM email_templates ORDER BY template_key, step")
    rows = _fetchall(c.fetchall())
    conn.close()
    return {(r["template_key"], r["step"]): r for r in rows}


def get_email_template(template_key: str, step: int) -> dict | None:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM email_templates WHERE template_key=? AND step=?"), (template_key, step))
    row = c.fetchone()
    conn.close()
    return _fetchone(row) if row else None


def save_email_template(template_key: str, step: int, subject: str, body: str):
    conn = get_db()
    c = conn.cursor()
    existing = get_email_template(template_key, step)
    if existing:
        c.execute(_q("UPDATE email_templates SET subject=?, body=?, updated_at=? WHERE template_key=? AND step=?"),
                  (subject, body, datetime.utcnow().isoformat(), template_key, step))
    else:
        c.execute(_q("INSERT INTO email_templates (template_key, step, subject, body, updated_at) VALUES (?,?,?,?,?)"),
                  (template_key, step, subject, body, datetime.utcnow().isoformat()))
    conn.commit()
    conn.close()


# ── LEADS ──────────────────────────────────────────────────────────────────────

def create_lead(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _named("""
        INSERT INTO leads (
            first_name, last_name, email, company, title, phone, website,
            city, country, industry, status, priority_score, services_needed,
            outsourcing_likelihood, pitch_angle, email_template, linkedin_url,
            follow_up_stage, description
        ) VALUES (
            :first_name, :last_name, :email, :company, :title, :phone, :website,
            :city, :country, :industry, :status, :priority_score, :services_needed,
            :outsourcing_likelihood, :pitch_angle, :email_template, :linkedin_url,
            :follow_up_stage, :description
        )
    """ + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, data)
    lid = _lastrow(c, "leads")
    conn.commit()
    conn.close()
    return lid


def get_lead(lead_id: int) -> dict | None:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM leads WHERE id = ?"), (lead_id,))
    row = c.fetchone()
    conn.close()
    return _fetchone(row)


def get_all_leads(search=None, status=None, country=None, template=None) -> list:
    conn = get_db()
    c = conn.cursor()
    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if search:
        query += _q(" AND (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ?)")
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if status:
        query += _q(" AND status = ?")
        params.append(status)
    if country:
        query += _q(" AND country = ?")
        params.append(country)
    if template:
        query += _q(" AND email_template = ?")
        params.append(template)

    query += " ORDER BY priority_score DESC, created_at DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return _fetchall(rows)


def update_lead(lead_id: int, data: dict):
    conn = get_db()
    c = conn.cursor()
    data["id"] = lead_id
    data["updated_at"] = datetime.utcnow().isoformat()
    fields = [k for k in data.keys() if k not in ("id", "created_at")]
    if _is_pg():
        set_clause = ", ".join(f"{f} = %({f})s" for f in fields)
        sql = f"UPDATE leads SET {set_clause} WHERE id = %(id)s"
    else:
        set_clause = ", ".join(f"{f} = :{f}" for f in fields)
        sql = f"UPDATE leads SET {set_clause} WHERE id = :id"
    c.execute(sql, data)
    conn.commit()
    conn.close()


def delete_lead(lead_id: int):
    conn = get_db()
    conn.cursor().execute(_q("DELETE FROM leads WHERE id = ?"), (lead_id,))
    conn.commit()
    conn.close()


def update_lead_status(lead_id: int, status: str, follow_up_stage: str = None):
    conn = get_db()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    if follow_up_stage:
        c.execute(_q("UPDATE leads SET status=?, follow_up_stage=?, updated_at=? WHERE id=?"),
                  (status, follow_up_stage, now, lead_id))
    else:
        c.execute(_q("UPDATE leads SET status=?, updated_at=? WHERE id=?"),
                  (status, now, lead_id))
    conn.commit()
    conn.close()


def advance_sequence_step(lead_id: int):
    conn = get_db()
    now = datetime.utcnow().isoformat()
    conn.cursor().execute(
        _q("UPDATE leads SET email_sequence_step=email_sequence_step+1, last_email_sent=?, updated_at=? WHERE id=?"),
        (now, now, lead_id),
    )
    conn.commit()
    conn.close()


# ── REPLIES ───────────────────────────────────────────────────────────────────

def add_reply(lead_id, from_email, subject, body, priority="Medium", source="Manual") -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO replies (lead_id, from_email, subject, body, priority, source) VALUES (?,?,?,?,?,?)")
    if _is_pg():
        sql += " RETURNING id"
    c.execute(sql, (lead_id, from_email, subject, body, priority, source))
    rid = _lastrow(c, "replies")
    conn.commit()
    conn.close()
    return rid


def get_replies(priority=None, status=None) -> list:
    conn = get_db()
    c = conn.cursor()
    query = """
        SELECT r.*, l.first_name, l.last_name, l.company
        FROM replies r
        LEFT JOIN leads l ON r.lead_id = l.id
        WHERE 1=1
    """
    params = []
    if priority:
        query += _q(" AND r.priority = ?")
        params.append(priority)
    if status:
        query += _q(" AND r.status = ?")
        params.append(status)
    query += " ORDER BY CASE r.priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, r.received_at DESC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return _fetchall(rows)


def update_reply(reply_id: int, priority: str = None, status: str = None):
    conn = get_db()
    c = conn.cursor()
    if priority:
        c.execute(_q("UPDATE replies SET priority=? WHERE id=?"), (priority, reply_id))
    if status:
        c.execute(_q("UPDATE replies SET status=? WHERE id=?"), (status, reply_id))
    conn.commit()
    conn.close()


def delete_reply(reply_id: int):
    conn = get_db()
    conn.cursor().execute(_q("DELETE FROM replies WHERE id=?"), (reply_id,))
    conn.commit()
    conn.close()


def reply_counts() -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT priority, COUNT(*) as cnt FROM replies WHERE status != 'Archived' GROUP BY priority")
    rows = c.fetchall()
    conn.close()
    counts = {"High": 0, "Medium": 0, "Low": 0}
    for r in _fetchall(rows):
        counts[r["priority"]] = r["cnt"]
    return counts


# ── EMAIL LOGS ─────────────────────────────────────────────────────────────────

def log_email(lead_id: int, subject: str, body: str, template_used: str, sequence_step: int) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO email_logs (lead_id, subject, body, template_used, sequence_step) VALUES (?,?,?,?,?)")
    if _is_pg():
        sql += " RETURNING id"
    c.execute(sql, (lead_id, subject, body, template_used, sequence_step))
    eid = _lastrow(c, "email_logs")
    conn.commit()
    conn.close()
    return eid


def get_email_logs(lead_id: int) -> list:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM email_logs WHERE lead_id=? ORDER BY sent_at DESC"), (lead_id,))
    rows = c.fetchall()
    conn.close()
    return _fetchall(rows)


def mark_email_opened(email_log_id: int):
    conn = get_db()
    conn.cursor().execute(
        _q("UPDATE email_logs SET opened=1, open_count=open_count+1 WHERE id=?"), (email_log_id,)
    )
    conn.commit()
    conn.close()


def mark_email_clicked(email_log_id: int):
    conn = get_db()
    conn.cursor().execute(_q("UPDATE email_logs SET clicked=1 WHERE id=?"), (email_log_id,))
    conn.commit()
    conn.close()


def mark_email_bounced(email_log_id: int, reason: str = ""):
    conn = get_db()
    conn.cursor().execute(
        _q("UPDATE email_logs SET bounced=1, status='bounced', bounce_reason=? WHERE id=?"),
        (reason, email_log_id),
    )
    conn.commit()
    conn.close()


# ── TASKS ──────────────────────────────────────────────────────────────────────

def create_task(lead_id: int, subject: str, due_date: str, priority: str = "Medium", description: str = "") -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO tasks (lead_id, subject, due_date, priority, description) VALUES (?,?,?,?,?)")
    if _is_pg():
        sql += " RETURNING id"
    c.execute(sql, (lead_id, subject, due_date, priority, description))
    tid = _lastrow(c, "tasks")
    conn.commit()
    conn.close()
    return tid


def get_tasks(lead_id: int = None, status: str = None) -> list:
    conn = get_db()
    c = conn.cursor()
    query = """
        SELECT t.*, l.first_name, l.last_name, l.company, l.email
        FROM tasks t
        JOIN leads l ON t.lead_id = l.id
        WHERE 1=1
    """
    params = []
    if lead_id:
        query += _q(" AND t.lead_id = ?")
        params.append(lead_id)
    if status:
        query += _q(" AND t.status = ?")
        params.append(status)
    query += " ORDER BY t.due_date ASC"
    c.execute(query, params)
    rows = c.fetchall()
    conn.close()
    return _fetchall(rows)


def complete_task(task_id: int):
    conn = get_db()
    conn.cursor().execute(_q("UPDATE tasks SET status='Completed' WHERE id=?"), (task_id,))
    conn.commit()
    conn.close()


# ── STATS ──────────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    conn = get_db()
    c = conn.cursor()

    def scalar(sql, params=()):
        c.execute(sql, params)
        row = c.fetchone()
        return list(dict(row).values())[0] if row else 0

    today_fn = "CURRENT_DATE" if _is_pg() else "DATE('now')"

    total_leads   = scalar("SELECT COUNT(*) as n FROM leads")
    hot_leads     = scalar(_q("SELECT COUNT(*) as n FROM leads WHERE status=?"), ('Hot',))
    new_leads     = scalar(_q("SELECT COUNT(*) as n FROM leads WHERE status=?"), ('New',))
    emails_sent   = scalar("SELECT COUNT(*) as n FROM email_logs")
    emails_today  = scalar(f"SELECT COUNT(*) as n FROM email_logs WHERE DATE(sent_at)={today_fn}")
    pending_tasks = scalar(_q("SELECT COUNT(*) as n FROM tasks WHERE status!=?"), ('Completed',))

    c.execute("SELECT status, COUNT(*) as cnt FROM leads GROUP BY status")
    status_counts = {r["status"]: r["cnt"] for r in _fetchall(c.fetchall())}

    c.execute("SELECT * FROM leads ORDER BY created_at DESC LIMIT 5")
    recent_leads = _fetchall(c.fetchall())

    c.execute("""
        SELECT e.*, l.first_name, l.last_name, l.company
        FROM email_logs e JOIN leads l ON e.lead_id=l.id
        ORDER BY e.sent_at DESC LIMIT 5
    """)
    recent_emails = _fetchall(c.fetchall())

    conn.close()
    return {
        "total_leads"  : total_leads,
        "hot_leads"    : hot_leads,
        "new_leads"    : new_leads,
        "emails_sent"  : emails_sent,
        "emails_today" : emails_today,
        "pending_tasks": pending_tasks,
        "status_counts": status_counts,
        "recent_leads" : recent_leads,
        "recent_emails": recent_emails,
    }

# ── INVALID LEADS WITH BOUNCE REASON ──────────────────────────────────────────

def get_invalid_leads_with_bounce() -> list:
    """Return Invalid leads joined with their latest bounce reason from email_logs."""
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT l.*,
               el.bounce_reason,
               el.sent_at as bounced_at
        FROM leads l
        LEFT JOIN (
            SELECT lead_id, bounce_reason, sent_at
            FROM email_logs
            WHERE bounced = 1
            ORDER BY sent_at DESC
        ) el ON el.lead_id = l.id
        WHERE l.status = 'Invalid'
        ORDER BY l.updated_at DESC
    """)
    rows = _fetchall(c.fetchall())
    conn.close()
    # Deduplicate (keep first/latest per lead)
    seen = set()
    result = []
    for r in rows:
        if r["id"] not in seen:
            seen.add(r["id"])
            result.append(r)
    return result


# ── DUE EMAILS ────────────────────────────────────────────────────────────────

def get_due_email_leads() -> list:
    """
    Return leads where the next sequence email is due today or overdue.
    Logic per template schedule:
      step 0 → always due (never emailed)
      step 1 → due if days_since_last >= schedule[1] - schedule[0]
      step 2 → due if days_since_last >= schedule[2] - schedule[1]
      step 3 → complete, skip
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz
    SCHEDULE = {"A": [0,4,9], "B": [0,5,12], "C": [0,4,10], "D": [0,4,9]}
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        SELECT id, first_name, last_name, email, company, country,
               status, email_template, email_sequence_step, last_email_sent
        FROM leads
        WHERE email_sequence_step < 3
        AND status NOT IN ('Unsubscribed','Invalid')
        ORDER BY last_email_sent ASC NULLS FIRST
    """)
    rows = _fetchall(c.fetchall())
    conn.close()

    now = _dt.now(_tz.utc).replace(tzinfo=None)
    due = []
    for lead in rows:
        step = lead.get("email_sequence_step", 0) or 0
        tpl  = lead.get("email_template", "A") or "A"
        sched = SCHEDULE.get(tpl, SCHEDULE["A"])

        if step == 0:
            # Never emailed — always due
            lead["next_email_due"] = "Now"
            lead["days_overdue"]   = 0
            due.append(lead)
        else:
            last_sent = lead.get("last_email_sent")
            if not last_sent:
                lead["next_email_due"] = "Now"
                lead["days_overdue"]   = 0
                due.append(lead)
                continue
            if isinstance(last_sent, str):
                try:
                    last_sent = _dt.fromisoformat(last_sent[:19])
                except Exception:
                    continue
            gap_needed = sched[step] - sched[step - 1]
            due_date   = last_sent + _td(days=gap_needed)
            days_diff  = (now - due_date).days
            if days_diff >= 0:
                lead["next_email_due"] = due_date.strftime("%b %d, %Y")
                lead["days_overdue"]   = days_diff
                due.append(lead)
            else:
                # Not due yet — include with future date so dashboard can show upcoming too
                lead["next_email_due"] = due_date.strftime("%b %d, %Y")
                lead["days_overdue"]   = days_diff  # negative = days remaining
                due.append(lead)
    # Sort: overdue first (days_overdue desc), then upcoming
    due.sort(key=lambda x: -x["days_overdue"])
    return due


# ── INTERNAL NOTES ─────────────────────────────────────────────────────────────

def get_notes(pinned_first=True) -> list:
    conn = get_db()
    c = conn.cursor()
    order = "ORDER BY is_pinned DESC, created_at DESC"
    c.execute(f"SELECT * FROM internal_notes {order}")
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows

def create_note(title: str, body: str, created_by: str, is_pinned: int = 0) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO internal_notes (title, body, created_by, is_pinned, created_at, updated_at) VALUES (?,?,?,?,?,?)" + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, (title, body, created_by, is_pinned, datetime.utcnow().isoformat(), datetime.utcnow().isoformat()))
    nid = _lastrow(c, "internal_notes")
    conn.commit(); conn.close()
    return nid

def update_note(note_id: int, title: str, body: str, is_pinned: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("UPDATE internal_notes SET title=?, body=?, is_pinned=?, updated_at=? WHERE id=?"),
              (title, body, is_pinned, datetime.utcnow().isoformat(), note_id))
    conn.commit(); conn.close()

def delete_note(note_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM internal_notes WHERE id=?"), (note_id,))
    conn.commit(); conn.close()


# ── RESPONSIBILITIES ───────────────────────────────────────────────────────────

def get_responsibilities(assigned_to=None) -> list:
    conn = get_db()
    c = conn.cursor()
    if assigned_to:
        c.execute(_q("SELECT * FROM responsibilities WHERE assigned_to=? ORDER BY created_at DESC"), (assigned_to,))
    else:
        c.execute("SELECT * FROM responsibilities ORDER BY assigned_to, created_at DESC")
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows

def create_responsibility(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO responsibilities (title, description, assigned_to, assigned_by, category, status, created_at) VALUES (?,?,?,?,?,?,?)" + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, (data["title"], data.get("description",""), data["assigned_to"], data["assigned_by"],
                    data.get("category","General"), data.get("status","Active"), datetime.utcnow().isoformat()))
    rid = _lastrow(c, "responsibilities")
    conn.commit(); conn.close()
    return rid

def update_responsibility_status(rid: int, status: str):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("UPDATE responsibilities SET status=? WHERE id=?"), (status, rid))
    conn.commit(); conn.close()

def delete_responsibility(rid: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM responsibilities WHERE id=?"), (rid,))
    conn.commit(); conn.close()


# ── TEAM TASKS ─────────────────────────────────────────────────────────────────

def get_team_tasks(assigned_to=None, status=None) -> list:
    conn = get_db()
    c = conn.cursor()
    q = "SELECT t.*, l.first_name as lead_fname, l.last_name as lead_lname, l.company as lead_company FROM team_tasks t LEFT JOIN leads l ON t.lead_id = l.id WHERE 1=1"
    params = []
    if assigned_to and assigned_to != "all":
        q += _q(" AND (t.assigned_to=? OR t.assigned_to='all')")
        params.append(assigned_to)
    if status:
        q += _q(" AND t.status=?")
        params.append(status)
    q += " ORDER BY t.due_date ASC NULLS LAST, t.created_at DESC"
    c.execute(q, params)
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows

def create_team_task(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO team_tasks (title, description, assigned_to, assigned_by, lead_id, due_date, priority, status, reminder_at, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)" + (" RETURNING id" if _is_pg() else ""))
    now = datetime.utcnow().isoformat()
    c.execute(sql, (data["title"], data.get("description",""), data["assigned_to"], data["assigned_by"],
                    data.get("lead_id"), data.get("due_date"), data.get("priority","Medium"),
                    data.get("status","Pending"), data.get("reminder_at"), now, now))
    tid = _lastrow(c, "team_tasks")
    conn.commit(); conn.close()
    return tid

def update_team_task_status(tid: int, status: str):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("UPDATE team_tasks SET status=?, updated_at=? WHERE id=?"), (status, datetime.utcnow().isoformat(), tid))
    conn.commit(); conn.close()

def delete_team_task(tid: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM team_tasks WHERE id=?"), (tid,))
    conn.commit(); conn.close()

def get_due_reminders() -> list:
    """Get team tasks with reminder_at <= now and reminder_sent=0."""
    conn = get_db()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    c.execute(_q("SELECT * FROM team_tasks WHERE reminder_at IS NOT NULL AND reminder_at <= ? AND reminder_sent=0 AND status != 'Done'"), (now,))
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows

def mark_reminder_sent(tid: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("UPDATE team_tasks SET reminder_sent=1 WHERE id=?"), (tid,))
    conn.commit(); conn.close()

# ── WHATSAPP LOGS ──────────────────────────────────────────────────────────────

def log_whatsapp(lead_id: int, phone: str, message: str, sent_by: str, method: str = "manual", status: str = "sent"):
    conn = get_db()
    c = conn.cursor()
    sql = _q("INSERT INTO whatsapp_logs (lead_id, phone, message, sent_by, method, status, sent_at) VALUES (?,?,?,?,?,?,?)" + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, (lead_id, phone, message, sent_by, method, status, datetime.utcnow().isoformat()))
    conn.commit(); conn.close()

def get_whatsapp_logs(lead_id: int) -> list:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM whatsapp_logs WHERE lead_id=? ORDER BY sent_at DESC"), (lead_id,))
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows

def get_whatsapp_stats() -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) as n FROM whatsapp_logs")
    total = (_fetchone(c.fetchone()) or {}).get("n", 0)
    today_fn = "CURRENT_DATE" if _is_pg() else "date('now')"
    c.execute(f"SELECT COUNT(*) as n FROM whatsapp_logs WHERE DATE(sent_at)={today_fn}")
    today = (_fetchone(c.fetchone()) or {}).get("n", 0)
    conn.close()
    return {"total": total, "today": today}


# ── EXPENSES ──────────────────────────────────────────────────────────────────

EXPENSE_CATEGORIES = {
    "Common": [
        "Electricity & Utilities", "Internet & Telecom", "Office Supplies & Stationery",
        "Software Subscriptions", "Marketing & Advertising", "Travel & Transportation",
        "Meals & Entertainment", "Professional Services", "Maintenance & Repairs",
        "Miscellaneous Common",
    ],
    "Project": [
        "Labour / Manpower", "Materials & Supplies", "Equipment & Tools",
        "Subcontractor / Vendor", "Site Visits & Travel", "Software / Licenses",
        "Documentation & Legal", "Miscellaneous Project",
    ],
    "Partner": [
        "Travel Reimbursement", "Meal Reimbursement", "Client Entertainment",
        "Training & Development", "Professional Membership", "Other Reimbursement",
    ],
}

INCOME_CATEGORIES = [
    "Project Revenue", "Consulting Fees", "Retainer / AMC",
    "Service Charges", "Grant / Subsidy", "Salary / Draw", "Other Income",
]

PARTNERS = {
    "hirakraj": {"name": "Hirakraj Bapat", "role": "CEO", "color": "#2980B9"},
    "jenish"  : {"name": "Jenish Patel",   "role": "CTO", "color": "#8E44AD"},
    "tirth"   : {"name": "Tirth Patel",    "role": "COO", "color": "#E74C3C"},
    "kishan"  : {"name": "Kishan Batavia", "role": "CFO", "color": "#16A085"},
}

PROJECTS = ["Extension", "Project Beta", "Project Gamma", "Project Delta", "Project Epsilon"]


def get_expenses(partner=None, month_year=None, expense_type=None, project=None) -> list:
    conn = get_db()
    c = conn.cursor()
    sql = "SELECT * FROM expenses WHERE 1=1"
    params = []
    if partner:
        sql += _q(" AND partner=?"); params.append(partner)
    if month_year:
        sql += _q(" AND month_year=?"); params.append(month_year)
    if expense_type:
        sql += _q(" AND expense_type=?"); params.append(expense_type)
    if project:
        sql += _q(" AND project_name=?"); params.append(project)
    sql += " ORDER BY date DESC, id DESC"
    c.execute(sql, params)
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows


def create_expense(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _named("""
        INSERT INTO expenses (date, month_year, partner, expense_type, project_name, category, description, amount, created_by)
        VALUES (:date, :month_year, :partner, :expense_type, :project_name, :category, :description, :amount, :created_by)
    """ + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, data)
    lid = _lastrow(c, "expenses")
    conn.commit()
    conn.close()
    return lid


def update_expense(expense_id: int, data: dict):
    conn = get_db()
    c = conn.cursor()
    c.execute(_named("""
        UPDATE expenses SET date=:date, month_year=:month_year, partner=:partner,
            expense_type=:expense_type, project_name=:project_name, category=:category,
            description=:description, amount=:amount WHERE id=:id
    """), {**data, "id": expense_id})
    conn.commit()
    conn.close()


def delete_expense(expense_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM expenses WHERE id=?"), (expense_id,))
    conn.commit()
    conn.close()


def get_expense_summary() -> dict:
    """Returns totals by month, partner, type, and category."""
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT month_year, SUM(amount) as total FROM expenses GROUP BY month_year ORDER BY MIN(date)")
    by_month = _fetchall(c.fetchall())

    c.execute("SELECT partner, SUM(amount) as total FROM expenses GROUP BY partner ORDER BY total DESC")
    by_partner = _fetchall(c.fetchall())

    c.execute("SELECT expense_type, SUM(amount) as total FROM expenses GROUP BY expense_type")
    by_type = _fetchall(c.fetchall())

    c.execute("SELECT category, SUM(amount) as total FROM expenses GROUP BY category ORDER BY total DESC LIMIT 10")
    by_category = _fetchall(c.fetchall())

    c.execute("SELECT COALESCE(SUM(amount),0) as total FROM expenses")
    total_exp = (_fetchone(c.fetchone()) or {}).get("total", 0)

    c.execute("SELECT COALESCE(SUM(amount),0) as total FROM income_entries")
    total_inc = (_fetchone(c.fetchone()) or {}).get("total", 0)

    conn.close()
    return {
        "by_month"   : by_month,
        "by_partner" : by_partner,
        "by_type"    : by_type,
        "by_category": by_category,
        "total_expenses": float(total_exp or 0),
        "total_income"  : float(total_inc or 0),
    }


# ── INCOME ────────────────────────────────────────────────────────────────────

def get_income(month_year=None, category=None) -> list:
    conn = get_db()
    c = conn.cursor()
    sql = "SELECT * FROM income_entries WHERE 1=1"
    params = []
    if month_year:
        sql += _q(" AND month_year=?"); params.append(month_year)
    if category:
        sql += _q(" AND income_category=?"); params.append(category)
    sql += " ORDER BY date DESC, id DESC"
    c.execute(sql, params)
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows


def create_income(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    sql = _named("""
        INSERT INTO income_entries (date, month_year, client_source, income_category, description, amount, created_by)
        VALUES (:date, :month_year, :client_source, :income_category, :description, :amount, :created_by)
    """ + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, data)
    lid = _lastrow(c, "income_entries")
    conn.commit()
    conn.close()
    return lid


def update_income(income_id: int, data: dict):
    conn = get_db()
    c = conn.cursor()
    c.execute(_named("""
        UPDATE income_entries SET date=:date, month_year=:month_year, client_source=:client_source,
            income_category=:income_category, description=:description, amount=:amount WHERE id=:id
    """), {**data, "id": income_id})
    conn.commit()
    conn.close()


def delete_income(income_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM income_entries WHERE id=?"), (income_id,))
    conn.commit()
    conn.close()


# ── PROJECTS ──────────────────────────────────────────────────────────────────

def get_projects(status=None) -> list:
    conn = get_db()
    c = conn.cursor()
    if status:
        c.execute(_q("SELECT * FROM projects WHERE status=? ORDER BY created_at DESC"), (status,))
    else:
        c.execute("SELECT * FROM projects ORDER BY created_at DESC")
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows


def get_project(project_id: int) -> dict | None:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM projects WHERE id=?"), (project_id,))
    row = _fetchone(c.fetchone())
    conn.close()
    return row


def create_project(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    sql = _named("""
        INSERT INTO projects (name, client_name, client_address, client_gstin,
            start_date, end_date, status, total_value, description, lead_id, created_by, created_at, updated_at)
        VALUES (:name,:client_name,:client_address,:client_gstin,
            :start_date,:end_date,:status,:total_value,:description,:lead_id,:created_by,:now,:now)
    """ + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, {**data, "now": now})
    pid = _lastrow(c, "projects")
    conn.commit()
    conn.close()
    return pid


def update_project(project_id: int, data: dict):
    conn = get_db()
    c = conn.cursor()
    c.execute(_named("""
        UPDATE projects SET name=:name, client_name=:client_name, client_address=:client_address,
            client_gstin=:client_gstin, start_date=:start_date, end_date=:end_date,
            status=:status, total_value=:total_value, description=:description,
            updated_at=:updated_at WHERE id=:id
    """), {**data, "id": project_id, "updated_at": datetime.utcnow().isoformat()})
    conn.commit()
    conn.close()


def delete_project(project_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM projects WHERE id=?"), (project_id,))
    conn.commit()
    conn.close()


# ── INVOICES ──────────────────────────────────────────────────────────────────

def get_invoices(status=None, project_id=None) -> list:
    conn = get_db()
    c = conn.cursor()
    sql = """SELECT i.*, p.name as project_name
             FROM invoices i LEFT JOIN projects p ON i.project_id = p.id
             WHERE 1=1"""
    params = []
    if status:
        sql += _q(" AND i.status=?"); params.append(status)
    if project_id:
        sql += _q(" AND i.project_id=?"); params.append(project_id)
    sql += " ORDER BY i.date DESC, i.id DESC"
    c.execute(sql, params)
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows


def get_invoice(invoice_id: int) -> dict | None:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("""SELECT i.*, p.name as project_name
                    FROM invoices i LEFT JOIN projects p ON i.project_id = p.id
                    WHERE i.id=?"""), (invoice_id,))
    row = _fetchone(c.fetchone())
    conn.close()
    return row


def get_invoice_items(invoice_id: int) -> list:
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT * FROM invoice_items WHERE invoice_id=? ORDER BY sort_order, id"), (invoice_id,))
    rows = _fetchall(c.fetchall())
    conn.close()
    return rows


def next_invoice_number(date_str: str) -> str:
    """Generate next invoice number in format IN/MMYY/NN."""
    from datetime import datetime as dt
    d = dt.strptime(date_str[:10], "%Y-%m-%d")
    prefix = f"IN/{d.strftime('%m%y')}/"
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("SELECT invoice_no FROM invoices WHERE invoice_no LIKE ? ORDER BY invoice_no DESC"), (prefix + "%",))
    rows = _fetchall(c.fetchall())
    conn.close()
    if not rows:
        return prefix + "01"
    last = rows[0]["invoice_no"]
    try:
        n = int(last.split("/")[-1]) + 1
    except Exception:
        n = len(rows) + 1
    return prefix + str(n).zfill(2)


def create_invoice(data: dict, items: list) -> int:
    conn = get_db()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    sql = _named("""
        INSERT INTO invoices (invoice_no, date, project_id, client_name, client_address, client_gstin,
            lut_number, gst_rate, subtotal, gst_amount, total, notes, status, created_by, created_at, updated_at)
        VALUES (:invoice_no,:date,:project_id,:client_name,:client_address,:client_gstin,
            :lut_number,:gst_rate,:subtotal,:gst_amount,:total,:notes,:status,:created_by,:now,:now)
    """ + (" RETURNING id" if _is_pg() else ""))
    c.execute(sql, {**data, "now": now})
    inv_id = _lastrow(c, "invoices")
    for i, item in enumerate(items):
        c.execute(_named("""
            INSERT INTO invoice_items (invoice_id, description, sac_code, unit, rate, amount, sort_order)
            VALUES (:invoice_id,:description,:sac_code,:unit,:rate,:amount,:sort_order)
        """), {**item, "invoice_id": inv_id, "sort_order": i})
    conn.commit()
    conn.close()
    return inv_id


def update_invoice(invoice_id: int, data: dict, items: list):
    conn = get_db()
    c = conn.cursor()
    c.execute(_named("""
        UPDATE invoices SET invoice_no=:invoice_no, date=:date, project_id=:project_id,
            client_name=:client_name, client_address=:client_address, client_gstin=:client_gstin,
            lut_number=:lut_number, gst_rate=:gst_rate, subtotal=:subtotal, gst_amount=:gst_amount,
            total=:total, notes=:notes, status=:status, updated_at=:updated_at WHERE id=:id
    """), {**data, "id": invoice_id, "updated_at": datetime.utcnow().isoformat()})
    c.execute(_q("DELETE FROM invoice_items WHERE invoice_id=?"), (invoice_id,))
    for i, item in enumerate(items):
        c.execute(_named("""
            INSERT INTO invoice_items (invoice_id, description, sac_code, unit, rate, amount, sort_order)
            VALUES (:invoice_id,:description,:sac_code,:unit,:rate,:amount,:sort_order)
        """), {**item, "invoice_id": invoice_id, "sort_order": i})
    conn.commit()
    conn.close()


def update_invoice_status(invoice_id: int, status: str):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("UPDATE invoices SET status=?, updated_at=? WHERE id=?"),
              (status, datetime.utcnow().isoformat(), invoice_id))
    conn.commit()
    conn.close()


def delete_invoice(invoice_id: int):
    conn = get_db()
    c = conn.cursor()
    c.execute(_q("DELETE FROM invoice_items WHERE invoice_id=?"), (invoice_id,))
    c.execute(_q("DELETE FROM invoices WHERE id=?"), (invoice_id,))
    conn.commit()
    conn.close()


def get_invoice_summary() -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT status, COUNT(*) as cnt, COALESCE(SUM(total),0) as total FROM invoices GROUP BY status")
    rows = _fetchall(c.fetchall())
    conn.close()
    summary = {"Draft": {"count": 0, "total": 0}, "Sent": {"count": 0, "total": 0},
               "Paid": {"count": 0, "total": 0}, "Overdue": {"count": 0, "total": 0}}
    for r in rows:
        s = r.get("status", "Draft")
        if s in summary:
            summary[s] = {"count": r["cnt"], "total": float(r["total"])}
    summary["grand_total"] = sum(v["total"] for v in summary.values())
    return summary


# ── APP CONFIG (key-value store) ───────────────────────────────────────────────

def get_config(key: str, default: str = "") -> str:
    """Read a runtime config value from app_config table."""
    try:
        conn = get_db()
        c = conn.cursor()
        c.execute(_q("SELECT value FROM app_config WHERE key=?"), (key,))
        row = c.fetchone()
        conn.close()
        if row:
            v = row["value"] if _is_pg() else row[0]
            return v if v is not None else default
    except Exception:
        pass
    return default


def set_config(key: str, value: str):
    """Upsert a runtime config value into app_config table."""
    conn = get_db()
    c = conn.cursor()
    now = datetime.utcnow().isoformat()
    if _is_pg():
        c.execute(
            "INSERT INTO app_config (key, value, updated_at) VALUES (%s, %s, %s) "
            "ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value, updated_at=EXCLUDED.updated_at",
            (key, value, now),
        )
    else:
        c.execute(
            "INSERT OR REPLACE INTO app_config (key, value, updated_at) VALUES (?, ?, ?)",
            (key, value, now),
        )
    conn.commit()
    conn.close()
