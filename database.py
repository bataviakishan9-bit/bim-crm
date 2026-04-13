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
    return bool(DATABASE_URL)

def get_db():
    if _is_pg():
        import psycopg2
        import psycopg2.extras
        conn = psycopg2.connect(DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)
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
                updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
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
                updated_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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

    conn.commit()
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
                updated_at         = :updated_at
            WHERE username = :username
        """)
    else:
        sql = _named("""
            INSERT INTO user_settings (
                username, sender_email, sender_name,
                zoho_client_id, zoho_client_secret, zoho_refresh_token,
                zoho_dc, zoho_account_id, is_locked, updated_at
            ) VALUES (
                :username, :sender_email, :sender_name,
                :zoho_client_id, :zoho_client_secret, :zoho_refresh_token,
                :zoho_dc, :zoho_account_id, :is_locked, :updated_at
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
