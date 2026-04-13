"""
BIM Infra Solutions — Custom CRM Database (SQLite)
"""
import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "bim_crm.db")


def get_db():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db():
    conn = get_db()
    c = conn.cursor()

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
    # Add bounce columns to existing DB if missing
    try:
        c.execute("ALTER TABLE email_logs ADD COLUMN bounced INTEGER DEFAULT 0")
    except Exception:
        pass
    try:
        c.execute("ALTER TABLE email_logs ADD COLUMN bounce_reason TEXT")
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

    conn.commit()
    conn.close()


# ── LEADS ──────────────────────────────────────────────────────────────────────

def create_lead(data: dict) -> int:
    conn = get_db()
    c = conn.cursor()
    c.execute("""
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
    """, data)
    lid = c.lastrowid
    conn.commit()
    conn.close()
    return lid


def get_lead(lead_id: int) -> dict | None:
    conn = get_db()
    row = conn.execute("SELECT * FROM leads WHERE id = ?", (lead_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def get_all_leads(search=None, status=None, country=None, template=None) -> list:
    conn = get_db()
    query = "SELECT * FROM leads WHERE 1=1"
    params = []

    if search:
        query += " AND (first_name LIKE ? OR last_name LIKE ? OR email LIKE ? OR company LIKE ?)"
        s = f"%{search}%"
        params.extend([s, s, s, s])
    if status:
        query += " AND status = ?"
        params.append(status)
    if country:
        query += " AND country = ?"
        params.append(country)
    if template:
        query += " AND email_template = ?"
        params.append(template)

    query += " ORDER BY priority_score DESC, created_at DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_lead(lead_id: int, data: dict):
    conn = get_db()
    data["id"] = lead_id
    data["updated_at"] = datetime.utcnow().isoformat()
    fields = [k for k in data.keys() if k not in ("id", "created_at")]
    set_clause = ", ".join(f"{f} = :{f}" for f in fields)
    conn.execute(f"UPDATE leads SET {set_clause} WHERE id = :id", data)
    conn.commit()
    conn.close()


def delete_lead(lead_id: int):
    conn = get_db()
    conn.execute("DELETE FROM leads WHERE id = ?", (lead_id,))
    conn.commit()
    conn.close()


def update_lead_status(lead_id: int, status: str, follow_up_stage: str = None):
    conn = get_db()
    if follow_up_stage:
        conn.execute(
            "UPDATE leads SET status=?, follow_up_stage=?, updated_at=? WHERE id=?",
            (status, follow_up_stage, datetime.utcnow().isoformat(), lead_id),
        )
    else:
        conn.execute(
            "UPDATE leads SET status=?, updated_at=? WHERE id=?",
            (status, datetime.utcnow().isoformat(), lead_id),
        )
    conn.commit()
    conn.close()


def advance_sequence_step(lead_id: int):
    conn = get_db()
    now = datetime.utcnow().isoformat()
    conn.execute(
        "UPDATE leads SET email_sequence_step=email_sequence_step+1, last_email_sent=?, updated_at=? WHERE id=?",
        (now, now, lead_id),
    )
    conn.commit()
    conn.close()


# ── REPLIES ───────────────────────────────────────────────────────────────────

def add_reply(lead_id, from_email, subject, body, priority="Medium", source="Manual") -> int:
    conn = get_db()
    c    = conn.cursor()
    c.execute("""
        INSERT INTO replies (lead_id, from_email, subject, body, priority, source)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (lead_id, from_email, subject, body, priority, source))
    rid = c.lastrowid
    conn.commit()
    conn.close()
    return rid

def get_replies(priority=None, status=None) -> list:
    conn  = get_db()
    query = """
        SELECT r.*, l.first_name, l.last_name, l.company
        FROM replies r
        LEFT JOIN leads l ON r.lead_id = l.id
        WHERE 1=1
    """
    params = []
    if priority:
        query += " AND r.priority = ?"
        params.append(priority)
    if status:
        query += " AND r.status = ?"
        params.append(status)
    query += " ORDER BY CASE r.priority WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, r.received_at DESC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]

def update_reply(reply_id: int, priority: str = None, status: str = None):
    conn = get_db()
    if priority:
        conn.execute("UPDATE replies SET priority=? WHERE id=?", (priority, reply_id))
    if status:
        conn.execute("UPDATE replies SET status=? WHERE id=?", (status, reply_id))
    conn.commit()
    conn.close()

def delete_reply(reply_id: int):
    conn = get_db()
    conn.execute("DELETE FROM replies WHERE id=?", (reply_id,))
    conn.commit()
    conn.close()

def reply_counts() -> dict:
    conn = get_db()
    rows = conn.execute(
        "SELECT priority, COUNT(*) as cnt FROM replies WHERE status != 'Archived' GROUP BY priority"
    ).fetchall()
    conn.close()
    counts = {"High": 0, "Medium": 0, "Low": 0}
    for r in rows:
        counts[r["priority"]] = r["cnt"]
    return counts

# ── EMAIL LOGS ─────────────────────────────────────────────────────────────────

def log_email(lead_id: int, subject: str, body: str, template_used: str, sequence_step: int) -> int:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO email_logs (lead_id, subject, body, template_used, sequence_step) VALUES (?,?,?,?,?)",
        (lead_id, subject, body, template_used, sequence_step),
    )
    eid = c.lastrowid
    conn.commit()
    conn.close()
    return eid


def get_email_logs(lead_id: int) -> list:
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM email_logs WHERE lead_id=? ORDER BY sent_at DESC", (lead_id,)
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def mark_email_opened(email_log_id: int):
    conn = get_db()
    conn.execute(
        "UPDATE email_logs SET opened=1, open_count=open_count+1 WHERE id=?",
        (email_log_id,),
    )
    conn.commit()
    conn.close()


def mark_email_clicked(email_log_id: int):
    conn = get_db()
    conn.execute("UPDATE email_logs SET clicked=1 WHERE id=?", (email_log_id,))
    conn.commit()
    conn.close()


def mark_email_bounced(email_log_id: int, reason: str = ""):
    conn = get_db()
    conn.execute(
        "UPDATE email_logs SET bounced=1, status='bounced', bounce_reason=? WHERE id=?",
        (reason, email_log_id),
    )
    conn.commit()
    conn.close()


# ── TASKS ──────────────────────────────────────────────────────────────────────

def create_task(lead_id: int, subject: str, due_date: str, priority: str = "Medium", description: str = "") -> int:
    conn = get_db()
    c = conn.cursor()
    c.execute(
        "INSERT INTO tasks (lead_id, subject, due_date, priority, description) VALUES (?,?,?,?,?)",
        (lead_id, subject, due_date, priority, description),
    )
    tid = c.lastrowid
    conn.commit()
    conn.close()
    return tid


def get_tasks(lead_id: int = None, status: str = None) -> list:
    conn = get_db()
    query = """
        SELECT t.*, l.first_name, l.last_name, l.company, l.email
        FROM tasks t
        JOIN leads l ON t.lead_id = l.id
        WHERE 1=1
    """
    params = []
    if lead_id:
        query += " AND t.lead_id = ?"
        params.append(lead_id)
    if status:
        query += " AND t.status = ?"
        params.append(status)
    query += " ORDER BY t.due_date ASC"
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def complete_task(task_id: int):
    conn = get_db()
    conn.execute("UPDATE tasks SET status='Completed' WHERE id=?", (task_id,))
    conn.commit()
    conn.close()


# ── STATS ──────────────────────────────────────────────────────────────────────

def get_stats() -> dict:
    conn = get_db()

    total_leads   = conn.execute("SELECT COUNT(*) FROM leads").fetchone()[0]
    hot_leads     = conn.execute("SELECT COUNT(*) FROM leads WHERE status='Hot'").fetchone()[0]
    new_leads     = conn.execute("SELECT COUNT(*) FROM leads WHERE status='New'").fetchone()[0]
    emails_sent   = conn.execute("SELECT COUNT(*) FROM email_logs").fetchone()[0]
    emails_today  = conn.execute("SELECT COUNT(*) FROM email_logs WHERE DATE(sent_at)=DATE('now')").fetchone()[0]
    pending_tasks = conn.execute("SELECT COUNT(*) FROM tasks WHERE status!='Completed'").fetchone()[0]

    status_rows   = conn.execute("SELECT status, COUNT(*) as cnt FROM leads GROUP BY status").fetchall()
    status_counts = {r["status"]: r["cnt"] for r in status_rows}

    recent_leads  = conn.execute("SELECT * FROM leads ORDER BY created_at DESC LIMIT 5").fetchall()
    recent_emails = conn.execute("""
        SELECT e.*, l.first_name, l.last_name, l.company
        FROM email_logs e JOIN leads l ON e.lead_id=l.id
        ORDER BY e.sent_at DESC LIMIT 5
    """).fetchall()

    conn.close()
    return {
        "total_leads"  : total_leads,
        "hot_leads"    : hot_leads,
        "new_leads"    : new_leads,
        "emails_sent"  : emails_sent,
        "emails_today" : emails_today,
        "pending_tasks": pending_tasks,
        "status_counts": status_counts,
        "recent_leads" : [dict(r) for r in recent_leads],
        "recent_emails": [dict(r) for r in recent_emails],
    }
