"""
BIM CRM — Mobile REST API
Mounted at /api/v1/* in app.py
Auth: Bearer token == API_SECRET env var (default: bim-mobile-2025)
User identity: X-User-Email header (maps Firebase email → CRM username)
"""
import os
from datetime import datetime
from flask import Blueprint, jsonify, request
import database as db

API_SECRET = os.getenv("API_SECRET", "bim-mobile-2025")

api = Blueprint("api_v1", __name__, url_prefix="/api/v1")

# Firebase email → CRM username mapping
_EMAIL_TO_USERNAME = {
    "kishan.batavia@biminfrasolutions.in": "kishan",
    "ceo@biminfrasolutions.com":           "hirakraj",
    "coo@biminfrasolutions.com":           "tirth",
    "services@biminfrasolutions.in":       "jenish",
}

_USERNAME_PROFILES = {
    "kishan":   {"name": "Kishan Batavia",  "role": "Admin / CFO",  "avatar_color": "#D4A017"},
    "hirakraj": {"name": "Hirakraj Bapat",  "role": "CEO",          "avatar_color": "#64B5F6"},
    "tirth":    {"name": "Tirth Patel",     "role": "COO",          "avatar_color": "#81C784"},
    "jenish":   {"name": "Jenish Patel",    "role": "CTO / Services","avatar_color": "#CE93D8"},
}


def _auth():
    """Return True if request carries valid Bearer token."""
    h = request.headers.get("Authorization", "")
    if h.startswith("Bearer "):
        return h[7:] == API_SECRET
    return False


def _require_auth():
    if not _auth():
        return jsonify({"error": "unauthorized"}), 401
    return None


def _caller_username() -> str:
    """Extract CRM username from X-User-Email header, fallback to 'mobile'."""
    email = request.headers.get("X-User-Email", "").lower().strip()
    return _EMAIL_TO_USERNAME.get(email, "mobile")


def _cors(response):
    """Add CORS headers so Flutter web (localhost) can call this API."""
    response.headers["Access-Control-Allow-Origin"]  = "*"
    response.headers["Access-Control-Allow-Headers"] = "Authorization, Content-Type, X-User-Email"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, PATCH, DELETE, OPTIONS"
    return response


@api.after_request
def after_request(response):
    return _cors(response)


@api.route("/<path:_>", methods=["OPTIONS"])
@api.route("/", methods=["OPTIONS"])
def options_handler(_=""):
    from flask import Response
    return _cors(Response(status=200))


# ── AUTH / PROFILE ─────────────────────────────────────────────────────────────

@api.route("/auth/me")
def auth_me():
    """Return CRM profile for the calling user (identified by X-User-Email)."""
    err = _require_auth()
    if err: return err
    email = request.headers.get("X-User-Email", "").lower().strip()
    username = _EMAIL_TO_USERNAME.get(email)
    if not username:
        return jsonify({"error": "unknown user", "email": email}), 404
    profile = _USERNAME_PROFILES.get(username, {})
    return jsonify({
        "username":    username,
        "email":       email,
        "name":        profile.get("name", username),
        "role":        profile.get("role", ""),
        "avatar_color":profile.get("avatar_color", "#64B5F6"),
    })


@api.route("/auth/team")
def auth_team():
    """Return all team members with their profiles."""
    err = _require_auth()
    if err: return err
    team = []
    for email, username in _EMAIL_TO_USERNAME.items():
        profile = _USERNAME_PROFILES.get(username, {})
        team.append({
            "username":    username,
            "email":       email,
            "name":        profile.get("name", username),
            "role":        profile.get("role", ""),
            "avatar_color":profile.get("avatar_color", "#64B5F6"),
        })
    return jsonify({"team": team})


# ── DASHBOARD ──────────────────────────────────────────────────────────────────

@api.route("/dashboard/metrics")
def dashboard_metrics():
    err = _require_auth()
    if err: return err
    try:
        stats = db.get_stats()
        sc = stats.get("status_counts", {})
        total = stats.get("total_leads", 0)
        closed = sc.get("Closed", 0)
        return jsonify({
            "total_leads":     total,
            "new":             sc.get("New", 0),
            "hot":             sc.get("Hot", 0),
            "contacted":       sc.get("Contacted", 0),
            "proposal":        sc.get("Proposal", 0),
            "closed":          closed,
            "unsubscribed":    sc.get("Unsubscribed", 0),
            "invalid":         sc.get("Invalid", 0),
            "emails_sent":     stats.get("emails_sent", 0),
            "emails_today":    stats.get("emails_today", 0),
            "pending_tasks":   stats.get("pending_tasks", 0),
            "conversion_rate": round(closed / total * 100, 1) if total else 0,
            "status_counts":   sc,
            "recent_leads":    stats.get("recent_leads", []),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── LEADS ──────────────────────────────────────────────────────────────────────

@api.route("/leads")
def list_leads():
    err = _require_auth()
    if err: return err
    try:
        search   = request.args.get("q") or request.args.get("search")
        status   = request.args.get("status")
        country  = request.args.get("country")
        page     = int(request.args.get("page", 1))
        per_page = int(request.args.get("per_page", 50))
        leads = db.get_all_leads(search=search, status=status, country=country)
        start = (page - 1) * per_page
        paginated = leads[start: start + per_page]
        return jsonify({
            "leads": [_lead_out(l) for l in paginated],
            "total": len(leads),
            "page":  page,
            "has_more": (start + per_page) < len(leads),
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/leads/<int:lead_id>")
def get_lead(lead_id):
    err = _require_auth()
    if err: return err
    lead = db.get_lead(lead_id)
    if not lead:
        return jsonify({"error": "Not found"}), 404
    return jsonify(_lead_out(lead))


@api.route("/leads", methods=["POST"])
def create_lead():
    err = _require_auth()
    if err: return err
    try:
        data = request.get_json(force=True) or {}
        payload = _lead_in(data)
        payload.setdefault("status", "New")
        payload.setdefault("assigned_to", _caller_username())
        lid = db.create_lead(payload)
        return jsonify(_lead_out(db.get_lead(lid))), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/leads/<int:lead_id>", methods=["PATCH", "PUT"])
def update_lead(lead_id):
    err = _require_auth()
    if err: return err
    if not db.get_lead(lead_id):
        return jsonify({"error": "Not found"}), 404
    try:
        data = request.get_json(force=True) or {}
        payload = _lead_in(data)
        db.update_lead(lead_id, payload)
        return jsonify(_lead_out(db.get_lead(lead_id)))
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/leads/<int:lead_id>/status", methods=["PATCH", "POST"])
def update_lead_status(lead_id):
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    status = data.get("status", "New")
    db.update_lead_status(lead_id, status)
    return jsonify(_lead_out(db.get_lead(lead_id)))


@api.route("/leads/<int:lead_id>", methods=["DELETE"])
def delete_lead(lead_id):
    err = _require_auth()
    if err: return err
    db.delete_lead(lead_id)
    return jsonify({"ok": True})


# ── TASKS ──────────────────────────────────────────────────────────────────────

@api.route("/tasks")
def list_tasks():
    err = _require_auth()
    if err: return err
    lead_id = request.args.get("lead_id", type=int)
    status  = request.args.get("status")
    tasks = db.get_tasks(lead_id=lead_id, status=status)
    return jsonify({"tasks": tasks})


@api.route("/tasks", methods=["POST"])
def create_task():
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    try:
        tid = db.create_task(
            lead_id=int(data["lead_id"]),
            subject=data["subject"],
            due_date=data.get("due_date", ""),
            priority=data.get("priority", "Medium"),
            description=data.get("description", ""),
        )
        return jsonify({"id": tid}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/tasks/<int:task_id>/complete", methods=["POST", "PATCH"])
def complete_task(task_id):
    err = _require_auth()
    if err: return err
    db.complete_task(task_id)
    return jsonify({"ok": True})


# ── REPLIES ────────────────────────────────────────────────────────────────────

@api.route("/replies")
def list_replies():
    err = _require_auth()
    if err: return err
    priority = request.args.get("priority")
    status   = request.args.get("status")
    replies = db.get_replies(priority=priority, status=status)
    return jsonify({"replies": replies, "counts": db.reply_counts()})


@api.route("/replies/<int:reply_id>/status", methods=["PATCH", "POST"])
def update_reply_status(reply_id):
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    db.update_reply(reply_id, status=data.get("status"))
    return jsonify({"ok": True})


# ── TEAM TASKS ─────────────────────────────────────────────────────────────────

@api.route("/team-tasks")
def list_team_tasks():
    err = _require_auth()
    if err: return err
    assigned_to = request.args.get("assigned_to")
    status      = request.args.get("status")
    # Default to current user's tasks
    if not assigned_to:
        assigned_to = _caller_username()
    tasks = db.get_team_tasks(assigned_to=assigned_to, status=status)
    return jsonify({"tasks": tasks})


@api.route("/team-tasks", methods=["POST"])
def create_team_task():
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    data.setdefault("assigned_by", _caller_username())
    try:
        tid = db.create_team_task(data)
        return jsonify({"id": tid}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@api.route("/team-tasks/<int:tid>/status", methods=["PATCH", "POST"])
def update_team_task_status(tid):
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    db.update_team_task_status(tid, data.get("status", "Done"))
    return jsonify({"ok": True})


# ── PROJECTS ───────────────────────────────────────────────────────────────────

@api.route("/projects")
def list_projects():
    err = _require_auth()
    if err: return err
    status = request.args.get("status")
    projects = db.get_projects(status=status)
    return jsonify({"projects": projects})


@api.route("/projects/<int:project_id>")
def get_project(project_id):
    err = _require_auth()
    if err: return err
    p = db.get_project(project_id)
    if not p:
        return jsonify({"error": "Not found"}), 404
    return jsonify(p)


@api.route("/projects", methods=["POST"])
def create_project():
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    data.setdefault("created_by", _caller_username())
    try:
        pid = db.create_project(data)
        return jsonify({"id": pid, **db.get_project(pid)}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── INVOICES ───────────────────────────────────────────────────────────────────

@api.route("/invoices")
def list_invoices():
    err = _require_auth()
    if err: return err
    status     = request.args.get("status")
    project_id = request.args.get("project_id", type=int)
    invoices = db.get_invoices(status=status, project_id=project_id)
    summary  = db.get_invoice_summary()
    return jsonify({"invoices": invoices, "summary": summary})


@api.route("/invoices/<int:invoice_id>")
def get_invoice(invoice_id):
    err = _require_auth()
    if err: return err
    inv = db.get_invoice(invoice_id)
    if not inv:
        return jsonify({"error": "Not found"}), 404
    items = db.get_invoice_items(invoice_id)
    return jsonify({**inv, "items": items})


# ── EXPENSES ───────────────────────────────────────────────────────────────────

@api.route("/expenses")
def list_expenses():
    err = _require_auth()
    if err: return err
    partner    = request.args.get("partner")
    month_year = request.args.get("month_year")
    expenses = db.get_expenses(partner=partner, month_year=month_year)
    summary  = db.get_expense_summary()
    return jsonify({"expenses": expenses, "summary": summary})


@api.route("/expenses", methods=["POST"])
def create_expense():
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    data.setdefault("created_by", _caller_username())
    try:
        eid = db.create_expense(data)
        return jsonify({"id": eid}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── INCOME ────────────────────────────────────────────────────────────────────

@api.route("/income")
def list_income():
    err = _require_auth()
    if err: return err
    month_year = request.args.get("month_year")
    income  = db.get_income(month_year=month_year)
    summary = db.get_expense_summary()
    return jsonify({
        "income": income,
        "total_income":   summary.get("total_income", 0),
        "total_expenses": summary.get("total_expenses", 0),
    })


@api.route("/income", methods=["POST"])
def create_income():
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    data.setdefault("created_by", _caller_username())
    try:
        iid = db.create_income(data)
        return jsonify({"id": iid}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── NOTES ─────────────────────────────────────────────────────────────────────

@api.route("/notes")
def list_notes():
    err = _require_auth()
    if err: return err
    return jsonify({"notes": db.get_notes()})


@api.route("/notes", methods=["POST"])
def create_note():
    err = _require_auth()
    if err: return err
    data = request.get_json(force=True) or {}
    try:
        nid = db.create_note(
            title=data.get("title", ""),
            body=data.get("body", ""),
            created_by=data.get("created_by") or _caller_username(),
            is_pinned=int(data.get("is_pinned", 0)),
        )
        return jsonify({"id": nid}), 201
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── RESPONSIBILITIES ──────────────────────────────────────────────────────────

@api.route("/responsibilities")
def list_responsibilities():
    err = _require_auth()
    if err: return err
    assigned_to = request.args.get("assigned_to") or _caller_username()
    return jsonify({"responsibilities": db.get_responsibilities(assigned_to=assigned_to)})


# ── EMAIL LOGS ────────────────────────────────────────────────────────────────

@api.route("/leads/<int:lead_id>/email-logs")
def lead_email_logs(lead_id):
    err = _require_auth()
    if err: return err
    return jsonify({"email_logs": db.get_email_logs(lead_id)})


# ── PIPELINE ──────────────────────────────────────────────────────────────────

@api.route("/pipeline")
def pipeline():
    err = _require_auth()
    if err: return err
    try:
        all_leads = db.get_all_leads()
        statuses  = ["New", "Hot", "Contacted", "Proposal", "Closed"]
        board = {s: [_lead_out(l) for l in all_leads if l.get("status") == s]
                 for s in statuses}
        return jsonify(board)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── HELPERS ───────────────────────────────────────────────────────────────────

def _lead_out(lead: dict) -> dict:
    """Normalize lead dict for mobile consumption."""
    if not lead:
        return {}
    fname = lead.get("first_name", "") or ""
    lname = lead.get("last_name", "") or ""
    name  = f"{fname} {lname}".strip() or lead.get("name", "")
    return {
        "id":            lead.get("id"),
        "name":          name,
        "first_name":    fname,
        "last_name":     lname,
        "email":         lead.get("email", ""),
        "company":       lead.get("company", ""),
        "title":         lead.get("title"),
        "phone":         lead.get("phone"),
        "website":       lead.get("website"),
        "city":          lead.get("city"),
        "country":       lead.get("country", "India"),
        "industry":      lead.get("industry"),
        "status":        lead.get("status", "New"),
        "priority_score":lead.get("priority_score", 0),
        "services_needed":lead.get("services_needed"),
        "linkedin_url":  lead.get("linkedin_url"),
        "description":   lead.get("description"),
        "source":        lead.get("source"),
        "assigned_to":   lead.get("assigned_to"),
        "notes":         lead.get("description"),
        "email_sequence_step": lead.get("email_sequence_step", 0),
        "last_email_sent":     lead.get("last_email_sent"),
        "created_at":    str(lead.get("created_at", "")),
        "updated_at":    str(lead.get("updated_at", "")),
    }


def _lead_in(data: dict) -> dict:
    """Map mobile payload to DB fields."""
    name = data.get("name", "")
    parts = name.split(" ", 1) if name else []
    return {
        "first_name":    data.get("first_name") or (parts[0] if parts else ""),
        "last_name":     data.get("last_name")  or (parts[1] if len(parts) > 1 else ""),
        "email":         data.get("email", ""),
        "company":       data.get("company", ""),
        "title":         data.get("title", ""),
        "phone":         data.get("phone", ""),
        "website":       data.get("website", ""),
        "city":          data.get("city", ""),
        "country":       data.get("country", "India"),
        "industry":      data.get("industry", ""),
        "status":        data.get("status", "New"),
        "priority_score":int(data.get("priority_score", 0)),
        "services_needed": data.get("services_needed", ""),
        "linkedin_url":  data.get("linkedin_url", ""),
        "description":   data.get("notes") or data.get("description", ""),
        "source":        data.get("source", ""),
    }
