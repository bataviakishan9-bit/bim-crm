"""
BIM Infra Solutions — Zoho Mail Client + Email Templates
"""
import os
import time
import json
import requests
import logging
from dotenv import load_dotenv

load_dotenv()
log = logging.getLogger(__name__)

def _zoho_dc():      return os.getenv("ZOHO_DC", "in")
def _zoho_acct_id(): return os.getenv("ZOHO_MAIL_ACCOUNT_ID", "")
SENDER_EMAIL       = "kishan.batavia@biminfrasolutions.in"
SENDER_NAME        = "Kishan Batavia — BIM Infra Solutions"
CALENDLY_LINK      = "https://calendly.com/kishanbatavia9/30min"
WEBSITE            = "https://www.biminfrasolutions.in"
PHONE              = "+91 94266 35181"
TITLE              = "Co-founder & CFO"
COMPANY_FULL       = "BIM INFRASOLUTIONS LLP"
PORTFOLIO_LINK       = "https://drive.google.com/file/d/1AJ0_5XUJ5JxoHw3cXVJ7jFVh0LOZIkMk/view?usp=sharing"
DRONE_PORTFOLIO_LINK = "https://drive.google.com/file/d/1krfX2CUme0sSI1U6gZ352J8nkvLX9_-c/view?usp=sharing"


class ZohoMailClient:
    """Handles Zoho Mail OAuth2 + sending emails."""

    def __init__(self):
        self._access_token = None
        self._token_expiry = 0

    def get_access_token(self) -> str:
        if time.time() > self._token_expiry - 60:
            self._refresh_token()
        return self._access_token

    def _refresh_token(self):
        # Resolution order: DB (survives redeploys) → env var (Render config)
        # IMPORTANT: do NOT call load_dotenv here — it would override the DB value
        client_id     = os.getenv("ZOHO_CLIENT_ID")
        client_secret = os.getenv("ZOHO_CLIENT_SECRET")
        dc            = os.getenv("ZOHO_DC", "in")

        # DB takes priority for refresh_token so in-app updates persist across deploys
        refresh_token = None
        try:
            import database as _db
            refresh_token = _db.get_config("ZOHO_REFRESH_TOKEN") or None
        except Exception:
            pass
        if not refresh_token:
            refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")

        if not client_id or not client_secret or not refresh_token:
            raise Exception(
                "Zoho credentials missing. Set ZOHO_CLIENT_ID, ZOHO_CLIENT_SECRET, "
                "ZOHO_REFRESH_TOKEN in Render env vars or via Settings → My Settings."
            )

        url = f"https://accounts.zoho.{dc}/oauth/v2/token"
        r = requests.post(url, params={
            "grant_type"   : "refresh_token",
            "client_id"    : client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        if "access_token" not in data:
            raise Exception(f"Token refresh failed: {data}")
        self._access_token = data["access_token"]
        self._token_expiry = time.time() + data.get("expires_in", 3600)
        log.info("Zoho Mail access token refreshed OK.")

    def _get_token_for(self, client_id: str, client_secret: str, refresh_token: str, dc: str) -> str:
        """Get access token using specific credentials (per-user support)."""
        # Use cached token only when using global credentials
        global_cid = os.getenv("ZOHO_CLIENT_ID")
        if client_id == global_cid:
            return self.get_access_token()
        # Per-user: always fetch a fresh token (no cache for now)
        url = f"https://accounts.zoho.{dc}/oauth/v2/token"
        r = requests.post(url, params={
            "grant_type"   : "refresh_token",
            "client_id"    : client_id,
            "client_secret": client_secret,
            "refresh_token": refresh_token,
        }, timeout=10)
        r.raise_for_status()
        data = r.json()
        if "access_token" not in data:
            raise Exception(f"Token refresh failed: {data}")
        return data["access_token"]

    def _upload_attachment(self, token: str, account_id: str, dc: str, file_path: str) -> str | None:
        """Upload a file to Zoho Mail and return the attachment storeName."""
        try:
            with open(file_path, "rb") as f:
                r = requests.post(
                    f"https://mail.zoho.{dc}/api/accounts/{account_id}/messages/attachments",
                    headers={"Authorization": f"Zoho-oauthtoken {token}"},
                    files={"attach": (os.path.basename(file_path), f, "application/pdf")},
                    timeout=30,
                )
            if r.status_code in (200, 201):
                store_name = r.json().get("data", {}).get("storeName")
                log.info("Attachment uploaded: %s → %s", os.path.basename(file_path), store_name)
                return store_name
            log.warning("Attachment upload failed [%s]: %s", r.status_code, r.text)
            return None
        except Exception as e:
            log.warning("Attachment upload error: %s", e)
            return None

    def send_email(self, to_address: str, subject: str, html_body: str,
                   attach_portfolio: bool = False, user_settings: dict = None) -> bool:
        # Use per-user credentials if provided, else fall back to .env globals
        s = user_settings or {}
        client_id     = s.get("zoho_client_id")     or os.getenv("ZOHO_CLIENT_ID")
        client_secret = s.get("zoho_client_secret") or os.getenv("ZOHO_CLIENT_SECRET")
        account_id    = s.get("zoho_account_id")    or os.getenv("ZOHO_MAIL_ACCOUNT_ID", "")
        dc            = s.get("zoho_dc")            or os.getenv("ZOHO_DC", "in")
        # Refresh token: user_settings → DB (app_config) → env var
        refresh_token = s.get("zoho_refresh_token") or None
        if not refresh_token:
            try:
                import database as _db
                refresh_token = _db.get_config("ZOHO_REFRESH_TOKEN") or None
            except Exception:
                pass
        if not refresh_token:
            refresh_token = os.getenv("ZOHO_REFRESH_TOKEN")
        sender_email  = s.get("sender_email")       or SENDER_EMAIL
        sender_name   = s.get("sender_name")        or SENDER_NAME

        if not account_id:
            log.error("ZOHO_MAIL_ACCOUNT_ID not set")
            return False
        if not client_id or not refresh_token:
            log.error("Zoho credentials missing")
            return False

        # Get token using the correct credentials for this user
        token = self._get_token_for(client_id, client_secret, refresh_token, dc)
        url   = f"https://mail.zoho.{dc}/api/accounts/{account_id}/messages"

        payload = {
            "fromAddress": sender_email,
            "toAddress"  : to_address,
            "subject"    : subject,
            "content"    : html_body,
            "mailFormat" : "html",
        }

        # Attach portfolio PDF if requested and file exists
        if attach_portfolio:
            pdf_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "BIMINFRA_PORTFOLIO.pdf")
            if os.path.exists(pdf_path):
                store_name = self._upload_attachment(token, account_id, dc, pdf_path)
                if store_name:
                    payload["attachments"] = [{"storeName": store_name, "fileName": "BIM_InfraSolutions_Portfolio_2025.pdf"}]
            else:
                log.warning("Portfolio PDF not found at %s", pdf_path)

        r = requests.post(
            url,
            headers={
                "Authorization": f"Zoho-oauthtoken {token}",
                "Content-Type" : "application/json",
            },
            data=json.dumps(payload),
            timeout=30,
        )

        if r.status_code in (200, 201):
            log.info("Email sent to %s: %s", to_address, subject)
            return True

        log.error("Email failed [%s]: %s", r.status_code, r.text)
        return False

    def send_sequence_email(self, lead: dict, step: int, user_settings: dict = None,
                            custom_template: dict = None) -> tuple:
        """
        Send the correct sequence email for a lead.
        Returns (success: bool, subject: str, html_body: str)
        """
        template   = lead.get("email_template", "A")
        first_name = lead.get("first_name", "there")
        company    = lead.get("company", "your company")

        # Build sender info from user_settings so the signature is personalised
        s = user_settings or {}
        sender_info = {
            "sender_name" : s.get("sender_name")  or SENDER_NAME,
            "sender_email": s.get("sender_email") or SENDER_EMAIL,
            "sender_title": s.get("sender_title") or TITLE,
            "sender_phone": s.get("sender_phone") or PHONE,
        }

        subject, html_body = get_email_content(template, step, first_name, company,
                                               custom_override=custom_template,
                                               sender_info=sender_info)

        if not subject:
            return False, "", ""

        success = self.send_email(
            to_address=lead["email"],
            subject=subject,
            html_body=html_body,
            attach_portfolio=False,
            user_settings=user_settings,
        )
        return success, subject, html_body

    def fetch_inbox_replies(self, lead_emails: list) -> list:
        """
        Fetch recent inbox emails from Zoho Mail.
        Returns list of reply dicts matching known lead emails.
        """
        acct_id = _zoho_acct_id()
        dc      = _zoho_dc()
        if not acct_id:
            return []
        try:
            token = self.get_access_token()
            r = requests.get(
                f"https://mail.zoho.{dc}/api/accounts/{acct_id}/messages/view",
                headers={"Authorization": f"Zoho-oauthtoken {token}"},
                params={"folderId": "inbox", "limit": 50, "sortorder": "false"},
                timeout=15,
            )
            if r.status_code != 200:
                return []

            messages = r.json().get("data", [])
            replies  = []

            for msg in messages:
                sender = msg.get("fromAddress", "").lower().strip()
                # Only include if sender is a known lead
                if sender in [e.lower() for e in lead_emails]:
                    replies.append({
                        "from_email": sender,
                        "subject"   : msg.get("subject", "(no subject)"),
                        "body"      : msg.get("summary", ""),
                        "received_at": msg.get("receivedTime", ""),
                    })
            return replies
        except Exception as e:
            log.warning("Inbox fetch failed: %s", e)
            return []

    def _get_full_message_body(self, token: str, folder_id: str, message_id: str) -> str:
        """Fetch the full HTML body of a message."""
        try:
            r = requests.get(
                f"https://mail.zoho.{_zoho_dc()}/api/accounts/{_zoho_acct_id()}"
                f"/folders/{folder_id}/messages/{message_id}/content",
                headers={"Authorization": f"Zoho-oauthtoken {token}"},
                timeout=10,
            )
            if r.status_code == 200:
                return r.json().get("data", {}).get("content", "")
            return ""
        except Exception:
            return ""

    def _extract_emails_from_html(self, html: str) -> list:
        """Extract all email addresses from href=mailto: links and plain text in HTML."""
        import re
        emails = []
        # mailto: href links  (most reliable in Zoho DSN HTML)
        for m in re.finditer(r'href=["\']mailto:([^"\'>\s]+)["\']', html, re.IGNORECASE):
            e = m.group(1).strip().lower()
            if "@" in e and e not in emails:
                emails.append(e)
        # Plain email pattern (fallback)
        for m in re.finditer(r'[\w.+%-]+@[\w.-]+\.\w{2,}', html):
            e = m.group(0).strip().lower()
            if e not in emails:
                emails.append(e)
        return emails

    def _parse_dsn_reason(self, html_body: str, summary: str, subject: str) -> tuple:
        """
        Parse a Delivery Status Notification (HTML body from Zoho).
        Returns (reason_str, is_permanent, is_delayed)
        """
        import re

        # Combine all text for searching
        combined = (html_body + " " + summary + " " + subject).lower()

        # ── SMTP status code ──────────────────────────────────────────
        # Zoho DSN format: "Status: 550" (plain text in HTML body)
        smtp_code = None
        m = re.search(r'\bstatus\s*[:\s]+(\d{3})\b', combined)
        if m:
            smtp_code = int(m.group(1))
        else:
            for code in [550, 551, 552, 553, 554, 421, 450, 452, 530]:
                if str(code) in combined:
                    smtp_code = code
                    break

        # ── Action: delayed vs failed ─────────────────────────────────
        # Zoho DSN format: "Action: failed" or "Action: delayed"
        action = ""
        m = re.search(r'\baction\s*[:\s]+(\w+)\b', combined)
        if m:
            action = m.group(1).lower()

        # Also check subject/summary keywords
        is_permanent = (
            action == "failed"
            or (smtp_code is not None and smtp_code >= 500)
            or any(k in combined for k in ["permanent error", "fatal error", "undelivered mail returned"])
        )
        is_delayed = (
            action == "delayed"
            or smtp_code == 421
            or (450 <= (smtp_code or 0) <= 452)
            or any(k in combined for k in ["warning message only", "will be retried", "retried for"])
        )

        # ── Reason string ─────────────────────────────────────────────
        reason_map = {
            550: "550 — Mailbox not found / address rejected",
            551: "551 — User not local",
            552: "552 — Mailbox storage full",
            553: "553 — Invalid email address format",
            554: "554 — Transaction failed (spam rejected / policy)",
            421: "421 — Host temporarily unavailable (retrying)",
            450: "450 — Mailbox temporarily unavailable",
            452: "452 — Insufficient storage on server",
            530: "530 — Authentication required / blocked",
        }
        reason = reason_map.get(smtp_code, "Delivery failure")

        # Append Diagnostic-Code text if present
        m = re.search(r'diagnostic-code[^:]*:\s*[^\n<]{0,20}([^\n<]{5,120})', combined)
        if m:
            diag = m.group(1).strip()
            if len(diag) > 5:
                reason += f" | {diag[:100]}"

        if is_delayed and not is_permanent:
            reason = f"[DELAYED — retrying] {reason}"

        return reason, is_permanent, is_delayed

    def fetch_delivery_status(self, lead_emails: list, already_synced: set = None) -> dict:
        """
        Scan Zoho Mail for delivery notifications, OOO, and real replies.
        Skips any message whose ID is in already_synced.

        Returns:
        {
          "bounced":    [{"email", "subject", "reason", "is_permanent", "is_delayed", "received_at", "raw_sender"}],
          "replied":    [{"email", "subject", "body", "received_at"}],
          "ooo":        [{"email", "subject", "received_at"}],
          "new_msg_ids": {type: [ids]}   — for saving to DB after processing
        }
        """
        if not _zoho_acct_id():
            return {"bounced": [], "replied": [], "ooo": [], "new_msg_ids": {}}

        if already_synced is None:
            already_synced = set()

        BOUNCE_SENDERS = [
            "mailer-daemon", "postmaster", "mail-delivery", "delivery-status",
            "mailerdaemon", "bounce",
        ]
        BOUNCE_SUBJECTS = [
            "undelivered", "undeliverable", "delivery failure", "delivery status",
            "mail delivery", "returned to sender", "could not deliver",
            "non-delivery", "ndr", "fatal error", "delayed",
        ]
        OOO_SUBJECTS = [
            "out of office", "auto-reply", "automatic reply", "away from",
            "on leave", "vacation", "holiday", "i am out", "i'm out",
            "absent", "auto response", "autoreply",
        ]

        try:
            token = self.get_access_token()
            lead_email_set = {e.lower() for e in lead_emails}
            result = {"bounced": [], "replied": [], "ooo": [], "new_msg_ids": {"bounce": [], "reply": [], "ooo": []}}

            # ── Auto-detect correct account ID ────────────────────────
            account_id = _zoho_acct_id()
            dc = _zoho_dc()
            acct_r = requests.get(
                f"https://mail.zoho.{dc}/api/accounts",
                headers={"Authorization": f"Zoho-oauthtoken {token}"},
                timeout=10,
            )
            if acct_r.status_code == 200:
                accounts = acct_r.json().get("data", [])
                if accounts:
                    account_id = str(accounts[0].get("accountId") or account_id)
                    log.info("Using Zoho account ID: %s", account_id)

            # ── Fetch last 200 messages ────────────────────────────────
            r = requests.get(
                f"https://mail.zoho.{dc}/api/accounts/{account_id}/messages/view",
                headers={"Authorization": f"Zoho-oauthtoken {token}"},
                params={"limit": 100, "sortorder": "false"},
                timeout=20,
            )
            if r.status_code != 200:
                log.warning("Message fetch failed: %s %s", r.status_code, r.text[:200])
                result["_error"] = f"Zoho API {r.status_code}: {r.text[:100]}"
                return result

            messages = r.json().get("data", [])
            log.info("Delivery scan: %d total messages, %d already synced", len(messages), len(already_synced))

            # Track seen message IDs within this batch (in-memory dedup)
            seen_msg_ids = set()

            for msg in messages:
                msg_id    = str(msg.get("messageId") or "")
                # Skip if already processed in a previous sync
                if msg_id in already_synced:
                    continue
                if msg_id in seen_msg_ids:
                    continue
                seen_msg_ids.add(msg_id)

                sender     = (msg.get("fromAddress") or "").lower().strip()
                subject    = (msg.get("subject") or "")
                subj_lower = subject.lower()
                summary    = (msg.get("summary") or "")
                recv       = msg.get("receivedTime", "")
                folder_id  = str(msg.get("folderId") or "")

                # ── 1. Delivery Status Notification ───────────────────
                is_bounce_sender  = any(b in sender for b in BOUNCE_SENDERS)
                is_bounce_subject = any(b in subj_lower for b in BOUNCE_SUBJECTS)

                if is_bounce_sender or is_bounce_subject:
                    # Use summary + toAddress first (no extra HTTP call needed)
                    reason, is_permanent, is_delayed = self._parse_dsn_reason(
                        "", summary, subject
                    )

                    # Find affected lead email from summary or toAddress (fast, no API call)
                    to_addr = (msg.get("toAddress") or "").lower().strip()
                    affected_email = None

                    # Check toAddress first
                    if to_addr in lead_email_set:
                        affected_email = to_addr

                    # Scan summary text
                    if not affected_email:
                        for le in lead_email_set:
                            if le in summary.lower():
                                affected_email = le
                                break

                    # Last resort: extract emails from summary using regex
                    if not affected_email:
                        import re as _re
                        for m in _re.finditer(r'[\w.+-]+@[\w.-]+\.\w+', summary):
                            e = m.group(0).lower()
                            if e in lead_email_set:
                                affected_email = e
                                break

                    log.info(
                        "DSN: %r | sender=%s | affected=%s | smtp=%s | permanent=%s | delayed=%s",
                        subject[:50], sender, affected_email, reason[:40], is_permanent, is_delayed,
                    )

                    result["bounced"].append({
                        "email"       : affected_email,
                        "subject"     : subject,
                        "reason"      : reason,
                        "is_permanent": is_permanent,
                        "is_delayed"  : is_delayed,
                        "received_at" : recv,
                        "raw_sender"  : sender,
                    })
                    if msg_id:
                        result["new_msg_ids"]["bounce"].append(msg_id)
                    continue

                # ── 2. Out-of-office detection ─────────────────────────
                is_ooo = any(o in subj_lower for o in OOO_SUBJECTS)
                if is_ooo and sender in lead_email_set:
                    result["ooo"].append({
                        "email"      : sender,
                        "subject"    : subject,
                        "received_at": recv,
                    })
                    if msg_id:
                        result["new_msg_ids"]["ooo"].append(msg_id)
                    log.info("OOO from %s", sender)
                    continue

                # ── 3. Real reply from a known lead ───────────────────
                if sender in lead_email_set:
                    result["replied"].append({
                        "email"      : sender,
                        "subject"    : subject,
                        "body"       : summary,
                        "received_at": recv,
                    })
                    if msg_id:
                        result["new_msg_ids"]["reply"].append(msg_id)
                    log.info("Reply from %s", sender)

            log.info(
                "Scan done: %d DSNs, %d replies, %d OOO (skipped %d already-synced)",
                len(result["bounced"]), len(result["replied"]), len(result["ooo"]),
                len(already_synced),
            )
            return result

        except Exception as e:
            log.warning("Delivery status fetch failed: %s", e)
            return {"bounced": [], "replied": [], "ooo": []}

    def test_connection(self) -> dict:
        """Test Zoho Mail connection. Returns status dict."""
        try:
            token = self.get_access_token()
            r = requests.get(
                f"https://mail.zoho.{_zoho_dc()}/api/accounts",
                headers={"Authorization": f"Zoho-oauthtoken {token}"},
                timeout=10,
            )
            if r.status_code == 200:
                accounts = r.json().get("data", [])
                return {"ok": True, "accounts": len(accounts), "token": token[:15] + "..."}
            return {"ok": False, "error": f"HTTP {r.status_code}: {r.text[:100]}"}
        except Exception as e:
            return {"ok": False, "error": str(e)}


mail_client = ZohoMailClient()


# ── EMAIL TEMPLATES ────────────────────────────────────────────────────────────

SEQUENCE_SCHEDULE = {
    "A": [0, 4, 9],
    "B": [0, 5, 12],
    "C": [0, 4, 10],
    "D": [0, 4, 9],
}


def get_portfolio_link(template: str) -> str:
    """Return correct portfolio link based on template."""
    if template in ("C", "D"):
        return DRONE_PORTFOLIO_LINK
    return PORTFOLIO_LINK


def get_email_content(template: str, step: int, first_name: str, company: str,
                      custom_override: dict = None, sender_info: dict = None) -> tuple:
    """Returns (subject, html_body) for the given template + step.
    Pass custom_override dict {subject, body} to use a DB-stored custom template.
    Pass sender_info dict {sender_name, sender_email, sender_title, sender_phone} to personalise signature."""
    if custom_override:
        raw_body = custom_override["body"].replace("{first_name}", first_name).replace("{company}", company)
        raw_subj = custom_override["subject"].replace("{first_name}", first_name).replace("{company}", company)
        port_link = get_portfolio_link(template)
        return raw_subj, _wrap_email(raw_body, port_link, sender_info=sender_info)

    # ── TEMPLATE A — Enterprise GC / Engineering Firms (USA/Canada/Australia) ──
    A = [
        {
            "subject": f"{first_name}, is {company}'s BIM team at full capacity right now?",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>I came across {company} and noticed you're handling significant construction/engineering projects — I wanted to reach out directly.</p>
                <p>I'm Kishan, Co-founder &amp; CFO at <strong>BIM INFRASOLUTIONS LLP</strong>. We're a technology-driven BIM consultancy with <strong>100+ projects delivered across USA, Canada, Germany, Australia and India</strong> — including a 110 km floating tunnel project for Parsons in the USA and multiple high-rise coordination projects.</p>
                <p>A quick question: when your BIM team hits capacity during peak project phases, what does {company} typically do — hire temps, delay, or push the in-house team harder?</p>
                <p>We step in as a seamless offshore extension. Specifically:</p>
                <ul style="line-height:2;">
                    <li>Revit modelling — Architectural, Structural, MEP (LOD 200–500)</li>
                    <li>Clash detection &amp; BIM coordination (BIM 360 / ACC / Procore)</li>
                    <li>4D scheduling &amp; 5D quantity takeoffs</li>
                    <li>Scan-to-BIM from LiDAR / point clouds</li>
                    <li>Dynamo / automation scripts to cut repetitive work</li>
                </ul>
                <p>We're offering <strong>3 paid pilot slots this quarter</strong> at a fixed project rate — results in 48 hours, no long-term commitment required.</p>
                <p>Worth a 30-minute call to see if there's a fit?</p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Book a 30-Min Call</a></p>
                <p style="margin-top:12px;">
                  <a href="{PORTFOLIO_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;font-size:13px;">&#128196; View Our Full Portfolio</a>
                </p>
            """,
        },
        {
            "subject": f"Re: BIM capacity at {company} — one example that might be relevant",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>Following up on my last note — wanted to share one project that might be directly relevant to {company}.</p>
                <p>We recently completed BIM coordination for a <strong>high-rise building in Canada</strong> (6,000 sqmt, LOD 300, full ARCHI + MEPF + Structure) — the client's internal team was overwhelmed with site activities. We delivered the coordination model in under 2 weeks, clash-free.</p>
                <p>We've done similar work for GCs and engineering firms in the USA — including a <strong>110 km floating tunnel for Parsons</strong> where we built a Dynamo-based automated alignment change system that saved weeks of manual rework.</p>
                <p>If {company} has any upcoming project where BIM capacity is a concern, I'd love to understand the scope and see if we can help.</p>
                <p><strong>3 things that set us apart:</strong></p>
                <ul style="line-height:2;">
                    <li>360,000+ hours of BIM work delivered</li>
                    <li>54 million sqmt of assets digitalized</li>
                    <li>Onboarding in 48 hours — we match your Revit templates &amp; standards</li>
                </ul>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Schedule 30 Minutes</a></p>
            """,
        },
        {
            "subject": f"Last note — pilot slots for {company}",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>I'll keep this short — this is my last note and I don't want to crowd your inbox.</p>
                <p>If BIM outsourcing ever comes up at {company} — whether for a single project, a capacity crunch, or an ongoing engagement — I'd genuinely love to be your first call.</p>
                <p>We have <strong>2 pilot slots remaining for Q2 2025</strong>. The pilot is a real project deliverable — Revit model, coordination or scan-to-BIM — at a fixed rate with zero risk.</p>
                <p>
                  <a href="{PORTFOLIO_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;font-size:13px;">&#128196; View Our Full Portfolio</a>
                </p>
                <p><a href="{CALENDLY_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Book a Final 30-Min Call</a></p>
                <p>Wishing {company} a great quarter ahead.</p>
            """,
        },
    ]

    # ── TEMPLATE B — Architecture Firms (USA/Germany/Europe) ──────────────────
    B = [
        {
            "subject": f"{first_name} — is your Revit team keeping up with project deadlines at {company}?",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>I'm reaching out because architecture firms like {company} are often dealing with the same challenge — Revit workload peaks during design development and CD phases, and the in-house team is stretched thin.</p>
                <p>I'm Kishan, Co-founder &amp; CFO at <strong>BIM INFRASOLUTIONS LLP</strong>. We work as a dedicated BIM extension team for architecture firms across the USA, Germany, and Europe.</p>
                <p>Some of our recent architecture work:</p>
                <ul style="line-height:2;">
                    <li><strong>Reutlingen City Hall, Germany</strong> — 70,000 sqmt full BIM model (ARCHI + MEPF + Structure)</li>
                    <li><strong>Nikko Hotel Düsseldorf</strong> — 40,000 sqmt full renovation Scan-to-BIM + coordination</li>
                    <li><strong>Bonn University</strong> — 30,000 sqmt LOD 400 Scan-to-BIM for historic renovation</li>
                    <li><strong>60+ parametric Revit families</strong> for European architecture firms (LOD 500)</li>
                </ul>
                <p>We work inside your standards — your Revit templates, your sheet naming, your family library. No ramp-up friction.</p>
                <p><strong>Deliverable in 48 hours. Pilot project available this quarter.</strong></p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Book a 30-Min Call</a></p>
            """,
        },
        {
            "subject": f"{first_name} — our work for firms like {company} (quick examples)",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>Following up — wanted to share a few quick specifics on how we've helped architecture firms similar to {company}.</p>
                <p><strong>What we typically handle:</strong></p>
                <ul style="line-height:2;">
                    <li>Revit CD documentation — floor plans, sections, elevations, details</li>
                    <li>Revit family creation (parametric, LOD 300–500)</li>
                    <li>BIM coordination — clash detection across ARCHI, MEPF, Structure</li>
                    <li>As-built to BIM (Scan-to-BIM, LOD 300–400)</li>
                    <li>BIM 360 / ACC project setup and management</li>
                </ul>
                <p><strong>Our numbers:</strong> 100+ international projects | 54M sqmt digitalized | 360K hours delivered | Teams in USA, Germany, India</p>
                <p>You can view our full portfolio here:
                  <a href="{PORTFOLIO_LINK}" style="color:#1B3A6B;font-weight:600;">BIM InfraSolutions Portfolio 2025 ↗</a>
                </p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Let's Talk — 30 Min Call</a></p>
            """,
        },
        {
            "subject": f"Final note from me — {company}",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>Last note — I know how busy architecture project schedules get and I won't keep following up after this.</p>
                <p>If {company} ever needs BIM/Revit support — whether it's a CD crunch, a Scan-to-BIM project, or extra coordination capacity — we're ready to step in within 48 hours.</p>
                <p>Our pilot offer remains open: <strong>one real project deliverable at a fixed rate</strong>, and you only continue if you're satisfied.</p>
                <p>
                  <a href="{PORTFOLIO_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;font-size:13px;">&#128196; View Our Full Portfolio</a>
                </p>
                <p><a href="{CALENDLY_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Book a Call Anytime</a></p>
                <p>Best of luck with your current projects, {first_name}.</p>
            """,
        },
    ]

    # ── TEMPLATE C — Drone / Scan-to-BIM / Survey Firms ──────────────────────
    C = [
        {
            "subject": f"{first_name} — turning {company}'s scan data into BIM models (we've done 15+ projects)",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>If {company} works with drone surveys, LiDAR scans, or point cloud data — I think what we do at <strong>BIM INFRASOLUTIONS LLP</strong> will be directly useful to you.</p>
                <p>We convert raw scan data into accurate, fully coordinated BIM models — architecture, structure, and MEP. Recent Scan-to-BIM projects we've delivered:</p>
                <ul style="line-height:2;">
                    <li><strong>Rochsburg Castle, Germany</strong> — 40,000 sqmt heritage structure, LOD 300 Scan-to-BIM</li>
                    <li><strong>Kampnagel Hamburg</strong> — 100,000 sqmt industrial renovation, Scan-to-BIM for Lacaton &amp; Vassal architects</li>
                    <li><strong>Bonn University</strong> — 30,000 sqmt historic building, LOD 400</li>
                    <li><strong>Autobahn Viaduct, Germany</strong> — 500m historic brick bridge</li>
                    <li><strong>Nikko Hotel Düsseldorf</strong> — 40,000 sqmt full hotel renovation</li>
                </ul>
                <p>We deliver from raw point cloud to finished Revit model in <strong>3–5 business days</strong> depending on complexity.</p>
                <p><strong>Paid pilot available:</strong> send us a sample scan file, we deliver a BIM model. You pay only if satisfied.</p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Book a 30-Min Demo Call</a></p>
            """,
        },
        {
            "subject": f"{first_name} — our Scan-to-BIM workflow for {company} (short overview)",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>Quick follow-up on my last note.</p>
                <p>Here's our standard Scan-to-BIM workflow that might be relevant for {company}'s projects:</p>
                <ol style="line-height:2;">
                    <li><strong>Receive</strong> — point cloud / E57 / RCP / drone data from you</li>
                    <li><strong>Register &amp; clean</strong> — align scans, remove noise</li>
                    <li><strong>Model</strong> — Revit model built to your LOD requirement (200–500)</li>
                    <li><strong>QC &amp; deliver</strong> — clash-free, your naming convention, your sheet setup</li>
                </ol>
                <p>Turnaround: <strong>3–5 business days</strong> for standard projects. Rush delivery available.</p>
                <p>We've digitalized <strong>54 million sqmt of assets</strong> globally — from heritage castles in Germany to industrial factories and university campuses.</p>
                <p>Happy to do a <strong>free test conversion</strong> on a small sample scan to show you the quality before any commitment.</p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">Schedule 30 Minutes</a></p>
            """,
        },
        {
            "subject": f"Last note — Scan-to-BIM pilot for {company}",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>This is my last follow-up — I don't want to be a nuisance.</p>
                <p>If {company} has any scan data sitting around that needs BIM conversion, or any upcoming survey project where you'll need a BIM deliverable, we'd love to be your partner for it.</p>
                <p><strong>What we offer:</strong> paid pilot, fixed price, satisfaction guaranteed. No lock-in.</p>
                <p>Our global Scan-to-BIM portfolio: <a href="{WEBSITE}" style="color:#1B3A6B;">{WEBSITE}</a></p>
                <p><a href="{CALENDLY_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;margin-top:8px;">One Last Call — 30 Min</a></p>
                <p>Wishing {company} great projects ahead.</p>
            """,
        },
    ]

    # ── TEMPLATE D — INFRA X / Geospatial / Survey Firms (India) ────────────────
    D = [
        {
            "subject": f"{first_name}, one question about how {company} monitors site progress",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>I'm reaching out because {company}'s work in geospatial and survey services aligns closely with something we've built and deployed at <strong>BIM INFRASOLUTIONS LLP</strong>.</p>
                <p>We've developed <strong>INFRA X</strong> — a site intelligence platform for large infrastructure and industrial projects. It gives project owners, PMCs, and site teams a single place to monitor everything happening on ground, in real time.</p>
                <p><strong>Key capabilities of INFRA X:</strong></p>
                <ul style="line-height:2;">
                    <li><strong>Compare View</strong> — overlay planned design vs. actual site condition at any point in time, week by week</li>
                    <li><strong>One-desk monitoring</strong> — all sites, all zones, all teams visible from a single dashboard — no more chasing WhatsApp updates</li>
                    <li><strong>Resource allocation tracking</strong> — know exactly where manpower, machinery, and materials are deployed at any given time</li>
                    <li><strong>Automated progress reports</strong> — volume calculations, milestone completion %, deviation alerts — generated without manual input</li>
                    <li><strong>As-built documentation</strong> — orthomosaic maps, point clouds, and 3D models captured and stored automatically</li>
                </ul>
                <p>We are currently <strong>live at the Khavada site</strong> in Gujarat — deployed for <strong>Kalpataru Projects International</strong> and <strong>Hitachi Energy India</strong> for their large-scale renewable energy and transmission infrastructure works.</p>
                <p>Given {company}'s expertise in geospatial and survey services, I'd love to explore a collaboration — as a technology partner, data processing partner, or on a joint project basis.</p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;">Book a 30-Min Call</a></p>
                <p style="margin-top:10px;"><a href="{DRONE_PORTFOLIO_LINK}" style="color:#1B3A6B;">View our INFRA X portfolio →</a></p>
            """,
        },
        {
            "subject": f"INFRA X at Khavada — what {company} can do with this",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>Following up on my last note about <strong>INFRA X</strong>, our site intelligence platform.</p>
                <p>Here's what's running live at the <strong>Khavada renewable energy site</strong> for Kalpataru and Hitachi Energy right now:</p>
                <ul style="line-height:2;">
                    <li><strong>Compare View</strong> — project managers pull up any date and see exactly how much work was done that week versus what was planned</li>
                    <li><strong>One-desk monitoring</strong> — the entire 500+ acre site is visible from one screen, updated every week</li>
                    <li><strong>Resource allocation</strong> — equipment deployment, labour zones, and material movement tracked and logged automatically</li>
                    <li><strong>Deviation alerts</strong> — if any section falls behind schedule, the system flags it before the weekly review meeting</li>
                    <li><strong>Client-facing reports</strong> — auto-generated and ready to share with the project owner every Monday morning</li>
                </ul>
                <p>For firms like {company} that already have the geospatial and survey capability, INFRA X adds the intelligence layer on top — and can be offered as a premium service to your infrastructure clients.</p>
                <p>We have <strong>3 partnership slots open</strong> with Survey of India empanelled firms this quarter.</p>
                <p><a href="{CALENDLY_LINK}" style="background:#1B3A6B;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;">Schedule a 30-Min Demo</a></p>
            """,
        },
        {
            "subject": f"Last note — INFRA X and {company}",
            "body": f"""
                <p>Hi {first_name},</p>
                <p>Last note from me — I'll keep it brief.</p>
                <p>If {company} ever works on a large infrastructure, renewable energy, or industrial site where the client is struggling to get clear visibility on ground progress — <strong>INFRA X</strong> solves exactly that.</p>
                <p>One dashboard. Compare view. Resource allocation. Automated reports. No manual data collection.</p>
                <p>We're live at <strong>Khavada with Kalpataru and Hitachi Energy</strong> — this is a working, deployed solution, not a pitch deck.</p>
                <p>Open to a partnership, white-label arrangement, or a referral on a project basis — whatever works best for {company}.</p>
                <p><a href="{CALENDLY_LINK}" style="background:#D4A017;color:#fff;padding:10px 20px;border-radius:5px;text-decoration:none;display:inline-block;">One Final Call — 30 Min</a></p>
                <p style="margin-top:10px;"><a href="{DRONE_PORTFOLIO_LINK}" style="color:#1B3A6B;">View INFRA X portfolio →</a></p>
                <p>Wishing {company} great projects ahead.</p>
            """,
        },
    ]

    all_templates = {"A": A, "B": B, "C": C, "D": D}
    steps = all_templates.get(template, A)
    if step >= len(steps):
        return "", ""

    content  = steps[step]
    port_link = get_portfolio_link(template)
    return content["subject"], _wrap_email(content["body"], port_link, sender_info=sender_info)


def _wrap_email(body: str, portfolio_link: str = None, sender_info: dict = None) -> str:
    """Simple clean email layout. sender_info dict personalises the signature."""
    s     = sender_info or {}
    name  = s.get("sender_name")  or SENDER_NAME
    email = s.get("sender_email") or SENDER_EMAIL
    title = s.get("sender_title") or TITLE
    phone = s.get("sender_phone") or PHONE
    port  = portfolio_link or PORTFOLIO_LINK
    return f"""<!DOCTYPE html>
<html>
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0"></head>
<body style="margin:0;padding:0;background:#ffffff;font-family:'Segoe UI',Arial,sans-serif;color:#222;font-size:14px;line-height:1.8;">
  <div style="max-width:580px;margin:30px auto;padding:0 20px;">

    <!-- Body -->
    <div style="margin-bottom:32px;">
      {body}
    </div>

    <!-- Divider -->
    <div style="border-top:1px solid #e0e0e0;margin:24px 0;"></div>

    <!-- Signature -->
    <div style="font-size:13px;color:#444;line-height:2;">
      <div style="font-weight:700;font-size:14px;color:#1B3A6B;">{name}</div>
      <div style="color:#888;">{title} — {COMPANY_FULL}</div>
      <div style="margin-top:4px;">
        {phone} &nbsp;|&nbsp;
        <a href="mailto:{email}" style="color:#1B3A6B;text-decoration:none;">{email}</a> &nbsp;|&nbsp;
        <a href="{WEBSITE}" style="color:#1B3A6B;text-decoration:none;">{WEBSITE}</a>
      </div>
      <div style="margin-top:10px;">
        <a href="{CALENDLY_LINK}" style="color:#fff;background:#1B3A6B;padding:8px 16px;border-radius:4px;text-decoration:none;font-size:12px;margin-right:8px;">Book a 30-Min Call</a>
        <a href="{port}" style="color:#1B3A6B;font-size:12px;text-decoration:underline;">View Portfolio</a>
      </div>
    </div>

  </div>
</body>
</html>"""
