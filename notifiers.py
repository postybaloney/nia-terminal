"""
Notification dispatchers — Slack webhook and email.

Configure via .env / Railway Variables:

  # Recommended for Railway (HTTP API, no port restrictions):
  RESEND_API_KEY=re_xxxxxxxxxx
  RESEND_FROM=Patent Intel <noreply@yourdomain.com>
  DIGEST_EMAIL_TO=team@example.com,cto@example.com

  # SMTP fallback (works locally; Railway blocks outbound SMTP):
  SMTP_HOST=smtp.gmail.com
  SMTP_PORT=587
  SMTP_USER=you@example.com
  SMTP_PASSWORD=app_password
  DIGEST_EMAIL_TO=team@example.com,cto@example.com

  # Slack:
  SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
"""
from __future__ import annotations

import logging
import smtplib
from datetime import date
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

import httpx

log = logging.getLogger(__name__)


# ── Slack ─────────────────────────────────────────────────────────────────────

async def send_slack_digest(
    digest_text: str,
    webhook_url: str,
    new_count: int,
    run_id: int,
) -> bool:
    """
    Post weekly digest to a Slack channel via Incoming Webhook.

    Returns True on success. Digest is split into sections to respect
    Slack's 3000-char block limit.
    """
    paragraphs = [p.strip() for p in digest_text.split("\n\n") if p.strip()]

    blocks = [
        {
            "type": "header",
            "text": {
                "type": "plain_text",
                "text": "Patent Intelligence Weekly Digest",
            },
        },
        {
            "type": "context",
            "elements": [
                {
                    "type": "mrkdwn",
                    "text": f"Run #{run_id} · {new_count} new patents this cycle · medtech & neurotech",
                }
            ],
        },
        {"type": "divider"},
    ]

    for para in paragraphs:
        blocks.append({
            "type": "section",
            "text": {"type": "mrkdwn", "text": para[:3000]},
        })

    blocks.append({"type": "divider"})
    blocks.append({
        "type": "context",
        "elements": [
            {
                "type": "mrkdwn",
                "text": "Sources: PatentsView · EPO OPS · Lens.org · Google BigQuery",
            }
        ],
    })

    payload = {"blocks": blocks}

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.post(webhook_url, json=payload)
            resp.raise_for_status()
            log.info("slack: digest posted successfully")
            return True
        except Exception as exc:
            log.error("slack: failed to post digest: %s", exc)
            return False


async def send_slack_alert(
    message: str,
    webhook_url: str,
    level: str = "info",
) -> None:
    """
    Send a short operational alert (pipeline errors, unusual volumes, etc.).
    level: "info" | "warning" | "error"
    """
    emoji = {"info": "ℹ️", "warning": "⚠️", "error": "🔴"}.get(level, "ℹ️")
    payload = {"text": f"{emoji} *patent_intel*: {message}"}

    async with httpx.AsyncClient(timeout=10) as client:
        try:
            resp = await client.post(webhook_url, json=payload)
            resp.raise_for_status()
        except Exception as exc:
            log.error("slack alert failed: %s", exc)


# ── Email helpers ─────────────────────────────────────────────────────────────

def _build_email_content(digest_text: str, new_count: int) -> tuple[str, str, str]:
    """Return (subject, plain_text, html_body)."""
    subject = f"Patent Intelligence Digest — {date.today().isoformat()} ({new_count} new patents)"
    plain = f"Patent Intelligence Weekly Digest\n{'='*40}\n\n{digest_text}"
    html_paras = "".join(
        f"<p style='margin:0 0 1em;line-height:1.6'>{p.strip()}</p>"
        for p in digest_text.split("\n\n")
        if p.strip()
    )
    html = f"""<!DOCTYPE html>
<html>
<head><meta charset="utf-8"></head>
<body style="font-family:Georgia,serif;max-width:640px;margin:40px auto;color:#1a1a1a;padding:0 20px">
  <h2 style="font-size:20px;font-weight:normal;border-bottom:1px solid #ddd;padding-bottom:12px;margin-bottom:24px">
    Patent Intelligence Weekly Digest
  </h2>
  {html_paras}
  <hr style="border:none;border-top:1px solid #eee;margin:32px 0">
  <p style="font-size:12px;color:#888">
    Sources: PatentsView · EPO OPS · Lens.org · Google BigQuery<br>
    {new_count} new patents this cycle · medtech &amp; neurotech
  </p>
</body>
</html>"""
    return subject, plain, html


# ── Resend (HTTP API — works on Railway) ──────────────────────────────────────

async def send_resend_digest(
    digest_text: str,
    api_key: str,
    from_address: str,
    to_addresses: list[str],
    new_count: int,
) -> bool:
    """
    Send digest via Resend HTTP API (https://resend.com).
    No outbound SMTP ports required — works on Railway.
    """
    if not to_addresses:
        log.warning("resend: no recipients configured")
        return False

    subject, plain, html = _build_email_content(digest_text, new_count)

    payload = {
        "from": from_address,
        "to": to_addresses,
        "subject": subject,
        "text": plain,
        "html": html,
    }

    async with httpx.AsyncClient(timeout=30) as client:
        try:
            resp = await client.post(
                "https://api.resend.com/emails",
                headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
                json=payload,
            )
            resp.raise_for_status()
            log.info("resend: digest sent to %s (id=%s)", to_addresses, resp.json().get("id"))
            return True
        except Exception as exc:
            log.error("resend: failed to send digest: %s", exc)
            return False


# ── SMTP (fallback — works locally, blocked on Railway) ───────────────────────

def send_email_digest(
    digest_text: str,
    smtp_host: str,
    smtp_port: int,
    smtp_user: str,
    smtp_password: str,
    to_addresses: list[str],
    new_count: int,
) -> bool:
    """
    Send weekly digest as a plain-text + HTML email via SMTP (STARTTLS).
    For Gmail, use an App Password (not account password).
    NOTE: Railway blocks outbound SMTP — use Resend instead on Railway.
    """
    if not to_addresses:
        log.warning("email: no recipients configured")
        return False

    subject, plain, html = _build_email_content(digest_text, new_count)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = smtp_user
    msg["To"] = ", ".join(to_addresses)
    msg.attach(MIMEText(plain, "plain"))
    msg.attach(MIMEText(html, "html"))

    try:
        with smtplib.SMTP(smtp_host, smtp_port) as server:
            server.ehlo()
            server.starttls()
            server.login(smtp_user, smtp_password)
            server.sendmail(smtp_user, to_addresses, msg.as_string())
        log.info("email: digest sent to %s", to_addresses)
        return True
    except Exception as exc:
        log.error("email: failed to send digest: %s", exc)
        return False


# ── Dispatcher ────────────────────────────────────────────────────────────────

async def dispatch_digest(
    digest_text: str,
    new_count: int,
    run_id: int,
) -> None:
    """
    Send digest to all configured channels.
    Called from scheduler.py after a successful run.

    Email priority: Resend (HTTP, Railway-compatible) → SMTP (local fallback).
    """
    from config import settings

    if settings.slack_webhook_url:
        await send_slack_digest(digest_text, settings.slack_webhook_url, new_count, run_id)

    to_addrs = [a.strip() for a in settings.digest_email_to.split(",") if a.strip()]

    if settings.resend_api_key and settings.resend_from and to_addrs:
        await send_resend_digest(
            digest_text=digest_text,
            api_key=settings.resend_api_key,
            from_address=settings.resend_from,
            to_addresses=to_addrs,
            new_count=new_count,
        )
    elif settings.smtp_host and settings.smtp_user and to_addrs:
        send_email_digest(
            digest_text=digest_text,
            smtp_host=settings.smtp_host,
            smtp_port=settings.smtp_port,
            smtp_user=settings.smtp_user,
            smtp_password=settings.smtp_password,
            to_addresses=to_addrs,
            new_count=new_count,
        )
    else:
        log.warning("dispatch_digest: no email channel configured (set RESEND_API_KEY or SMTP_USER)")
