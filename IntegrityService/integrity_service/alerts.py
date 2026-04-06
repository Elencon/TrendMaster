from typing import Optional
from email.mime.text import MIMEText
import smtplib
from datetime import datetime

from .config import load_config


def send_alert(title: str, message: str) -> None:
    """
    Send an email alert using settings from config.json.

    Parameters
    ----------
    title : str
        Subject line of the alert email.
    message : str
        Body text of the alert email.
    """

    cfg = load_config()
    alert_cfg: Optional[dict] = cfg.get("alert_email")

    # If alerting is not configured, silently skip
    if not alert_cfg:
        return

    try:
        msg = MIMEText(message)
        msg["Subject"] = title
        msg["From"] = alert_cfg["from"]
        msg["To"] = alert_cfg["to"]

        smtp_server: str = alert_cfg["smtp_server"]

        with smtplib.SMTP(smtp_server) as server:
            server.send_message(msg)

    except Exception as e:
        # You can replace this with logging if needed
        print(f"[{datetime.now().isoformat()}] Failed to send alert: {e}")