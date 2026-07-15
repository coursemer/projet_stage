"""
OutlookNotifier — envoi de notifications par e-mail via Outlook / Office 365 SMTP.

Lit automatiquement OUTLOOK_SMTP_USER / OUTLOOK_SMTP_PASSWORD / OUTLOOK_RECIPIENTS
depuis l'environnement. Peut aussi être instancié avec des valeurs explicites.

Usage :
    from spark.alerting.outlook_notifier import OutlookNotifier

    notifier = OutlookNotifier()   # lit les variables d'environnement
    notifier = OutlookNotifier(
        smtp_user="alerts@delomid-it.com",
        smtp_password="...",
        recipients=["ops@delomid-it.com"],
    )

    ok = notifier.send_alert(
        pipeline="clean_sales",
        metric="rejection_rate_pct",
        severity="CRITICAL",
        value=45.2,
        details="Taux de rejet 45.2% > seuil 20%",
    )

    ok = notifier.send_test()
"""
from __future__ import annotations

import os
import smtplib
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

_SEV_COLOR = {
    "CRITICAL": "#E81123",
    "HIGH":     "#E74856",
    "WARNING":  "#FFB900",
    "MEDIUM":   "#FFB900",
    "LOW":      "#00B7C3",
    "INFO":     "#0078D4",
}


class OutlookNotifier:
    """Envoi de notifications par e-mail via le SMTP Outlook / Office 365."""

    def __init__(
        self,
        smtp_user:     Optional[str]       = None,
        smtp_password: Optional[str]       = None,
        smtp_host:     str                 = "smtp.office365.com",
        smtp_port:     int                 = 587,
        from_addr:     Optional[str]       = None,
        recipients:    Optional[List[str]] = None,
        dry_run:       bool                = False,
    ) -> None:
        self.smtp_user     = smtp_user or os.getenv("OUTLOOK_SMTP_USER", "")
        self.smtp_password = smtp_password or os.getenv("OUTLOOK_SMTP_PASSWORD", "")
        self.smtp_host     = smtp_host or os.getenv("OUTLOOK_SMTP_HOST", "smtp.office365.com")
        self.smtp_port     = int(os.getenv("OUTLOOK_SMTP_PORT", smtp_port))
        self.from_addr     = from_addr or os.getenv("OUTLOOK_FROM_ADDR", self.smtp_user)
        self.recipients    = recipients or _split_env("OUTLOOK_RECIPIENTS")
        self.dry_run       = dry_run

    @property
    def configured(self) -> bool:
        return bool(self.smtp_user and self.smtp_password and self.recipients)

    # ── API publique ──────────────────────────────────────────────────────────

    def send_alert(
        self,
        pipeline: str,
        metric:   str,
        severity: str,
        value:    float,
        details:  str  = "",
        ts:       Optional[str] = None,
    ) -> Dict[str, Any]:
        """Envoie un e-mail d'alerte via Outlook. Retourne {"ok": bool, "detail": str}."""
        if not self.configured:
            return {"ok": False, "detail": "OUTLOOK_SMTP_USER / OUTLOOK_SMTP_PASSWORD / OUTLOOK_RECIPIENTS non configurés"}

        color = _SEV_COLOR.get(severity.upper(), "#E74856")
        ts    = ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        subject = f"[Data Trust] [{severity}] {pipeline} — {metric}"
        body_html = f"""
        <html><body style="font-family:Arial,sans-serif">
        <h2 style="color:{color}">🔔 Alerte {severity}</h2>
        <table border="1" cellpadding="6" cellspacing="0">
          <tr><td><b>Pipeline</b></td><td>{pipeline}</td></tr>
          <tr><td><b>Métrique</b></td><td>{metric}</td></tr>
          <tr><td><b>Valeur</b></td><td>{value}</td></tr>
          <tr><td><b>Sévérité</b></td><td>{severity}</td></tr>
          <tr><td><b>Détail</b></td><td>{details or '—'}</td></tr>
          <tr><td><b>Timestamp</b></td><td>{ts}</td></tr>
        </table>
        <p style="color:gray;font-size:11px">Data Trust Agent — alerting automatique</p>
        </body></html>
        """
        return self._send(subject, body_html)

    def send_many(self, alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Envoie un e-mail pour chaque alerte de la liste."""
        return [
            self.send_alert(
                pipeline=a.get("source", a.get("pipeline", "?")),
                metric=a.get("metric_name", a.get("metric", "?")),
                severity=str(a.get("severity", "INFO")).upper(),
                value=float(a.get("value", 0)),
                details=str(a.get("details", "")),
                ts=a.get("ts"),
            )
            for a in alerts
        ]

    def send_test(self) -> Dict[str, Any]:
        """Envoie un e-mail de test pour vérifier la connexion Outlook."""
        if not self.configured:
            return {"ok": False, "detail": "OUTLOOK_SMTP_USER / OUTLOOK_SMTP_PASSWORD / OUTLOOK_RECIPIENTS non configurés"}

        subject = "Test Data Trust Agent — Outlook"
        body_html = f"""
        <html><body style="font-family:Arial,sans-serif">
        <h2 style="color:#0078D4">✅ Connexion Outlook opérationnelle</h2>
        <p>Data Trust Agent — test de notification</p>
        <p style="color:gray;font-size:11px">
          {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}
        </p>
        </body></html>
        """
        return self._send(subject, body_html)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _send(self, subject: str, body_html: str) -> Dict[str, Any]:
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = self.from_addr
        msg["To"]      = ", ".join(self.recipients)
        msg.attach(MIMEText(body_html, "html"))

        if self.dry_run:
            return {"ok": True, "detail": f"[dry-run] SMTP {self.smtp_host}:{self.smtp_port} → {self.recipients}"}

        try:
            with smtplib.SMTP(self.smtp_host, self.smtp_port, timeout=15) as server:
                server.starttls()
                server.login(self.smtp_user, self.smtp_password)
                server.sendmail(self.from_addr, self.recipients, msg.as_string())
            return {"ok": True, "detail": f"envoyé à {self.recipients}"}
        except Exception as exc:
            return {"ok": False, "detail": str(exc)}


def _split_env(name: str) -> List[str]:
    raw = os.getenv(name, "")
    return [addr.strip() for addr in raw.split(",") if addr.strip()]
