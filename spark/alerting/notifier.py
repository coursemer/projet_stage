"""
NotificationService — Envoi de notifications multi-canaux (Semaine 14).

Canaux supportés :
  console — affichage terminal (toujours disponible, fallback garanti)
  teams   — webhook Microsoft Teams (HTTP POST JSON)
  email   — SMTP (smtplib standard, TLS optionnel)

Cooldown :
  Un log SQLite (notification_log) évite de renvoyer la même alerte
  plusieurs fois pendant la fenêtre de silence configurée dans AlertRule.

Usage :
    from spark.alerting import NotificationService, NotificationChannel, AlertRule

    svc = NotificationService(channels=[
        NotificationChannel("console"),
        NotificationChannel("teams", {"webhook_url": "https://..."}),
        NotificationChannel("email", {
            "smtp_host": "smtp.gmail.com", "smtp_port": 587,
            "smtp_user": "alert@co.fr", "smtp_password": "...",
            "from_addr": "alert@co.fr", "use_tls": True,
        }),
    ])
    rule = AlertRule(channels=["teams", "email"], recipients=["ops@co.fr"])
    results = svc.notify(alert_dict, rule)
"""
from __future__ import annotations

import json
import os
import smtplib
import sqlite3
import sys
import urllib.request
from datetime import datetime, timezone
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

BASE_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from spark.alerting.models import AlertRule, NotificationChannel

_DDL_LOG = """
CREATE TABLE IF NOT EXISTS notification_log (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id     TEXT NOT NULL,
    channel     TEXT NOT NULL,
    metric_name TEXT NOT NULL,
    pipeline    TEXT NOT NULL,
    severity    TEXT NOT NULL,
    sent_at     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_rule ON notification_log(rule_id, metric_name, pipeline, sent_at);
"""


class NotificationService:
    """
    Orchestre l'envoi de notifications vers les canaux configurés.

    Paramètres
    ----------
    channels    : liste de NotificationChannel à utiliser
    log_db_path : chemin SQLite pour le cooldown log
    dry_run     : si True, simule l'envoi sans appel réseau/SMTP réel
    """

    def __init__(
        self,
        channels:    Optional[List[NotificationChannel]] = None,
        log_db_path: str = "spark/data/notification_log.db",
        dry_run:     bool = False,
    ) -> None:
        self.channels    = channels or [NotificationChannel("console")]
        self.dry_run     = dry_run
        self.log_db_path = log_db_path
        os.makedirs(os.path.dirname(log_db_path) or ".", exist_ok=True)
        self._init_log_db()

    # ── API publique ──────────────────────────────────────────────────────────

    def notify(self, alert: Dict[str, Any], rule: AlertRule) -> List[Dict[str, Any]]:
        """
        Envoie une notification pour `alert` selon `rule`.

        Retourne la liste des résultats par canal :
          [{"channel": ..., "status": "sent"|"cooldown"|"error", "detail": ...}]
        """
        pipeline = _extract(alert, "source", "pipeline", default="unknown")
        metric   = _extract(alert, "metric_name", "metric", default="unknown")
        severity = _extract(alert, "severity", default="HIGH").upper()

        results = []
        for ch_name in rule.channels:
            channel = self._find_channel(ch_name)

            # Cooldown check
            if self._in_cooldown(rule.rule_id, ch_name, metric, pipeline, rule.cooldown_min):
                results.append({"channel": ch_name, "status": "cooldown", "detail": "within cooldown window"})
                continue

            try:
                detail = self._dispatch(channel, alert, pipeline, metric, severity, rule)
                self._log_sent(rule.rule_id, ch_name, metric, pipeline, severity)
                results.append({"channel": ch_name, "status": "sent", "detail": detail})
            except Exception as exc:
                results.append({"channel": ch_name, "status": "error", "detail": str(exc)})

        return results

    def notify_many(
        self, alert: Dict[str, Any], rules: List[AlertRule]
    ) -> List[Dict[str, Any]]:
        """Notifie pour chaque règle de la liste."""
        results = []
        for rule in rules:
            results.extend(self.notify(alert, rule))
        return results

    # ── Dispatch ──────────────────────────────────────────────────────────────

    def _dispatch(
        self,
        channel:  NotificationChannel,
        alert:    Dict[str, Any],
        pipeline: str,
        metric:   str,
        severity: str,
        rule:     AlertRule,
    ) -> str:
        if channel.is_console():
            return self._send_console(alert, pipeline, metric, severity)
        elif channel.is_teams():
            return self._send_teams(channel, alert, pipeline, metric, severity)
        elif channel.is_email():
            return self._send_email(channel, alert, pipeline, metric, severity, rule.recipients)
        else:
            return self._send_console(alert, pipeline, metric, severity)

    # ── Canaux ────────────────────────────────────────────────────────────────

    def _send_console(
        self, alert: Dict, pipeline: str, metric: str, severity: str
    ) -> str:
        ts  = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        val = alert.get("value", "N/A")
        det = alert.get("details", "")
        msg = (
            f"\n{'═'*60}\n"
            f"  🔔 ALERTE [{severity}]  {ts}\n"
            f"  Pipeline : {pipeline}\n"
            f"  Métrique : {metric}  =  {val}\n"
            f"  Détail   : {det}\n"
            f"{'═'*60}"
        )
        if not self.dry_run:
            print(msg)
        return msg.strip()

    def _send_teams(
        self,
        channel:  NotificationChannel,
        alert:    Dict,
        pipeline: str,
        metric:   str,
        severity: str,
    ) -> str:
        webhook_url = channel.config.get("webhook_url", "")
        if not webhook_url:
            raise ValueError("teams channel missing 'webhook_url'")

        color_map = {"LOW": "00B7C3", "MEDIUM": "FFB900", "HIGH": "E74856", "CRITICAL": "E81123"}
        color = color_map.get(severity, "E74856")
        val   = alert.get("value", "N/A")
        det   = alert.get("details", "")

        # Teams Adaptive Card (Connector Card format)
        payload = {
            "@type":      "MessageCard",
            "@context":   "https://schema.org/extensions",
            "themeColor": color,
            "summary":    f"[{severity}] {pipeline} — {metric}",
            "sections": [{
                "activityTitle": f"🔔 Alerte {severity} — Data Trust Agent",
                "activitySubtitle": f"Pipeline : **{pipeline}**",
                "facts": [
                    {"name": "Métrique",  "value": metric},
                    {"name": "Valeur",    "value": str(val)},
                    {"name": "Sévérité",  "value": severity},
                    {"name": "Détail",    "value": det or "—"},
                    {"name": "Timestamp", "value": alert.get("ts", "—")},
                ],
            }],
        }
        body = json.dumps(payload).encode("utf-8")

        if self.dry_run:
            return f"[dry-run] POST {webhook_url} — {len(body)} bytes"

        req = urllib.request.Request(
            webhook_url,
            data=body,
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            status = resp.status
        return f"HTTP {status}"

    def _send_email(
        self,
        channel:    NotificationChannel,
        alert:      Dict,
        pipeline:   str,
        metric:     str,
        severity:   str,
        recipients: List[str],
    ) -> str:
        cfg = channel.config
        smtp_host = cfg.get("smtp_host", "localhost")
        smtp_port = int(cfg.get("smtp_port", 587))
        smtp_user = cfg.get("smtp_user", "")
        smtp_pwd  = cfg.get("smtp_password", "")
        from_addr = cfg.get("from_addr", smtp_user)
        use_tls   = bool(cfg.get("use_tls", True))

        if not recipients:
            raise ValueError("email channel: aucun destinataire configuré")

        val = alert.get("value", "N/A")
        det = alert.get("details", "")
        ts  = alert.get("ts", datetime.now(timezone.utc).isoformat())

        subject = f"[Data Trust] [{severity}] {pipeline} — {metric}"
        body_html = f"""
        <html><body style="font-family:Arial,sans-serif">
        <h2 style="color:#E74856">🔔 Alerte {severity}</h2>
        <table border="1" cellpadding="6" cellspacing="0">
          <tr><td><b>Pipeline</b></td><td>{pipeline}</td></tr>
          <tr><td><b>Métrique</b></td><td>{metric}</td></tr>
          <tr><td><b>Valeur</b></td><td>{val}</td></tr>
          <tr><td><b>Sévérité</b></td><td>{severity}</td></tr>
          <tr><td><b>Détail</b></td><td>{det}</td></tr>
          <tr><td><b>Timestamp</b></td><td>{ts}</td></tr>
        </table>
        <p style="color:gray;font-size:11px">Data Trust Agent — alerting automatique</p>
        </body></html>
        """
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"]    = from_addr
        msg["To"]      = ", ".join(recipients)
        msg.attach(MIMEText(body_html, "html"))

        if self.dry_run:
            return f"[dry-run] SMTP {smtp_host}:{smtp_port} → {recipients}"

        with smtplib.SMTP(smtp_host, smtp_port, timeout=15) as server:
            if use_tls:
                server.starttls()
            if smtp_user:
                server.login(smtp_user, smtp_pwd)
            server.sendmail(from_addr, recipients, msg.as_string())
        return f"sent to {recipients}"

    # ── Cooldown ──────────────────────────────────────────────────────────────

    def _in_cooldown(
        self, rule_id: str, channel: str, metric: str, pipeline: str, minutes: int
    ) -> bool:
        if minutes <= 0:
            return False
        with self._connect_log() as conn:
            row = conn.execute(
                """SELECT sent_at FROM notification_log
                   WHERE rule_id=? AND channel=? AND metric_name=? AND pipeline=?
                   ORDER BY sent_at DESC LIMIT 1""",
                (rule_id, channel, metric, pipeline),
            ).fetchone()
        if not row:
            return False
        last = datetime.fromisoformat(row[0])
        delta = (datetime.now(timezone.utc) - last).total_seconds() / 60
        return delta < minutes

    def _log_sent(
        self, rule_id: str, channel: str, metric: str, pipeline: str, severity: str
    ) -> None:
        with self._connect_log() as conn:
            conn.execute(
                """INSERT INTO notification_log
                   (rule_id, channel, metric_name, pipeline, severity, sent_at)
                   VALUES (?,?,?,?,?,?)""",
                (rule_id, channel, metric, pipeline, severity,
                 datetime.now(timezone.utc).isoformat()),
            )
            conn.commit()

    # ── Internal ──────────────────────────────────────────────────────────────

    def _find_channel(self, name: str) -> NotificationChannel:
        for ch in self.channels:
            if ch.name == name:
                return ch
        return NotificationChannel("console")

    def _init_log_db(self) -> None:
        with self._connect_log() as conn:
            conn.executescript(_DDL_LOG)
            conn.commit()

    def _connect_log(self) -> sqlite3.Connection:
        return sqlite3.connect(self.log_db_path)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _extract(d: Dict, *keys: str, default: str = "") -> str:
    for k in keys:
        if k in d:
            return str(d[k])
    return default
