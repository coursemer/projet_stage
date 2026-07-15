"""
TeamsNotifier — envoi de notifications Microsoft Teams via Incoming Webhook.

Lit automatiquement TEAMS_WEBHOOK_URL depuis l'environnement.
Peut aussi être instancié avec une URL explicite.

Usage :
    from spark.alerting.teams_notifier import TeamsNotifier

    notifier = TeamsNotifier()                          # lit TEAMS_WEBHOOK_URL
    notifier = TeamsNotifier(webhook_url="https://…")   # URL explicite

    ok = notifier.send_alert(
        pipeline="clean_sales",
        metric="rejection_rate_pct",
        severity="CRITICAL",
        value=45.2,
        details="Taux de rejet 45.2% > seuil 20%",
    )

    ok = notifier.send_test()   # carte de test "pong"
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


_SEV_COLOR = {
    "CRITICAL": "E81123",
    "HIGH":     "E74856",
    "WARNING":  "FFB900",
    "MEDIUM":   "FFB900",
    "LOW":      "00B7C3",
    "INFO":     "0078D4",
}


class TeamsNotifier:
    """Envoi de notifications vers un canal Microsoft Teams (Incoming Webhook)."""

    def __init__(
        self,
        webhook_url: Optional[str] = None,
        dry_run: bool = False,
    ) -> None:
        self.webhook_url = webhook_url or os.getenv("TEAMS_WEBHOOK_URL", "")
        self.dry_run     = dry_run

    @property
    def configured(self) -> bool:
        return bool(self.webhook_url)

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
        """Envoie une carte d'alerte dans Teams. Retourne {"ok": bool, "detail": str}."""
        if not self.configured:
            return {"ok": False, "detail": "TEAMS_WEBHOOK_URL non configuré"}

        color   = _SEV_COLOR.get(severity.upper(), "E74856")
        ts      = ts or datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

        payload = {
            "@type":      "MessageCard",
            "@context":   "https://schema.org/extensions",
            "themeColor": color,
            "summary":    f"[{severity}] {pipeline} — {metric}",
            "sections": [{
                "activityTitle":    f"🔔 Alerte **{severity}** — Data Trust Agent",
                "activitySubtitle": f"Pipeline : **{pipeline}**",
                "activityImage":    "https://upload.wikimedia.org/wikipedia/commons/thumb/4/44/Microsoft_logo.svg/512px-Microsoft_logo.svg.png",
                "facts": [
                    {"name": "Métrique",  "value": metric},
                    {"name": "Valeur",    "value": str(value)},
                    {"name": "Sévérité",  "value": f"**{severity}**"},
                    {"name": "Détail",    "value": details or "—"},
                    {"name": "Timestamp", "value": ts},
                ],
                "markdown": True,
            }],
            "potentialAction": [{
                "@type": "OpenUri",
                "name":  "Voir le dashboard",
                "targets": [{"os": "default", "uri": "http://localhost:8501"}],
            }],
        }
        return self._post(payload)

    def send_many(self, alerts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Envoie une notification pour chaque alerte de la liste."""
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
        """Envoie une carte de test pour vérifier la connexion Teams."""
        if not self.configured:
            return {"ok": False, "detail": "TEAMS_WEBHOOK_URL non configuré"}

        payload = {
            "@type":      "MessageCard",
            "@context":   "https://schema.org/extensions",
            "themeColor": "0078D4",
            "summary":    "Test Data Trust Agent",
            "sections": [{
                "activityTitle":    "✅ Connexion Teams opérationnelle",
                "activitySubtitle": "Data Trust Agent — test de notification",
                "facts": [
                    {"name": "Statut",    "value": "OK"},
                    {"name": "Timestamp", "value": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")},
                    {"name": "Dashboard", "value": "http://localhost:8501"},
                ],
                "markdown": True,
            }],
        }
        return self._post(payload)

    # ── Internal ──────────────────────────────────────────────────────────────

    def _post(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")

        if self.dry_run:
            return {"ok": True, "detail": f"[dry-run] POST {self.webhook_url} — {len(body)} bytes"}

        try:
            req = urllib.request.Request(
                self.webhook_url,
                data=body,
                headers={"Content-Type": "application/json; charset=utf-8"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=10) as resp:
                status = resp.status
                body_r = resp.read().decode(errors="replace")
            if status == 200 and body_r.strip() == "1":
                return {"ok": True, "detail": f"HTTP {status}"}
            return {"ok": True, "detail": f"HTTP {status} — {body_r[:80]}"}
        except Exception as exc:
            return {"ok": False, "detail": str(exc)}
