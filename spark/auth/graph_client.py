"""
graph_client — Appels Microsoft Graph exécutés au nom de l'utilisateur connecté
(délégation via son access token, obtenu par spark.auth.msal_auth).

Permet au Data Trust Agent d'agir avec l'identité Microsoft liée par l'utilisateur :
lire son profil, envoyer un e-mail en son nom, poster dans un canal Teams dont il est membre.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

import requests

GRAPH_BASE = "https://graph.microsoft.com/v1.0"


class GraphClient:
    """Client Microsoft Graph agissant avec le token délégué d'un utilisateur."""

    def __init__(self, access_token: str) -> None:
        self.access_token = access_token

    @property
    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.access_token}",
            "Content-Type": "application/json",
        }

    # ── Profil ────────────────────────────────────────────────────────────────

    def get_me(self) -> Dict[str, Any]:
        r = requests.get(f"{GRAPH_BASE}/me", headers=self._headers, timeout=10)
        r.raise_for_status()
        return r.json()

    # ── Outlook ───────────────────────────────────────────────────────────────

    def send_mail(self, subject: str, body_html: str, to: List[str]) -> Dict[str, Any]:
        """Envoie un e-mail au nom de l'utilisateur connecté (scope Mail.Send)."""
        payload = {
            "message": {
                "subject": subject,
                "body": {"contentType": "HTML", "content": body_html},
                "toRecipients": [{"emailAddress": {"address": addr}} for addr in to],
            },
            "saveToSentItems": True,
        }
        r = requests.post(f"{GRAPH_BASE}/me/sendMail", headers=self._headers, json=payload, timeout=15)
        if r.status_code == 202:
            return {"ok": True, "detail": f"e-mail envoyé à {to}"}
        return {"ok": False, "detail": f"HTTP {r.status_code} — {r.text[:200]}"}

    # ── Teams ─────────────────────────────────────────────────────────────────

    def list_joined_teams(self) -> List[Dict[str, Any]]:
        r = requests.get(f"{GRAPH_BASE}/me/joinedTeams", headers=self._headers, timeout=10)
        r.raise_for_status()
        return r.json().get("value", [])

    def list_channels(self, team_id: str) -> List[Dict[str, Any]]:
        r = requests.get(f"{GRAPH_BASE}/teams/{team_id}/channels", headers=self._headers, timeout=10)
        r.raise_for_status()
        return r.json().get("value", [])

    def send_channel_message(self, team_id: str, channel_id: str, content_html: str) -> Dict[str, Any]:
        """Poste un message dans un canal Teams au nom de l'utilisateur connecté
        (scope ChannelMessage.Send ; l'utilisateur doit être membre du canal)."""
        payload = {"body": {"contentType": "html", "content": content_html}}
        r = requests.post(
            f"{GRAPH_BASE}/teams/{team_id}/channels/{channel_id}/messages",
            headers=self._headers, json=payload, timeout=15,
        )
        if r.status_code in (200, 201):
            return {"ok": True, "detail": "message envoyé dans le canal Teams"}
        return {"ok": False, "detail": f"HTTP {r.status_code} — {r.text[:200]}"}
