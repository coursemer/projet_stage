"""
msal_auth — Authentification Microsoft (Entra ID / Azure AD) via MSAL,
flux "authorization code" pour une application web.

Variables d'environnement requises :
    AZURE_CLIENT_ID       — ID d'application (client) de l'app registration
    AZURE_CLIENT_SECRET   — secret client
    AZURE_TENANT_ID       — ID de tenant (ou "common" / "organizations")
    AZURE_REDIRECT_URI    — URI de redirection enregistrée (ex: http://localhost:8501)

Scopes délégués demandés : profil de base + envoi d'e-mail + lecture/écriture
Teams, pour permettre au Data Trust Agent d'agir au nom de l'utilisateur connecté.
"""
from __future__ import annotations

import os
from typing import Any, Dict

import msal

SCOPES = [
    "User.Read",
    "Mail.Send",
    "Team.ReadBasic.All",
    "Channel.ReadBasic.All",
    "ChannelMessage.Send",
]


def _require_env(name: str) -> str:
    value = os.environ.get(name, "")
    if not value:
        raise KeyError(name)
    return value


def _authority() -> str:
    tenant = os.getenv("AZURE_TENANT_ID", "common")
    return f"https://login.microsoftonline.com/{tenant}"


def _redirect_uri() -> str:
    return _require_env("AZURE_REDIRECT_URI")


def get_msal_app() -> msal.ConfidentialClientApplication:
    return msal.ConfidentialClientApplication(
        client_id=_require_env("AZURE_CLIENT_ID"),
        client_credential=_require_env("AZURE_CLIENT_SECRET"),
        authority=_authority(),
    )


def get_auth_url(state: str) -> str:
    """URL vers laquelle rediriger l'utilisateur pour se connecter avec son compte Microsoft."""
    return get_msal_app().get_authorization_request_url(
        scopes=SCOPES,
        state=state,
        redirect_uri=_redirect_uri(),
    )


def acquire_token_by_code(code: str) -> Dict[str, Any]:
    """Échange le code d'autorisation reçu sur l'URI de redirection contre un token."""
    result = get_msal_app().acquire_token_by_authorization_code(
        code=code,
        scopes=SCOPES,
        redirect_uri=_redirect_uri(),
    )
    if "access_token" not in result:
        raise RuntimeError(result.get("error_description", "Échec de l'authentification Microsoft"))
    return result


def acquire_token_by_refresh_token(refresh_token: str) -> Dict[str, Any]:
    """Renouvelle un access token expiré à partir du refresh token."""
    result = get_msal_app().acquire_token_by_refresh_token(refresh_token, scopes=SCOPES)
    if "access_token" not in result:
        raise RuntimeError(result.get("error_description", "Échec du renouvellement du token Microsoft"))
    return result
