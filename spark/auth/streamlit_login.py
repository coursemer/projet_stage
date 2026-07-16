"""
streamlit_login — Porte de connexion Microsoft pour le dashboard Streamlit.

Implémente le flux "authorization code" MSAL directement dans Streamlit :
- affiche un bouton "Se connecter avec Microsoft" tant que l'utilisateur n'est pas authentifié
- gère le retour de redirection (code/state dans st.query_params)
- garde le token (+ refresh token) en session_state pour le reste de la session Streamlit
- expose require_login() à appeler tout en haut du script, avant st.set_page_config()

Le paramètre CSRF "state" ne peut pas être vérifié via st.session_state : la
redirection vers login.microsoftonline.com puis le retour vers l'app constituent
une navigation complète du navigateur, donc Streamlit peut démarrer une toute
nouvelle session (session_state vide) au retour. Le "state" est donc suivi dans
un registre au niveau du process (survit tant que le serveur Streamlit tourne),
et non dans session_state.
"""
from __future__ import annotations

import threading
import time
import uuid
from typing import Any, Dict, Optional

import streamlit as st

from . import msal_auth
from ..ui import theme

SESSION_KEY = "ms_auth"

_STATE_TTL_SECONDS = 600  # durée max pour compléter une connexion
_pending_states: Dict[str, float] = {}
_pending_states_lock = threading.Lock()


def _new_state() -> str:
    state = str(uuid.uuid4())
    now = time.time()
    with _pending_states_lock:
        for s, ts in list(_pending_states.items()):
            if now - ts > _STATE_TTL_SECONDS:
                _pending_states.pop(s, None)
        _pending_states[state] = now
    return state


def _consume_state(state: Optional[str]) -> bool:
    if not state:
        return False
    with _pending_states_lock:
        created_at = _pending_states.pop(state, None)
    return created_at is not None and (time.time() - created_at) <= _STATE_TTL_SECONDS


def _token_valid(auth: Optional[Dict[str, Any]]) -> bool:
    return bool(auth) and auth.get("expires_at", 0) > time.time()


def _to_session(result: Dict[str, Any]) -> Dict[str, Any]:
    claims = result.get("id_token_claims", {}) or {}
    return {
        "access_token": result["access_token"],
        "refresh_token": result.get("refresh_token"),
        "expires_at": time.time() + result.get("expires_in", 3600) - 60,
        "name": claims.get("name"),
        "email": claims.get("preferred_username") or claims.get("email"),
    }


def _refresh_if_needed() -> None:
    auth = st.session_state.get(SESSION_KEY)
    if auth and not _token_valid(auth) and auth.get("refresh_token"):
        try:
            result = msal_auth.acquire_token_by_refresh_token(auth["refresh_token"])
            st.session_state[SESSION_KEY] = _to_session(result)
        except Exception:
            st.session_state.pop(SESSION_KEY, None)


def _render_login_page(error: Optional[str] = None) -> None:
    st.set_page_config(page_title="Data Trust Agent — Connexion", page_icon="🛡️", layout="centered")
    theme.inject_login_theme()

    if error:
        st.error(error)

    state = _new_state()
    try:
        auth_url = msal_auth.get_auth_url(state)
    except KeyError as exc:
        st.error(
            f"Configuration Microsoft manquante : variable d'environnement {exc} absente. "
            "Voir .env.example."
        )
        st.stop()

    st.markdown(
        """
        <div class="dt-login-wrap">
          <div class="dt-orb dt-orb-a"></div>
          <div class="dt-orb dt-orb-b"></div>
          <div class="dt-login-card">
            <h1>Data Trust Agent</h1>
            <p>Connectez-vous avec votre compte Microsoft pour accéder au dashboard
            et lier votre compte (envoi d'e-mails / messages Teams en votre nom).</p>
        """,
        unsafe_allow_html=True,
    )
    st.link_button("Se connecter avec Microsoft", auth_url, type="primary", use_container_width=True)
    st.markdown("</div></div>", unsafe_allow_html=True)


def require_login() -> Dict[str, Any]:
    """Bloque le rendu tant que l'utilisateur n'est pas connecté à son compte Microsoft.

    À appeler en tout premier, avant tout autre appel Streamlit (y compris
    st.set_page_config côté page principale). Retourne les infos de session
    (name, email, access_token, refresh_token) une fois l'utilisateur authentifié.
    """
    params = st.query_params

    if "error" in params:
        st.session_state["auth_error"] = (
            f"{params.get('error')} — {params.get('error_description', '')}"
        )
        st.query_params.clear()
        st.rerun()

    if "code" in params:
        if not _consume_state(params.get("state")):
            st.query_params.clear()
            _render_login_page(error="Lien de connexion expiré ou déjà utilisé, veuillez réessayer.")
            st.stop()
        try:
            result = msal_auth.acquire_token_by_code(params["code"])
            st.session_state[SESSION_KEY] = _to_session(result)
        except Exception as exc:
            st.session_state["auth_error"] = str(exc)
        st.query_params.clear()
        st.rerun()

    _refresh_if_needed()
    auth = st.session_state.get(SESSION_KEY)

    if not _token_valid(auth):
        st.session_state.pop(SESSION_KEY, None)
        error = None
        if st.session_state.get("auth_error"):
            error = f"Échec de connexion Microsoft : {st.session_state.pop('auth_error')}"
        _render_login_page(error=error)
        st.stop()

    return auth


def logout() -> None:
    st.session_state.pop(SESSION_KEY, None)
    st.query_params.clear()
    st.rerun()
