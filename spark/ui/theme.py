"""
theme — thème visuel partagé (sombre, accent bleu/violet en dégradé) pour
la page de connexion et le dashboard Streamlit du Data Trust Agent.
"""
from __future__ import annotations

import streamlit as st

ACCENT_A = "#2563eb"   # bleu
ACCENT_B = "#7c3aed"   # violet
ACCENT_GRADIENT = f"linear-gradient(135deg, {ACCENT_A}, {ACCENT_B})"

_CSS = f"""
<style>
:root {{
    --dt-bg: #0a0a12;
    --dt-card: #14141f;
    --dt-border: #24243a;
    --dt-text: #f1f5f9;
    --dt-muted: #8b8ba7;
    --dt-accent-a: {ACCENT_A};
    --dt-accent-b: {ACCENT_B};
}}

.stApp {{ background: var(--dt-bg); }}

section[data-testid="stSidebar"] {{
    background: #0d0d16;
    border-right: 1px solid var(--dt-border);
}}
section[data-testid="stSidebar"] * {{ color: var(--dt-text); }}
section[data-testid="stSidebar"] .stCaption, section[data-testid="stSidebar"] small {{
    color: var(--dt-muted) !important;
}}

/* Cartes KPI (st.metric) */
div[data-testid="stMetric"] {{
    background: var(--dt-card);
    border: 1px solid var(--dt-border);
    border-radius: 16px;
    padding: 16px 18px 12px 18px;
    position: relative;
    overflow: hidden;
}}
div[data-testid="stMetric"]::before {{
    content: '';
    position: absolute; top: 0; left: 0; right: 0; height: 3px;
    background: linear-gradient(135deg, var(--dt-accent-a), var(--dt-accent-b));
}}
div[data-testid="stMetricLabel"] {{ color: var(--dt-muted) !important; }}
div[data-testid="stMetricValue"] {{ color: var(--dt-text) !important; }}

/* Boutons pleins (gradient accent) */
.stButton > button, .stLinkButton > a {{
    background: linear-gradient(135deg, var(--dt-accent-a), var(--dt-accent-b)) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 10px !important;
    font-weight: 600 !important;
    box-shadow: 0 6px 20px rgba(37, 99, 235, .25);
}}
.stButton > button:hover, .stLinkButton > a:hover {{ filter: brightness(1.08); }}

/* Cartes (expanders, dataframes) */
div[data-testid="stExpander"] {{
    background: var(--dt-card);
    border: 1px solid var(--dt-border);
    border-radius: 14px;
}}
div[data-testid="stDataFrame"] {{
    border: 1px solid var(--dt-border);
    border-radius: 12px;
    overflow: hidden;
}}

/* Navigation (radio) façon pilules dans la sidebar */
section[data-testid="stSidebar"] div[role="radiogroup"] label {{
    background: var(--dt-card);
    border: 1px solid var(--dt-border);
    border-radius: 10px;
    padding: 6px 10px;
    margin-bottom: 4px;
}}

/* Badge "connecté" */
.dt-badge {{
    display: inline-block; padding: 3px 10px; border-radius: 999px;
    background: linear-gradient(135deg, var(--dt-accent-a), var(--dt-accent-b));
    color: #fff; font-size: .7rem; font-weight: 700; letter-spacing: .4px;
    vertical-align: middle; margin-left: 8px;
}}
</style>
"""

_LOGIN_CSS = """
<style>
.dt-login-wrap {
    position: relative;
    min-height: 62vh;
    display: flex;
    align-items: center;
    justify-content: center;
}
.dt-orb {
    position: absolute;
    width: 240px; height: 240px;
    border-radius: 50%;
    filter: blur(6px);
    opacity: .8;
    z-index: 0;
}
.dt-orb-a {
    background: radial-gradient(circle at 30% 30%, #60a5fa, #1d4ed8);
    top: 6%; left: 24%;
}
.dt-orb-b {
    background: radial-gradient(circle at 30% 30%, #a78bfa, #6d28d9);
    bottom: 4%; right: 22%;
}
.dt-login-card {
    position: relative; z-index: 1;
    background: rgba(20, 20, 31, .92);
    backdrop-filter: blur(14px);
    border: 1px solid #24243a;
    border-radius: 20px;
    padding: 40px 44px 30px 44px;
    width: 380px;
    box-shadow: 0 24px 70px rgba(0, 0, 0, .55);
}
.dt-login-card h1 {
    color: #f1f5f9; font-size: 1.6rem; font-weight: 800; margin-bottom: 6px;
}
.dt-login-card p {
    color: #8b8ba7; font-size: .88rem; line-height: 1.5; margin-bottom: 26px;
}
</style>
"""


def inject_app_theme() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)


def inject_login_theme() -> None:
    st.markdown(_CSS, unsafe_allow_html=True)
    st.markdown(_LOGIN_CSS, unsafe_allow_html=True)
