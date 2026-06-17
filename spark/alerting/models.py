"""
Modèles de données — Semaine 14 : Alerting & notifications.

AlertRule        — règle de notification (pipeline × métrique × sévérité → canaux)
NotificationChannel — configuration d'un canal (teams/email/console)
IncidentRecord   — incident cycle de vie (open → investigating → resolved)
RunbookEntry     — procédure de remédiation (pattern → étapes)
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


# ─── Sévérités (ordre croissant) ─────────────────────────────────────────────

SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}


def severity_gte(sev: str, min_sev: str) -> bool:
    """True si sev >= min_sev dans l'ordre LOW < MEDIUM < HIGH < CRITICAL."""
    return SEVERITY_ORDER.get(sev.upper(), -1) >= SEVERITY_ORDER.get(min_sev.upper(), 0)


# ─── AlertRule ────────────────────────────────────────────────────────────────

@dataclass
class AlertRule:
    """
    Règle de notification configurable.

    pipeline_name : nom exact ou "*" pour tous les pipelines
    metric_name   : nom exact ou "*" pour toutes les métriques
    severity_min  : sévérité minimale (LOW/MEDIUM/HIGH/CRITICAL)
    channels      : liste de canaux cibles ("teams", "email", "console")
    recipients    : adresses e-mail destinataires (canal email)
    cooldown_min  : silence entre deux notifications identiques (minutes)
    enabled       : permet de désactiver sans supprimer
    rule_id       : généré automatiquement si vide
    created_at    : timestamp UTC de création
    """
    pipeline_name: str = "*"
    metric_name:   str = "*"
    severity_min:  str = "HIGH"
    channels:      List[str] = field(default_factory=lambda: ["console"])
    recipients:    List[str] = field(default_factory=list)
    cooldown_min:  int = 30
    enabled:       bool = True
    rule_id:       str = ""
    created_at:    datetime = field(default_factory=lambda: datetime.now(timezone.utc))

    def __post_init__(self) -> None:
        if not self.rule_id:
            ts = self.created_at.strftime("%Y%m%d%H%M%S%f")
            self.rule_id = f"{self.pipeline_name}__{self.metric_name}__{ts}"

    def matches(self, pipeline: str, metric: str, severity: str) -> bool:
        """Indique si cette règle s'applique à l'alerte donnée."""
        if not self.enabled:
            return False
        pipeline_ok = self.pipeline_name == "*" or self.pipeline_name == pipeline
        metric_ok   = self.metric_name   == "*" or self.metric_name   == metric
        severity_ok = severity_gte(severity, self.severity_min)
        return pipeline_ok and metric_ok and severity_ok


# ─── NotificationChannel ─────────────────────────────────────────────────────

@dataclass
class NotificationChannel:
    """
    Configuration d'un canal de notification.

    name   : identifiant du canal ("teams", "email", "console")
    config : paramètres spécifiques au canal

    Canal "teams"  → config["webhook_url"] (str)
    Canal "email"  → config["smtp_host", "smtp_port", "smtp_user", "smtp_password",
                            "from_addr", "use_tls"]
    Canal "console"→ config peut être vide (affichage terminal)
    """
    name:   str
    config: Dict[str, Any] = field(default_factory=dict)

    def is_teams(self)   -> bool: return self.name == "teams"
    def is_email(self)   -> bool: return self.name == "email"
    def is_console(self) -> bool: return self.name == "console"


# ─── IncidentRecord ───────────────────────────────────────────────────────────

INCIDENT_STATUSES = ("open", "investigating", "resolved")


@dataclass
class IncidentRecord:
    """
    Incident cycle de vie (open → investigating → resolved).

    id          : identifiant auto-incrémenté (0 avant insertion)
    pipeline    : pipeline source
    title       : titre court de l'incident
    description : détail de l'incident
    severity    : LOW / MEDIUM / HIGH / CRITICAL
    status      : open / investigating / resolved
    alert_ids   : IDs des alertes AlertManager associées
    notes       : journal des actions (liste de texte horodaté)
    """
    pipeline:     str
    title:        str
    description:  str = ""
    severity:     str = "HIGH"
    status:       str = "open"
    alert_ids:    List[int] = field(default_factory=list)
    notes:        List[str] = field(default_factory=list)
    id:           int = 0
    created_at:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at:   datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at:  Optional[datetime] = None

    def add_note(self, text: str) -> None:
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        self.notes.append(f"[{ts}] {text}")
        self.updated_at = datetime.now(timezone.utc)

    def escalate(self) -> str:
        """Monte la sévérité d'un cran. Retourne la nouvelle valeur."""
        order = ["LOW", "MEDIUM", "HIGH", "CRITICAL"]
        idx = order.index(self.severity.upper()) if self.severity.upper() in order else 1
        self.severity = order[min(idx + 1, 3)]
        self.updated_at = datetime.now(timezone.utc)
        return self.severity

    @property
    def is_open(self) -> bool:
        return self.status != "resolved"


# ─── RunbookEntry ─────────────────────────────────────────────────────────────

@dataclass
class RunbookEntry:
    """
    Procédure de remédiation pour un type d'incident.

    pattern     : motif de métrique ciblée (ex: "rejection_rate", "volume_drop")
    title       : titre lisible du runbook
    steps       : liste d'étapes ordonnées (texte libre ou commandes shell)
    tags        : mots-clés (ex: ["data-quality", "etl"])
    auto_exec   : True si les steps peuvent être exécutées automatiquement
    """
    pattern:   str
    title:     str
    steps:     List[str] = field(default_factory=list)
    tags:      List[str] = field(default_factory=list)
    auto_exec: bool = False
    id:        int = 0
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
