"""Eligibility run step definitions and helpers."""

from datetime import datetime, timezone

ELIGIBILITY_STEPS = [
    {"name": "run_pipeline", "label": "Ejecutar Pipeline (Phase 0–9)"},
    {"name": "download_results", "label": "Descargar Resultados"},
    {"name": "approve", "label": "Aprobar Resultados"},
    {"name": "apply_sql", "label": "Aplicar a SQL Server"},
    {"name": "download_mordom", "label": "Descargar MorDom actualizado"},
    {"name": "upload_mordom", "label": "Subir MorDom corregido"},
    {"name": "apply_mordom", "label": "Aplicar correcciones MorDom"},
    {"name": "complete", "label": "Listo"},
]

# Valid status transitions
VALID_STATUSES = {
    "pending",
    "running",
    "results_ready",
    "applying_sql",
    "mordom_downloading",
    "mordom_ready",
    "mordom_uploaded",
    "applying_mordom",
    "ready",
    "failed",
    "cancelled",
}

# Statuses that can be cancelled
CANCELLABLE_STATUSES = {
    "pending",
    "running",
    "results_ready",
    "applying_sql",
    "mordom_downloading",
    "mordom_ready",
    "mordom_uploaded",
    "applying_mordom",
}


def build_eligibility_steps() -> list[dict]:
    """Build initial steps list with all steps pending."""
    return [
        {
            "name": step["name"],
            "label": step["label"],
            "status": "pending",
            "started_at": None,
            "completed_at": None,
        }
        for step in ELIGIBILITY_STEPS
    ]


def update_step_status(
    steps: list[dict], step_name: str, new_status: str
) -> list[dict]:
    """Update a specific step's status in the steps list."""
    now = datetime.now(timezone.utc).isoformat()
    for step in steps:
        if step["name"] == step_name:
            step["status"] = new_status
            if new_status == "running" and step["started_at"] is None:
                step["started_at"] = now
            if new_status == "completed":
                step["completed_at"] = now
            break
    return steps
