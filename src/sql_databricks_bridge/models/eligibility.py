"""Eligibility run step definitions and helpers."""

from datetime import datetime, timezone

ELIGIBILITY_STEPS = [
    {"name": "stage1_job", "label": "Ejecutando Fase 1"},
    {"name": "stage1_download", "label": "Descarga Fase 1 lista"},
    {"name": "stage1_upload", "label": "Subir CSV Fase 1"},
    {"name": "stage2_job", "label": "Ejecutando Fase 2"},
    {"name": "stage2_download", "label": "Descarga Fase 2 lista"},
    {"name": "stage2_upload", "label": "Subir CSV Fase 2"},
    {"name": "finalize", "label": "Finalizar"},
    {"name": "complete", "label": "Completado"},
]

# Valid status transitions
VALID_STATUSES = {
    "pending",
    "stage1_running",
    "stage1_ready",
    "stage1_uploaded",
    "stage2_running",
    "stage2_ready",
    "stage2_uploaded",
    "finalized",
    "failed",
    "cancelled",
}

# Statuses that can be cancelled
CANCELLABLE_STATUSES = {
    "pending",
    "stage1_running",
    "stage1_ready",
    "stage1_uploaded",
    "stage2_running",
    "stage2_ready",
    "stage2_uploaded",
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
