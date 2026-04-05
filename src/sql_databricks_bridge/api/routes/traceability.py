"""Traceability API endpoints -- register and query process run snapshots."""

import csv
import io
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.traceability_table import (
    get_tag_tables,
    get_traceability_tag,
    insert_traceability_tag,
    list_traceability_tags,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/traceability", tags=["Traceability"])


# --- Schemas ---


class TagTableItem(BaseModel):
    table_name: str
    full_name: str
    catalog: str
    schema_name: str
    row_count: int
    size_bytes: int | None = None


class Tag(BaseModel):
    tag_id: str
    run_id: str
    country: str
    period: int
    stage: str
    triggered_by: str
    started_at: str
    completed_at: str | None = None
    status: str
    tables_count: int
    created_at: str


class TagDetail(Tag):
    tables: list[TagTableItem]


class TagListResponse(BaseModel):
    items: list[Tag]
    total: int
    limit: int
    offset: int


class CreateTagRequest(BaseModel):
    run_id: str = Field(..., description="Pipeline run ID")
    country: str = Field(..., description="Country code (e.g. 'chile')")
    period: int = Field(..., description="Period as YYYYMM (e.g. 202501)")
    stage: str = Field(..., description="Pipeline stage (e.g. 'inicio-precios')")
    triggered_by: str = Field(..., description="User email or identifier who triggered the run")
    started_at: str = Field(..., description="ISO 8601 start timestamp")
    completed_at: str | None = Field(default=None, description="ISO 8601 completion timestamp")
    status: str = Field(default="completed", description="Run status: 'completed' or 'failed'")
    tables: list[TagTableItem] = Field(default_factory=list, description="Tables produced by this run")


# --- Helpers ---


def _get_databricks_client(request: Request) -> DatabricksClient:
    client = getattr(request.app.state, "databricks_client", None)
    if client is not None:
        return client
    return DatabricksClient()


def _row_to_tag(row: dict[str, Any]) -> Tag:
    return Tag(
        tag_id=str(row["tag_id"]),
        run_id=str(row["run_id"]),
        country=str(row["country"]),
        period=int(row["period"]),
        stage=str(row["stage"]),
        triggered_by=str(row["triggered_by"]),
        started_at=str(row["started_at"]),
        completed_at=str(row["completed_at"]) if row.get("completed_at") else None,
        status=str(row["status"]),
        tables_count=int(row["tables_count"]),
        created_at=str(row["created_at"]),
    )


def _row_to_table_item(row: dict[str, Any]) -> TagTableItem:
    return TagTableItem(
        table_name=str(row["table_name"]),
        full_name=str(row["full_name"]),
        catalog=str(row["catalog"]),
        schema_name=str(row["schema_name"]),
        row_count=int(row["row_count"]),
        size_bytes=int(row["size_bytes"]) if row.get("size_bytes") is not None else None,
    )


# --- Endpoints ---


@router.get(
    "/tags",
    response_model=TagListResponse,
    summary="List process run snapshots",
)
async def list_tags(
    user: CurrentAzureADUser,
    raw_request: Request,
    country: str | None = Query(default=None),
    stage: str | None = Query(default=None),
    period: int | None = Query(default=None),
    date_from: str | None = Query(default=None),
    date_to: str | None = Query(default=None),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> TagListResponse:
    db_client = _get_databricks_client(raw_request)
    settings = get_settings()

    rows, total = list_traceability_tags(
        db_client,
        settings.traceability_tags_table,
        country=country,
        stage=stage,
        period=period,
        date_from=date_from,
        date_to=date_to,
        limit=limit,
        offset=offset,
    )

    return TagListResponse(
        items=[_row_to_tag(r) for r in rows],
        total=total,
        limit=limit,
        offset=offset,
    )


@router.get(
    "/tags/{tag_id}",
    response_model=TagDetail,
    summary="Get process run snapshot detail",
)
async def get_tag(
    tag_id: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> TagDetail:
    db_client = _get_databricks_client(raw_request)
    settings = get_settings()

    row = get_traceability_tag(db_client, settings.traceability_tags_table, tag_id=tag_id)
    if row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Tag '{tag_id}' not found"},
        )

    table_rows = get_tag_tables(db_client, settings.traceability_tag_tables_table, tag_id=tag_id)

    tag = _row_to_tag(row)
    return TagDetail(**tag.model_dump(), tables=[_row_to_table_item(t) for t in table_rows])


@router.get(
    "/tags/{tag_id}/tables/{table_name}/download",
    summary="Download table as CSV",
)
async def download_table_csv(
    tag_id: str,
    table_name: str,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> StreamingResponse:
    db_client = _get_databricks_client(raw_request)
    settings = get_settings()

    # Verify tag exists
    tag_row = get_traceability_tag(db_client, settings.traceability_tags_table, tag_id=tag_id)
    if tag_row is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Tag '{tag_id}' not found"},
        )

    # Find the table entry to get full_name
    table_rows = get_tag_tables(db_client, settings.traceability_tag_tables_table, tag_id=tag_id)
    matched = next((t for t in table_rows if t["table_name"] == table_name), None)
    if matched is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Table '{table_name}' not found in tag '{tag_id}'"},
        )

    full_name = str(matched["full_name"])

    try:
        rows = db_client.execute_sql(f"SELECT * FROM {full_name} LIMIT 100000")
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={"error": "databricks_error", "message": str(e)},
        )

    def generate_csv() -> Any:
        buf = io.StringIO()
        if rows:
            writer = csv.DictWriter(buf, fieldnames=list(rows[0].keys()))
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
        yield buf.getvalue()

    return StreamingResponse(
        generate_csv(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{table_name}.csv"'},
    )


@router.get(
    "/tags/{tag_id}/download",
    summary="Download all tables as ZIP (not yet implemented)",
    status_code=status.HTTP_501_NOT_IMPLEMENTED,
)
async def download_all_tables(
    tag_id: str,
    user: CurrentAzureADUser,
) -> dict[str, str]:
    raise HTTPException(
        status_code=status.HTTP_501_NOT_IMPLEMENTED,
        detail={"error": "not_implemented", "message": "ZIP download is not yet implemented"},
    )


@router.post(
    "/tags",
    response_model=Tag,
    status_code=status.HTTP_201_CREATED,
    summary="Register a process run snapshot",
)
async def create_tag(
    body: CreateTagRequest,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> Tag:
    import uuid

    db_client = _get_databricks_client(raw_request)
    settings = get_settings()

    tag_id = str(uuid.uuid4())

    insert_traceability_tag(
        db_client,
        settings.traceability_tags_table,
        settings.traceability_tag_tables_table,
        tag_id=tag_id,
        run_id=body.run_id,
        country=body.country,
        period=body.period,
        stage=body.stage,
        triggered_by=body.triggered_by,
        started_at=body.started_at,
        completed_at=body.completed_at,
        status=body.status,
        tables=[t.model_dump() for t in body.tables],
    )

    logger.info(f"User {user.email} registered traceability tag {tag_id} for {body.country}/{body.stage}")

    from datetime import datetime

    return Tag(
        tag_id=tag_id,
        run_id=body.run_id,
        country=body.country,
        period=body.period,
        stage=body.stage,
        triggered_by=body.triggered_by,
        started_at=body.started_at,
        completed_at=body.completed_at,
        status=body.status,
        tables_count=len(body.tables),
        created_at=datetime.utcnow().isoformat(),
    )
