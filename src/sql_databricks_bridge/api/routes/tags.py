"""Version tags API endpoints -- tag and query Delta table versions."""

import logging

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from sql_databricks_bridge.api.dependencies import CurrentAzureADUser
from sql_databricks_bridge.core.config import get_settings
from sql_databricks_bridge.core.delta_writer import DeltaTableWriter
from sql_databricks_bridge.db.databricks import DatabricksClient
from sql_databricks_bridge.db.version_tags_table import (
    delete_version_tag,
    get_version_tag,
    insert_version_tag,
    list_version_tags,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tags", tags=["Tags"])


# --- Schemas ---


class CreateTagRequest(BaseModel):
    table_name: str = Field(..., description="Fully-qualified Delta table name (catalog.schema.table)")
    version: int | None = Field(
        default=None,
        description="Delta table version to tag. null = latest version.",
    )
    tag: str = Field(..., description="Tag label (e.g. 'validated', 'pre-calibration')")
    description: str | None = Field(default=None, description="Optional description for the tag")


class TagResponse(BaseModel):
    table_name: str
    version: int
    tag: str
    description: str | None = None
    created_by: str
    created_at: str
    job_id: str | None = None


class TagListResponse(BaseModel):
    items: list[TagResponse]
    total: int
    limit: int
    offset: int


class HistoryEntry(BaseModel):
    version: int | None = None
    timestamp: str | None = None
    operation: str | None = None
    user_name: str | None = None
    operation_parameters: str | None = None


class TableHistoryResponse(BaseModel):
    table_name: str
    history: list[HistoryEntry]
    tags: list[TagResponse]
    total: int
    limit: int
    offset: int


# --- Helpers ---


def _get_databricks_client(request: Request) -> DatabricksClient:
    """Get DatabricksClient from app.state, or create a new one as fallback."""
    client = getattr(request.app.state, "databricks_client", None)
    if client is not None:
        return client
    return DatabricksClient()


def _row_to_tag_response(row: dict) -> TagResponse:
    """Convert a DB row dict to TagResponse."""
    return TagResponse(
        table_name=row["table_name"],
        version=int(row["version"]),
        tag=row["tag"],
        description=row.get("description"),
        created_by=row["created_by"],
        created_at=str(row["created_at"]),
        job_id=row.get("job_id"),
    )


# --- Endpoints ---


@router.post(
    "",
    response_model=TagResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create version tag",
    description="Tag a specific version of a Delta table. If version is null, tags the latest version.",
)
async def create_tag(
    request: CreateTagRequest,
    user: CurrentAzureADUser,
    raw_request: Request,
) -> TagResponse:
    """Create a new version tag for a Delta table."""
    if not user.can_trigger_sync:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": "User role does not allow managing tags"},
        )

    db_client = _get_databricks_client(raw_request)
    settings = get_settings()
    tags_tbl = settings.version_tags_table

    # Resolve version if not provided
    version = request.version
    if version is None:
        writer = DeltaTableWriter(db_client)
        try:
            version = writer.get_current_version(request.table_name)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail={"error": "not_found", "message": f"Table not found or no history: {e}"},
            )

    # Check if tag already exists for this table
    existing = get_version_tag(db_client, tags_tbl, table_name=request.table_name, tag=request.tag)
    if existing:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail={
                "error": "conflict",
                "message": f"Tag '{request.tag}' already exists for table '{request.table_name}' at version {existing['version']}",
            },
        )

    insert_version_tag(
        db_client,
        tags_tbl,
        table_name=request.table_name,
        version=version,
        tag=request.tag,
        created_by=user.email,
        description=request.description,
    )

    logger.info(f"User {user.email} tagged {request.table_name} v{version} as '{request.tag}'")

    return TagResponse(
        table_name=request.table_name,
        version=version,
        tag=request.tag,
        description=request.description,
        created_by=user.email,
        created_at="just_created",
    )


@router.get(
    "",
    response_model=TagListResponse,
    summary="List version tags",
    description="List version tags with optional filtering by table name or tag.",
)
async def list_tags(
    user: CurrentAzureADUser,
    raw_request: Request,
    table_name: str | None = Query(default=None, description="Filter by table name"),
    tag: str | None = Query(default=None, description="Filter by tag"),
    limit: int = Query(default=50, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> TagListResponse:
    """List version tags."""
    db_client = _get_databricks_client(raw_request)
    settings = get_settings()
    tags_tbl = settings.version_tags_table

    rows, total = list_version_tags(
        db_client,
        tags_tbl,
        table_name=table_name,
        tag=tag,
        limit=limit,
        offset=offset,
    )

    items = [_row_to_tag_response(row) for row in rows]

    return TagListResponse(items=items, total=total, limit=limit, offset=offset)


@router.delete(
    "/{tag}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete version tag",
    description="Delete a version tag for a specific table.",
)
async def delete_tag(
    tag: str,
    user: CurrentAzureADUser,
    raw_request: Request,
    table_name: str = Query(..., description="Fully-qualified Delta table name"),
) -> None:
    """Delete a version tag."""
    if not user.can_trigger_sync:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail={"error": "forbidden", "message": "User role does not allow managing tags"},
        )

    db_client = _get_databricks_client(raw_request)
    settings = get_settings()
    tags_tbl = settings.version_tags_table

    deleted = delete_version_tag(db_client, tags_tbl, table_name=table_name, tag=tag)
    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Tag '{tag}' not found for table '{table_name}'"},
        )

    logger.info(f"User {user.email} deleted tag '{tag}' from {table_name}")


@router.get(
    "/history/{table_name:path}",
    response_model=TableHistoryResponse,
    summary="Get table history with tags",
    description="Get Delta table version history annotated with any existing tags.",
)
async def get_table_history_with_tags(
    table_name: str,
    user: CurrentAzureADUser,
    raw_request: Request,
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
) -> TableHistoryResponse:
    """Get Delta table version history with associated tags."""
    db_client = _get_databricks_client(raw_request)
    settings = get_settings()
    tags_tbl = settings.version_tags_table

    writer = DeltaTableWriter(db_client)

    # Get total version count from latest version number
    try:
        total = writer.get_current_version(table_name) + 1
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Table not found or no history: {e}"},
        )

    # DESCRIBE HISTORY doesn't support OFFSET, so over-fetch and slice
    try:
        all_rows = writer.get_history(table_name, limit=offset + limit)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail={"error": "not_found", "message": f"Cannot read history: {e}"},
        )

    page_rows = all_rows[offset:offset + limit]

    history = []
    for row in page_rows:
        history.append(
            HistoryEntry(
                version=int(row["version"]) if row.get("version") is not None else None,
                timestamp=str(row.get("timestamp", "")),
                operation=row.get("operation"),
                user_name=row.get("userName"),
                operation_parameters=str(row.get("operationParameters", "")),
            )
        )

    # Get tags for this table
    tag_rows, _ = list_version_tags(
        db_client, tags_tbl, table_name=table_name, limit=200
    )
    tags = [_row_to_tag_response(row) for row in tag_rows]

    return TableHistoryResponse(
        table_name=table_name,
        history=history,
        tags=tags,
        total=total,
        limit=limit,
        offset=offset,
    )
