"""Pydantic schemas for API requests and responses."""

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JobStatus(str, Enum):
    """Job execution status."""

    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class ExtractionRequest(BaseModel):
    """Request to start a data extraction job."""

    queries_path: str = Field(
        ...,
        description="Path to directory containing SQL query files",
    )
    config_path: str = Field(
        ...,
        description="Path to directory containing YAML config files",
    )
    country: str = Field(
        ...,
        description="Country name for parameter resolution",
    )
    destination: str | None = Field(
        default=None,
        description="Databricks volume path (used for staging only; tables written to catalog.schema)",
    )
    target_catalog: str | None = Field(
        default=None,
        description="Override default Unity Catalog name for output tables",
    )
    target_schema: str | None = Field(
        default=None,
        description="Override default schema name for output tables",
    )
    queries: list[str] | None = Field(
        default=None,
        description="Specific queries to run (None = all)",
    )
    overwrite: bool = Field(
        default=False,
        description="Overwrite existing files",
    )
    chunk_size: int = Field(
        default=100_000,
        description="Rows per extraction chunk",
    )


class ExtractionResponse(BaseModel):
    """Response when extraction job is started."""

    job_id: str = Field(..., description="Unique job identifier")
    status: JobStatus = Field(default=JobStatus.PENDING)
    message: str = Field(default="Job queued")
    queries_count: int = Field(..., description="Number of queries to execute")


class QueryResult(BaseModel):
    """Result of a single query execution."""

    query_name: str
    status: JobStatus
    rows_extracted: int = 0
    table_name: str | None = None
    files_created: list[str] = Field(default_factory=list)
    error: str | None = None
    duration_seconds: float = 0.0
    started_at: datetime | None = None


class JobStatusResponse(BaseModel):
    """Detailed job status response."""

    job_id: str
    status: JobStatus
    country: str
    destination: str
    created_at: datetime
    started_at: datetime | None = None
    completed_at: datetime | None = None
    queries_total: int
    queries_completed: int
    queries_failed: int
    results: list[QueryResult] = Field(default_factory=list)
    error: str | None = None


class SyncEventOperation(str, Enum):
    """Sync event operation types."""

    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


class SyncEventStatus(str, Enum):
    """Sync event status."""

    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"


class SyncEvent(BaseModel):
    """Event for Databricks to SQL sync."""

    event_id: str
    operation: SyncEventOperation
    source_table: str = Field(..., description="Databricks source table (catalog.schema.table)")
    target_table: str = Field(..., description="SQL Server target table (schema.table)")
    country: str = Field(..., description="Country code for SQL Server connection")
    primary_keys: list[str] = Field(
        default_factory=list, description="PK columns for UPDATE/DELETE"
    )
    priority: int = Field(default=0, description="Higher priority processed first")
    status: SyncEventStatus = Field(default=SyncEventStatus.PENDING)
    rows_expected: int | None = None
    rows_affected: int | None = None
    discrepancy: int | None = None
    warning: str | None = None
    error_message: str | None = None
    created_at: datetime | None = None
    processed_at: datetime | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class PermissionAccess(str, Enum):
    """Permission access levels."""

    READ = "read"
    WRITE = "write"
    READ_WRITE = "read_write"


class TablePermission(BaseModel):
    """Permission for a single table."""

    table: str
    access: PermissionAccess
    max_delete_rows: int | None = Field(
        default=None,
        description="Maximum rows allowed for DELETE operations",
    )


class UserPermissions(BaseModel):
    """User with their table permissions."""

    token: str
    name: str
    permissions: list[TablePermission] = Field(default_factory=list)
