"""Configuration management using pydantic-settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SQLServerSettings(BaseSettings):
    """SQL Server connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="SQLSERVER_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="localhost", description="SQL Server hostname")
    port: int = Field(default=1433, description="SQL Server port")
    database: str = Field(default="master", description="Database name")
    username: str = Field(default="sa", description="Username")
    password: SecretStr = Field(default=SecretStr(""), description="Password")
    driver: str = Field(
        default="ODBC Driver 17 for SQL Server",
        description="ODBC driver name",
    )
    trust_server_certificate: bool = Field(
        default=True,
        description="Trust server certificate",
    )
    use_windows_auth: bool = Field(
        default=True,
        description="Use Windows Authentication (Trusted_Connection)",
    )
    query_timeout_seconds: int = Field(
        default=600,
        description=(
            "Per-statement pyodbc timeout (covers SQLExecute, not SQLFetch). "
            "Use keepalive_seconds for fetch-side hangs."
        ),
    )
    keepalive_seconds: int = Field(
        default=30,
        description=(
            "TCP keep-alive interval in seconds, passed to the ODBC driver via "
            "the connection string 'Keepalive' parameter. Detects dead "
            "connections during long-running fetchmany() calls (which are not "
            "covered by SQL_ATTR_QUERY_TIMEOUT). Set to 0 to disable."
        ),
    )
    lock_timeout_seconds: int = Field(
        default=60,
        description=(
            "Session SET LOCK_TIMEOUT. Any query that waits on an incompatible "
            "lock longer than this raises SQL Server error 1222 instead of "
            "blocking indefinitely (e.g. on a schema lock from concurrent DDL, "
            "which READ UNCOMMITTED does not bypass). Set to -1 to wait forever."
        ),
    )

    @property
    def connection_string(self) -> str:
        """Build ODBC connection string."""
        trust_cert = "yes" if self.trust_server_certificate else "no"
        if self.use_windows_auth:
            return (
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.host},{self.port};"
                f"DATABASE={self.database};"
                f"Trusted_Connection=yes;"
                f"TrustServerCertificate={trust_cert}"
            )
        return (
            f"DRIVER={{{self.driver}}};"
            f"SERVER={self.host},{self.port};"
            f"DATABASE={self.database};"
            f"UID={self.username};"
            f"PWD={self.password.get_secret_value()};"
            f"TrustServerCertificate={trust_cert}"
        )

    @property
    def sqlalchemy_url(self) -> str:
        """Build SQLAlchemy connection URL."""
        trust_cert = "yes" if self.trust_server_certificate else "no"
        driver_encoded = self.driver.replace(" ", "+")
        if self.use_windows_auth:
            return (
                f"mssql+pyodbc://@{self.host}:{self.port}/{self.database}"
                f"?driver={driver_encoded}&TrustServerCertificate={trust_cert}"
                f"&Trusted_Connection=yes"
            )
        return (
            f"mssql+pyodbc://{self.username}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?driver={driver_encoded}&TrustServerCertificate={trust_cert}"
        )


class DatabricksSettings(BaseSettings):
    """Databricks connection settings."""

    model_config = SettingsConfigDict(
        env_prefix="DATABRICKS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    host: str = Field(default="", description="Databricks workspace URL")
    token: SecretStr = Field(default=SecretStr(""), description="Personal access token")
    client_id: str | None = Field(default=None, description="OAuth client ID (Service Principal)")
    client_secret: SecretStr | None = Field(
        default=None,
        description="OAuth client secret",
    )
    warehouse_id: str = Field(default="", description="SQL Warehouse ID for statement execution")
    poller_warehouse_id: str = Field(
        default="",
        description=(
            "Optional dedicated SQL Warehouse ID for the EventPoller. "
            "When set, reverse-sync event queries/updates run on this warehouse "
            "instead of the main one, avoiding contention with diff-sync/triggers."
        ),
    )
    catalog: str = Field(default="main", description="Unity Catalog name")
    schema_name: str = Field(default="default", description="Schema name", alias="schema")
    volume: str = Field(default="", description="Volume name for file storage")

    @property
    def volume_path(self) -> str:
        """Full volume path."""
        return f"/Volumes/{self.catalog}/{self.schema_name}/{self.volume}"


class BridgeSettings(BaseSettings):
    """Bridge service settings."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Service settings
    service_name: str = Field(default="sql-databricks-bridge")
    environment: Literal["development", "staging", "production"] = Field(default="development")
    debug: bool = Field(default=False)
    log_level: str = Field(default="INFO")

    # API settings
    api_host: str = Field(default="0.0.0.0")
    api_port: int = Field(default=8000)
    api_prefix: str = Field(default="/api/v1")

    # Polling settings
    polling_interval_seconds: int = Field(default=10, description="Interval for event polling")
    max_events_per_poll: int = Field(default=100, description="Max events per poll cycle")

    # Schedules file (YAML with cron-like task schedules; empty = bundled default)
    schedules_file: str = Field(default="", description="Override path to schedules YAML (empty = bundled default)")

    # Extraction settings
    extraction_chunk_size: int = Field(default=100_000, description="Rows per extraction chunk")
    max_retries: int = Field(default=3, description="Max retry attempts")
    retry_delay_seconds: int = Field(default=5, description="Initial retry delay")
    max_parallel_queries: int = Field(default=4, ge=1, le=20, description="Max parallel queries per extraction job")
    query_row_limit: int = Field(default=0, ge=0, description="Row limit per query (0 = no limit, use TOP N)")
    lookback_months: int = Field(default=24, ge=1, le=120, description="Rolling lookback months for time-filtered queries")

    # Two-phase sync (Phase 1: extract without warehouse, Phase 2: write with warehouse)
    two_phase_sync: bool = Field(default=False, description="Enable two-phase sync to minimize warehouse usage")

    # Startup behaviour
    ensure_tables_on_startup: bool = Field(default=False, description="Run CREATE TABLE IF NOT EXISTS DDL on startup (wakes SQL Warehouse)")

    # Local persistence
    sqlite_db_path: str = Field(default=".bridge_data/jobs.db", description="Local SQLite database path")

    # Queries path (configurable via .env)
    queries_path: str = Field(default="queries", description="Path to directory containing SQL query files")

    # Stages & jobs table
    stages_file: str = Field(default="", description="Override path to stages YAML (empty = bundled default)")
    jobs_table: str = Field(default="bridge.events.trigger_jobs", description="Databricks Delta table for job history")
    events_table: str = Field(default="bridge.events.bridge_events", description="Databricks Delta table for sync events (EventPoller)")

    version_tags_table: str = Field(default="bridge.events.version_tags", description="Databricks Delta table for version tags")
    traceability_tags_table: str = Field(default="bridge.events.traceability_tags", description="Databricks Delta table for traceability tags")
    traceability_tag_tables_table: str = Field(default="bridge.events.traceability_tag_tables", description="Databricks Delta table for traceability tag tables")
    fingerprint_table: str = Field(default="`000-sql-databricks-bridge`.events.sync_fingerprints", description="Databricks Delta table for differential sync fingerprints")

    # Differential sync defaults (server-side, applied even if client doesn't request it)
    # Format: "table1:l1_col:l2_col,table2:l1_col:l2_col" e.g. "j_atoscompra_new:periodo:idproduto"
    diff_sync_tables: str = Field(default="", description="Tables that use differential sync by default (format: table:l1_col:l2_col,...)")

    # Checksum column subsets for wide tables (avoid CHECKSUM(*) which is slow on 100+ columns)
    # Format: "table1:col1+col2+col3,table2:col1+col2" e.g. "j_atoscompra_new:periodo+preco+quantidade+idproduto"
    diff_sync_checksum_columns: str = Field(default="", description="Column subsets for CHECKSUM in diff sync (format: table:col1+col2+...,...)")

    # Incremental sync for append-only tables (only download new rows above max key)
    # Format: "table1:key_col,table2:key_col" e.g. "nac_ato:idAto,hato_cabecalho:idhato_cabecalho"
    incremental_sync_tables: str = Field(default="", description="Append-only tables that use incremental sync (format: table:key_col,...)")

    # Entries (countries/servers) to hide from listing and reject on trigger.
    # Format: comma-separated names e.g. "KTCLSQL001,KTCLSQL002"
    disabled_entries: str = Field(default="", description="Entries to hide from /metadata/countries and reject on /trigger (comma-separated)")

    # Per-entry query exclusions. Hides matching queries from listing and
    # rejects them on /trigger. Format: "entry:query,entry:query"
    # e.g. "KTCLSQL005:loc_psdata_compras,KTCLSQL005:loc_psdata_procesado"
    disabled_queries: str = Field(default="", description="Queries to disable per entry (format: entry:query,entry:query)")

    # Simulador sync settings
    simulador_base_path: str = Field(default="", description="Network share base path for simulador ZIP files")
    simulador_countries: str = Field(default="AR:argentina,BO:bolivia,CE:cam,CL:chile,CO:colombia,EC:ecuador,MX:mexico,PE:peru", description="Country code to schema mapping for simulador sync")

    # Auth settings
    auth_enabled: bool = Field(default=True, description="Enable token authentication")
    permissions_file: str = Field(default="", description="Override path to permissions YAML (empty = bundled default)")
    permissions_hot_reload: bool = Field(default=True)

    # Azure AD settings
    azure_ad_tenant_id: str = Field(default="", description="Azure AD tenant ID")
    azure_ad_client_id: str = Field(default="", description="Azure AD app client ID (audience)")
    authorized_users_file: str = Field(default="", description="Override path to authorized users YAML (empty = bundled default)")

    # GitHub OAuth settings
    github_client_id: str = Field(default="", description="GitHub OAuth App client ID")
    github_client_secret: SecretStr = Field(default=SecretStr(""), description="GitHub OAuth App client secret")
    github_callback_url: str = Field(
        default="http://localhost:8000/auth/github/callback",
        description="GitHub OAuth callback URL (must match GitHub App registration)",
    )
    frontend_url: str = Field(
        default="http://localhost:5173",
        description="Frontend URL — where the callback redirects after issuing the JWT",
    )
    jwt_secret: SecretStr = Field(
        default=SecretStr("dev-secret-change-in-production"),
        description="Secret for signing/verifying GitHub app JWTs",
    )
    jwt_expiry_hours: int = Field(default=8, description="GitHub JWT expiry in hours")
    admin_emails: str = Field(
        default="",
        description="Comma-separated admin email whitelist. Empty = any GitHub user gets admin.",
    )

    # E2E testing
    skip_sync_data: bool = Field(default=False, description="Skip SQL Server sync, jump to Databricks steps (e2e testing)")
    mock_data_availability: bool = Field(default=False, description="Mock data availability (all countries show elegibilidad+pesaje=true). For testing without SQL Server.")

    # Calibration job prefix (matches DAB target prefix, e.g. '[dev] ' for development)
    calibration_job_prefix: str = Field(default="[dev] ", description="Prefix prepended to calibration job names when searching Databricks. Set to '' for production.")

    # CORS settings
    cors_allowed_origins: str = Field(
        default="",
        description="Comma-separated allowed origins (empty = use default based on environment)",
    )

    # Sub-settings
    sql_server: SQLServerSettings = Field(default_factory=SQLServerSettings)
    databricks: DatabricksSettings = Field(default_factory=DatabricksSettings)


@lru_cache
def get_settings() -> BridgeSettings:
    """Get cached settings instance."""
    return BridgeSettings()
