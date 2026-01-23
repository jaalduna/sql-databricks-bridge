"""Configuration management using pydantic-settings."""

from functools import lru_cache
from typing import Literal

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class SQLServerSettings(BaseSettings):
    """SQL Server connection settings."""

    model_config = SettingsConfigDict(env_prefix="SQLSERVER_")

    host: str = Field(default="localhost", description="SQL Server hostname")
    port: int = Field(default=1433, description="SQL Server port")
    database: str = Field(default="master", description="Database name")
    username: str = Field(default="sa", description="Username")
    password: SecretStr = Field(default=SecretStr(""), description="Password")
    driver: str = Field(
        default="ODBC Driver 18 for SQL Server",
        description="ODBC driver name",
    )
    trust_server_certificate: bool = Field(
        default=True,
        description="Trust server certificate",
    )

    @property
    def connection_string(self) -> str:
        """Build ODBC connection string."""
        trust_cert = "yes" if self.trust_server_certificate else "no"
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
        return (
            f"mssql+pyodbc://{self.username}:{self.password.get_secret_value()}"
            f"@{self.host}:{self.port}/{self.database}"
            f"?driver={driver_encoded}&TrustServerCertificate={trust_cert}"
        )


class DatabricksSettings(BaseSettings):
    """Databricks connection settings."""

    model_config = SettingsConfigDict(env_prefix="DATABRICKS_")

    host: str = Field(default="", description="Databricks workspace URL")
    token: SecretStr = Field(default=SecretStr(""), description="Personal access token")
    client_id: str | None = Field(default=None, description="OAuth client ID (Service Principal)")
    client_secret: SecretStr | None = Field(
        default=None,
        description="OAuth client secret",
    )
    warehouse_id: str = Field(default="", description="SQL Warehouse ID for statement execution")
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

    # Extraction settings
    extraction_chunk_size: int = Field(default=100_000, description="Rows per extraction chunk")
    max_retries: int = Field(default=3, description="Max retry attempts")
    retry_delay_seconds: int = Field(default=5, description="Initial retry delay")

    # Auth settings
    auth_enabled: bool = Field(default=True, description="Enable token authentication")
    permissions_file: str = Field(default="config/permissions.yaml")
    permissions_hot_reload: bool = Field(default=True)

    # Sub-settings
    sql_server: SQLServerSettings = Field(default_factory=SQLServerSettings)
    databricks: DatabricksSettings = Field(default_factory=DatabricksSettings)


@lru_cache
def get_settings() -> BridgeSettings:
    """Get cached settings instance."""
    return BridgeSettings()
