"""Unit tests for database clients."""

from unittest.mock import MagicMock, patch, PropertyMock

import polars as pl
import pytest


class TestSQLServerClient:
    """Tests for SQLServerClient."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock SQL Server settings."""
        settings = MagicMock()
        settings.host = "localhost"
        settings.port = 1433
        settings.database = "testdb"
        settings.username = "sa"
        settings.password = MagicMock()
        settings.password.get_secret_value.return_value = "password"
        settings.driver = "ODBC Driver 18 for SQL Server"
        settings.trust_server_certificate = True
        settings.sqlalchemy_url = "mssql+pyodbc://sa:password@localhost:1433/testdb"
        return settings

    @pytest.fixture
    def client(self, mock_settings):
        """Create SQLServerClient with mocked settings."""
        with patch(
            "sql_databricks_bridge.db.sql_server.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value.sql_server = mock_settings

            from sql_databricks_bridge.db.sql_server import SQLServerClient

            client = SQLServerClient(settings=mock_settings)
            return client

    def test_init_with_settings(self, mock_settings):
        """Initialize with provided settings."""
        from sql_databricks_bridge.db.sql_server import SQLServerClient

        client = SQLServerClient(settings=mock_settings)

        assert client.settings is mock_settings

    def test_engine_created_lazily(self, client):
        """Engine is created on first access."""
        assert client._engine is None

        with patch("sql_databricks_bridge.db.sql_server.create_engine") as mock_create:
            mock_create.return_value = MagicMock()
            _ = client.engine

        mock_create.assert_called_once()

    def test_engine_cached(self, client):
        """Engine is cached after creation."""
        with patch("sql_databricks_bridge.db.sql_server.create_engine") as mock_create:
            mock_engine = MagicMock()
            mock_create.return_value = mock_engine

            engine1 = client.engine
            engine2 = client.engine

        assert engine1 is engine2
        mock_create.assert_called_once()

    def test_test_connection_success(self, client):
        """test_connection returns True on success."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(type(client), "engine", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_engine
            result = client.test_connection()

        assert result is True

    def test_test_connection_failure(self, client):
        """test_connection returns False on failure."""
        from sqlalchemy.exc import SQLAlchemyError

        mock_engine = MagicMock()
        mock_engine.connect.side_effect = SQLAlchemyError("Connection failed")

        with patch.object(type(client), "engine", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_engine
            result = client.test_connection()

        assert result is False

    def test_execute_query_returns_dataframe(self, client):
        """execute_query returns Polars DataFrame."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = [(1, "Alice"), (2, "Bob")]
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(type(client), "engine", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_engine
            result = client.execute_query("SELECT * FROM users")

        assert isinstance(result, pl.DataFrame)
        assert len(result) == 2
        assert result.columns == ["id", "name"]

    def test_execute_query_empty_result(self, client):
        """execute_query returns empty DataFrame for no results."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.keys.return_value = ["id", "name"]
        mock_result.fetchall.return_value = []
        mock_conn.execute.return_value = mock_result
        mock_engine.connect.return_value.__enter__.return_value = mock_conn

        with patch.object(type(client), "engine", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_engine
            result = client.execute_query("SELECT * FROM users WHERE 1=0")

        assert isinstance(result, pl.DataFrame)
        assert result.is_empty()

    def test_execute_write_returns_rowcount(self, client):
        """execute_write returns affected row count."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_result = MagicMock()
        mock_result.rowcount = 5
        mock_conn.execute.return_value = mock_result
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        with patch.object(type(client), "engine", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_engine
            result = client.execute_write("UPDATE users SET active = 1")

        assert result == 5

    def test_bulk_insert_returns_count(self, client):
        """bulk_insert returns inserted row count."""
        mock_engine = MagicMock()
        mock_conn = MagicMock()
        mock_engine.begin.return_value.__enter__.return_value = mock_conn

        df = pl.DataFrame({"id": [1, 2, 3], "name": ["a", "b", "c"]})

        with patch.object(type(client), "engine", new_callable=PropertyMock) as mock_prop:
            mock_prop.return_value = mock_engine
            result = client.bulk_insert("users", df)

        assert result == 3

    def test_bulk_insert_empty_dataframe(self, client):
        """bulk_insert returns 0 for empty DataFrame."""
        df = pl.DataFrame({"id": [], "name": []})

        result = client.bulk_insert("users", df)

        assert result == 0

    def test_close_disposes_engine(self, client):
        """close disposes the engine."""
        mock_engine = MagicMock()
        client._engine = mock_engine

        client.close()

        mock_engine.dispose.assert_called_once()
        assert client._engine is None


class TestDatabricksClient:
    """Tests for DatabricksClient."""

    @pytest.fixture
    def mock_settings(self):
        """Create mock Databricks settings."""
        settings = MagicMock()
        settings.host = "https://test.azuredatabricks.net"
        settings.token = MagicMock()
        settings.token.get_secret_value.return_value = "dapi123"
        settings.client_id = None
        settings.client_secret = None
        settings.warehouse_id = "abc123"
        settings.catalog = "main"
        settings.schema_name = "default"
        settings.volume = "test_volume"
        return settings

    @pytest.fixture
    def client(self, mock_settings):
        """Create DatabricksClient with mocked settings."""
        with patch(
            "sql_databricks_bridge.db.databricks.get_settings"
        ) as mock_get_settings:
            mock_get_settings.return_value.databricks = mock_settings

            from sql_databricks_bridge.db.databricks import DatabricksClient

            client = DatabricksClient(settings=mock_settings)
            return client

    def test_init_with_settings(self, mock_settings):
        """Initialize with provided settings."""
        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient(settings=mock_settings)

        assert client.settings is mock_settings

    def test_client_created_with_token(self, client, mock_settings):
        """WorkspaceClient is created with token auth."""
        with patch("sql_databricks_bridge.db.databricks.WorkspaceClient") as mock_ws:
            mock_ws.return_value = MagicMock()
            _ = client.client

        mock_ws.assert_called_once()
        call_kwargs = mock_ws.call_args[1]
        assert call_kwargs["host"] == mock_settings.host
        assert call_kwargs["token"] == "dapi123"

    def test_client_created_with_service_principal(self, mock_settings):
        """WorkspaceClient is created with SP auth."""
        mock_settings.client_id = "sp-client-id"
        mock_settings.client_secret = MagicMock()
        mock_settings.client_secret.get_secret_value.return_value = "sp-secret"

        from sql_databricks_bridge.db.databricks import DatabricksClient

        client = DatabricksClient(settings=mock_settings)

        with patch("sql_databricks_bridge.db.databricks.WorkspaceClient") as mock_ws:
            mock_ws.return_value = MagicMock()
            _ = client.client

        mock_ws.assert_called_once()
        call_kwargs = mock_ws.call_args[1]
        assert call_kwargs["client_id"] == "sp-client-id"

    def test_test_connection_success(self, client):
        """test_connection returns True on success."""
        mock_workspace = MagicMock()
        client._client = mock_workspace

        result = client.test_connection()

        assert result is True
        mock_workspace.current_user.me.assert_called_once()

    def test_test_connection_failure(self, client):
        """test_connection returns False on failure."""
        mock_workspace = MagicMock()
        mock_workspace.current_user.me.side_effect = Exception("Auth failed")
        client._client = mock_workspace

        result = client.test_connection()

        assert result is False

    def test_execute_sql_returns_results(self, client, mock_settings):
        """execute_sql returns query results."""
        mock_workspace = MagicMock()
        mock_statement = MagicMock()
        mock_statement.result.data_array = [["1", "Alice"], ["2", "Bob"]]

        # Create proper column mocks with name attribute
        col1 = MagicMock()
        col1.name = "id"
        col2 = MagicMock()
        col2.name = "name"
        mock_statement.manifest.schema.columns = [col1, col2]

        mock_workspace.statement_execution.execute_statement.return_value = mock_statement
        client._client = mock_workspace

        results = client.execute_sql("SELECT * FROM users")

        assert len(results) == 2
        assert results[0] == {"id": "1", "name": "Alice"}

    def test_execute_sql_empty_results(self, client, mock_settings):
        """execute_sql returns empty list for no results."""
        mock_workspace = MagicMock()
        mock_statement = MagicMock()
        mock_statement.result = None
        mock_workspace.statement_execution.execute_statement.return_value = mock_statement
        client._client = mock_workspace

        results = client.execute_sql("SELECT * FROM users WHERE 1=0")

        assert results == []

    def test_upload_dataframe(self, client):
        """upload_dataframe uploads parquet to volume."""
        mock_workspace = MagicMock()
        client._client = mock_workspace

        df = pl.DataFrame({"id": [1, 2], "name": ["a", "b"]})

        result = client.upload_dataframe(df, "/Volumes/test/path/file.parquet")

        assert result == "/Volumes/test/path/file.parquet"
        mock_workspace.files.upload.assert_called_once()

    def test_delete_file(self, client):
        """delete_file removes file from volume."""
        mock_workspace = MagicMock()
        client._client = mock_workspace

        client.delete_file("/Volumes/test/path/file.parquet")

        mock_workspace.files.delete.assert_called_once_with("/Volumes/test/path/file.parquet")
