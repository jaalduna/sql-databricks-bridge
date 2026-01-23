"""Unit tests for query loader."""

import tempfile
from pathlib import Path

import pytest

from sql_databricks_bridge.core.query_loader import QueryLoader, QueryNotFoundError


@pytest.fixture
def queries_dir():
    """Create a temporary directory with sample SQL files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        queries_path = Path(tmpdir)

        # Create sample SQL files
        (queries_path / "users.sql").write_text(
            "SELECT * FROM {schema}.users WHERE country = '{country}'"
        )
        (queries_path / "orders.sql").write_text(
            "SELECT o.*, u.name FROM {schema}.orders o "
            "JOIN {schema}.users u ON o.user_id = u.id "
            "WHERE o.created_at > '{start_date}'"
        )
        (queries_path / "simple.sql").write_text("SELECT 1")

        yield queries_path


class TestQueryLoader:
    """Tests for QueryLoader class."""

    def test_discover_queries(self, queries_dir):
        """Test that all SQL files are discovered."""
        loader = QueryLoader(queries_dir)
        queries = loader.discover_queries()

        assert len(queries) == 3
        assert "users" in queries
        assert "orders" in queries
        assert "simple" in queries

    def test_discover_queries_nonexistent_path(self):
        """Test error when path doesn't exist."""
        loader = QueryLoader("/nonexistent/path")

        with pytest.raises(FileNotFoundError):
            loader.discover_queries()

    def test_get_query(self, queries_dir):
        """Test getting a specific query."""
        loader = QueryLoader(queries_dir)

        query = loader.get_query("simple")
        assert query == "SELECT 1"

    def test_get_query_not_found(self, queries_dir):
        """Test error when query doesn't exist."""
        loader = QueryLoader(queries_dir)

        with pytest.raises(QueryNotFoundError):
            loader.get_query("nonexistent")

    def test_list_queries(self, queries_dir):
        """Test listing all query names."""
        loader = QueryLoader(queries_dir)
        names = loader.list_queries()

        assert set(names) == {"users", "orders", "simple"}

    def test_get_query_parameters(self, queries_dir):
        """Test extracting parameter placeholders."""
        loader = QueryLoader(queries_dir)

        params = loader.get_query_parameters(loader.get_query("users"))
        assert set(params) == {"schema", "country"}

        params = loader.get_query_parameters(loader.get_query("orders"))
        assert set(params) == {"schema", "start_date"}

        params = loader.get_query_parameters(loader.get_query("simple"))
        assert params == []

    def test_format_query(self, queries_dir):
        """Test formatting query with parameters."""
        loader = QueryLoader(queries_dir)

        formatted = loader.format_query(
            "users",
            {"schema": "dbo", "country": "Colombia"},
        )
        assert formatted == "SELECT * FROM dbo.users WHERE country = 'Colombia'"

    def test_format_query_missing_params(self, queries_dir):
        """Test error when required parameter is missing."""
        loader = QueryLoader(queries_dir)

        with pytest.raises(KeyError) as exc_info:
            loader.format_query("users", {"schema": "dbo"})

        assert "country" in str(exc_info.value)

    def test_reload(self, queries_dir):
        """Test reloading queries from disk."""
        loader = QueryLoader(queries_dir)
        loader.discover_queries()

        # Modify a file
        (queries_dir / "simple.sql").write_text("SELECT 2")

        # Before reload, still has old content
        assert loader.get_query("simple") == "SELECT 1"

        # After reload, has new content
        loader.reload()
        assert loader.get_query("simple") == "SELECT 2"


class TestQueryParameterExtraction:
    """Tests for parameter extraction edge cases."""

    def test_duplicate_parameters(self, queries_dir):
        """Test that duplicate parameters are deduplicated."""
        loader = QueryLoader(queries_dir)

        query = "SELECT * FROM {schema}.a, {schema}.b WHERE x = '{schema}'"
        params = loader.get_query_parameters(query)

        assert params == ["schema"]

    def test_escaped_braces(self, queries_dir):
        """Test that escaped braces are not treated as parameters."""
        loader = QueryLoader(queries_dir)

        query = "SELECT '{{literal}}' FROM {table}"
        params = loader.get_query_parameters(query)

        assert params == ["table"]

    def test_underscore_in_parameter(self, queries_dir):
        """Test parameters with underscores."""
        loader = QueryLoader(queries_dir)

        query = "SELECT * FROM {schema_name}.{table_name}"
        params = loader.get_query_parameters(query)

        assert set(params) == {"schema_name", "table_name"}
