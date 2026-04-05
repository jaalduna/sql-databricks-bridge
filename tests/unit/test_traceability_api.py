"""Unit tests for traceability API endpoints."""

from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from sql_databricks_bridge.auth.authorized_users import AuthorizedUser


# --- Fixtures ---


@pytest.fixture
def admin_user():
    return AuthorizedUser(
        email="admin@test.com",
        name="Admin",
        roles=["admin"],
        countries=["*"],
    )


def _make_client(user: AuthorizedUser):
    from sql_databricks_bridge.api.dependencies import get_current_azure_ad_user

    with patch("sql_databricks_bridge.main._event_poller", None):
        from sql_databricks_bridge.main import app

        app.dependency_overrides[get_current_azure_ad_user] = lambda: user
        client = TestClient(app)
        yield client
        app.dependency_overrides.pop(get_current_azure_ad_user, None)


@pytest.fixture
def admin_client(admin_user):
    yield from _make_client(admin_user)


SAMPLE_TAG_ROW = {
    "tag_id": "abc-123",
    "run_id": "run-001",
    "country": "chile",
    "period": 202501,
    "stage": "inicio-precios",
    "triggered_by": "user@test.com",
    "started_at": "2025-01-15T08:00:00",
    "completed_at": "2025-01-15T08:05:00",
    "status": "completed",
    "tables_count": 1,
    "created_at": "2025-01-15T08:00:00",
}

SAMPLE_TABLE_ROW = {
    "tag_id": "abc-123",
    "table_name": "jatoscompra_new",
    "full_name": "hive_metastore.chile_202501.jatoscompra_new",
    "catalog": "hive_metastore",
    "schema_name": "chile_202501",
    "row_count": 150000,
    "size_bytes": None,
}


class TestListTags:
    def test_returns_200(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [[{"cnt": 0}], []]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags")

        assert response.status_code == 200

    def test_response_shape(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [
            [{"cnt": 1}],
            [SAMPLE_TAG_ROW],
        ]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags")

        data = response.json()
        assert data["total"] == 1
        assert data["limit"] == 50
        assert data["offset"] == 0
        assert len(data["items"]) == 1
        assert data["items"][0]["tag_id"] == "abc-123"
        assert data["items"][0]["country"] == "chile"

    def test_passes_country_filter(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [[{"cnt": 0}], []]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            admin_client.get("/api/v1/traceability/tags?country=chile")

        count_sql = mock_db.execute_sql.call_args_list[0][0][0]
        assert "chile" in count_sql

    def test_pagination_params(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [[{"cnt": 100}], []]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags?limit=10&offset=20")

        data = response.json()
        assert data["limit"] == 10
        assert data["offset"] == 20


class TestGetTag:
    def test_returns_200_when_found(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [
            [SAMPLE_TAG_ROW],
            [SAMPLE_TABLE_ROW],
        ]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags/abc-123")

        assert response.status_code == 200
        data = response.json()
        assert data["tag_id"] == "abc-123"
        assert len(data["tables"]) == 1
        assert data["tables"][0]["table_name"] == "jatoscompra_new"

    def test_returns_404_when_not_found(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = []

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags/missing-id")

        assert response.status_code == 404

    def test_tables_included_in_response(self, admin_client):
        mock_db = MagicMock()
        table_row2 = {**SAMPLE_TABLE_ROW, "table_name": "jatoscompra_old", "full_name": "hive_metastore.chile_202501.jatoscompra_old"}
        mock_db.execute_sql.side_effect = [
            [SAMPLE_TAG_ROW],
            [SAMPLE_TABLE_ROW, table_row2],
        ]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags/abc-123")

        data = response.json()
        assert len(data["tables"]) == 2


class TestDownloadTableCsv:
    def test_returns_404_when_tag_not_found(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = []

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags/bad-id/tables/some_table/download")

        assert response.status_code == 404

    def test_returns_404_when_table_not_in_tag(self, admin_client):
        mock_db = MagicMock()
        # tag found, but no tables match
        mock_db.execute_sql.side_effect = [
            [SAMPLE_TAG_ROW],
            [],  # no tables
        ]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags/abc-123/tables/nonexistent/download")

        assert response.status_code == 404

    def test_returns_csv_content(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.side_effect = [
            [SAMPLE_TAG_ROW],
            [SAMPLE_TABLE_ROW],
            [{"col1": "a", "col2": 1}, {"col1": "b", "col2": 2}],
        ]

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.get("/api/v1/traceability/tags/abc-123/tables/jatoscompra_new/download")

        assert response.status_code == 200
        assert "text/csv" in response.headers["content-type"]
        assert "jatoscompra_new.csv" in response.headers["content-disposition"]


class TestDownloadAllZip:
    def test_returns_501(self, admin_client):
        response = admin_client.get("/api/v1/traceability/tags/abc-123/download")
        assert response.status_code == 501


class TestCreateTag:
    def test_creates_tag_returns_201(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = []

        payload = {
            "run_id": "run-001",
            "country": "chile",
            "period": 202501,
            "stage": "inicio-precios",
            "triggered_by": "user@test.com",
            "started_at": "2025-01-15T08:00:00",
            "status": "completed",
            "tables": [
                {
                    "table_name": "jatoscompra_new",
                    "full_name": "hive_metastore.chile_202501.jatoscompra_new",
                    "catalog": "hive_metastore",
                    "schema_name": "chile_202501",
                    "row_count": 150000,
                }
            ],
        }

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            response = admin_client.post("/api/v1/traceability/tags", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["country"] == "chile"
        assert data["stage"] == "inicio-precios"
        assert data["tables_count"] == 1
        assert "tag_id" in data

    def test_create_tag_calls_insert(self, admin_client):
        mock_db = MagicMock()
        mock_db.execute_sql.return_value = []

        payload = {
            "run_id": "run-002",
            "country": "mexico",
            "period": 202502,
            "stage": "pesaje",
            "triggered_by": "ops@test.com",
            "started_at": "2025-02-01T10:00:00",
            "status": "completed",
            "tables": [],
        }

        with patch("sql_databricks_bridge.api.routes.traceability._get_databricks_client", return_value=mock_db):
            admin_client.post("/api/v1/traceability/tags", json=payload)

        # At least one INSERT was called
        assert mock_db.execute_sql.call_count >= 1
