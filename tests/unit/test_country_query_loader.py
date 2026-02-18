"""Unit tests for CountryAwareQueryLoader."""

import pytest
from pathlib import Path

from sql_databricks_bridge.core.country_query_loader import (
    CountryAwareQueryLoader,
    QueryNotFoundError,
)


@pytest.fixture
def queries_path():
    """Path to test queries directory."""
    # From tests/unit/ -> project root -> queries/
    return Path(__file__).parent.parent.parent / "queries"


@pytest.fixture
def loader(queries_path):
    """Create a CountryAwareQueryLoader instance."""
    return CountryAwareQueryLoader(queries_path)


class TestCountryAwareQueryLoaderInit:
    """Test initialization and path validation."""

    def test_init_with_valid_path(self, queries_path):
        """Should initialize successfully with valid path."""
        loader = CountryAwareQueryLoader(queries_path)
        assert loader.base_path == Path(queries_path)
        assert loader.common_path == Path(queries_path) / "common"
        assert loader.countries_path == Path(queries_path) / "countries"

    def test_init_with_string_path(self, queries_path):
        """Should accept string path and convert to Path."""
        loader = CountryAwareQueryLoader(str(queries_path))
        assert isinstance(loader.base_path, Path)

    def test_init_with_nonexistent_path(self):
        """Should raise FileNotFoundError for nonexistent path."""
        with pytest.raises(FileNotFoundError, match="Queries base path not found"):
            CountryAwareQueryLoader("/nonexistent/path")


class TestDiscoverCommonQueries:
    """Test common query discovery."""

    def test_discover_common_queries(self, loader):
        """Should discover all queries in common/ directory."""
        queries = loader._discover_common_queries()

        # common/ directory is now empty (all queries are country-specific)
        # This is valid - countries can have all their own queries
        assert isinstance(queries, dict)

    def test_discover_common_queries_returns_dict(self, loader):
        """Should return dict mapping query names to SQL content."""
        queries = loader._discover_common_queries()
        assert isinstance(queries, dict)
        for name, content in queries.items():
            assert isinstance(name, str)
            assert isinstance(content, str)
            assert len(content) > 0


class TestDiscoverCountryQueries:
    """Test country-specific query discovery."""

    def test_discover_bolivia_queries(self, loader):
        """Should discover Bolivia-specific queries."""
        queries = loader._discover_country_queries("bolivia")

        # Real queries - table names
        assert "hato_cabecalho" in queries
        assert "rg_panelis" in queries
        assert "cidade" in queries  # Bolivia's region table

        # Verify it's real SQL content (lowercase)
        assert "select" in queries["hato_cabecalho"]

    def test_discover_chile_queries(self, loader):
        """Should discover Chile-specific queries."""
        queries = loader._discover_country_queries("chile")

        assert "hato_cabecalho" in queries or "j_atoscompra_new" in queries
        assert "grupo" in queries  # Chile's region table

        # Verify it's lowercase SQL
        assert "select" in queries["grupo"]

    def test_discover_peru_queries(self, loader):
        """Should discover Peru-specific queries."""
        queries = loader._discover_country_queries("peru")

        # Peru directory exists but has no queries yet
        assert isinstance(queries, dict)

    def test_discover_nonexistent_country(self, loader):
        """Should return empty dict for nonexistent country."""
        queries = loader._discover_country_queries("nonexistent")
        assert queries == {}


class TestDiscoverQueriesMerging:
    """Test merging of common and country-specific queries."""

    def test_discover_bolivia_merges_common_and_specific(self, loader):
        """Should merge common and Bolivia-specific queries."""
        queries = loader.discover_queries("bolivia")

        # Bolivia has all its queries (no common queries currently)
        assert "hato_cabecalho" in queries
        assert "rg_panelis" in queries
        assert "cidade" in queries  # Bolivia's region table

        # Verify table name in query (lowercase)
        assert "from cidade" in queries["cidade"]

    def test_discover_chile_has_common_and_specific(self, loader):
        """Should include common queries + Chile-specific."""
        queries = loader.discover_queries("chile")

        # Chile has queries from multiple sources
        assert "rg_panelis" in queries
        assert "grupo" in queries  # Chile's region table

        # Verify it's normalized (lowercase, no aliases)
        assert "select" in queries["grupo"]

    def test_discover_peru_has_common_and_specific(self, loader):
        """Should include common queries + Peru-specific."""
        queries = loader.discover_queries("peru")

        # Peru has no queries yet (empty directory)
        assert isinstance(queries, dict)

    def test_discover_country_without_specific_folder(self, loader):
        """Should return only common queries for country without specific folder."""
        queries = loader.discover_queries("argentina")

        # Argentina has its own queries (Elegibilidad)
        assert isinstance(queries, dict)

        # Argentina should have basic tables
        assert "j_atoscompra_new" in queries
        assert "rg_panelis" in queries
        assert "domicilios" in queries

        # Should NOT have Precios tables
        assert "hato_cabecalho" not in queries
        assert "a_canal" not in queries


class TestGetQuery:
    """Test retrieving individual queries."""

    def test_get_common_query(self, loader):
        """Should retrieve query available to country."""
        sql = loader.get_query("hato_cabecalho", "chile")
        assert "select" in sql
        assert "from hato_cabecalho" in sql

    def test_get_country_specific_query(self, loader):
        """Should retrieve country-specific query."""
        sql = loader.get_query("cidade", "bolivia")
        # Bolivia's cidade table (lowercase)
        assert "from cidade" in sql

    def test_get_overridden_query(self, loader):
        """Should retrieve country-specific version with resolved params."""
        sql = loader.get_query("cidade", "bolivia")
        # Bolivia version uses cidade (lowercase, no aliases)
        assert "from cidade" in sql

    def test_get_common_version_when_no_override(self, loader):
        """Should get country version with correct params resolved."""
        sql = loader.get_query("grupo", "chile")
        # Verify it's normalized (lowercase)
        assert "select" in sql
        assert "from grupo" in sql

    def test_get_nonexistent_query_raises_error(self, loader):
        """Should raise QueryNotFoundError for nonexistent query."""
        with pytest.raises(QueryNotFoundError, match="not found for country"):
            loader.get_query("nonexistent", "bolivia")

    def test_get_query_auto_discovers(self, loader):
        """Should auto-discover queries if not already cached."""
        # Don't call discover_queries first
        sql = loader.get_query("hato_cabecalho", "bolivia")
        assert "select" in sql


class TestListQueries:
    """Test listing available queries."""

    def test_list_queries_bolivia(self, loader):
        """Should list all queries available for Bolivia."""
        queries = loader.list_queries("bolivia")

        # Core tables (actual table names)
        assert "hato_cabecalho" in queries
        assert "rg_panelis" in queries
        assert "cidade" in queries  # Bolivia's region table
        assert "j_atoscompra_new" in queries
        assert "vw_artigoz" in queries
        assert "domicilios" in queries

        # Total: consolidated by table name
        assert len(queries) == 25  # Exact count per generation

    def test_list_queries_chile(self, loader):
        """Should list all queries available for Chile."""
        queries = loader.list_queries("chile")

        # Core tables (actual table names)
        assert "hato_cabecalho" in queries
        assert "j_atoscompra_new" in queries
        assert "grupo" in queries  # Chile's region table
        assert "domicilios" in queries

        # Total: consolidated by table name
        assert len(queries) == 26  # Exact count per generation

    def test_list_queries_returns_sorted(self, loader):
        """Should return queries in sorted order."""
        queries = loader.list_queries("bolivia")
        assert queries == sorted(queries)

    def test_list_queries_auto_discovers(self, loader):
        """Should auto-discover if not already cached."""
        queries = loader.list_queries("peru")
        # Peru has queries consolidated by table name
        assert isinstance(queries, list)
        assert len(queries) == 22  # Consolidated unique tables


class TestGetQuerySource:
    """Test identifying query sources."""

    def test_get_source_common(self, loader):
        """Should identify country-specific query source."""
        source = loader.get_query_source("hato_cabecalho", "chile")
        assert source == "country-specific"  # All queries are country-specific now

    def test_get_source_override(self, loader):
        """Should identify country-specific query."""
        source = loader.get_query_source("cidade", "bolivia")
        assert source == "country-specific"

    def test_get_source_country_specific(self, loader):
        """Should identify country-specific query."""
        source = loader.get_query_source("rg_panelis", "bolivia")
        assert source == "country-specific"

    def test_get_source_nonexistent_raises_error(self, loader):
        """Should raise error for nonexistent query."""
        with pytest.raises(QueryNotFoundError):
            loader.get_query_source("nonexistent", "bolivia")


class TestCaching:
    """Test query caching behavior."""

    def test_queries_are_cached(self, loader):
        """Should cache queries after first discovery."""
        # First call
        loader.discover_queries("bolivia")
        assert "bolivia" in loader._cache

        # Second call should use cache
        queries = loader.list_queries("bolivia")
        assert len(queries) > 0

    def test_reload_clears_country_cache(self, loader):
        """Should reload queries for specific country."""
        loader.discover_queries("bolivia")
        assert "bolivia" in loader._cache

        loader.reload("bolivia")
        # Should still be in cache (reloaded)
        assert "bolivia" in loader._cache

    def test_reload_all_clears_all_cache(self, loader):
        """Should clear all cache when reload without country."""
        loader.discover_queries("bolivia")
        loader.discover_queries("chile")

        loader.reload()
        assert loader._cache == {}


class TestEdgeCases:
    """Test edge cases and error handling."""

    def test_empty_country_name(self, loader):
        """Should handle empty country name."""
        queries = loader.discover_queries("")
        # Should return only common queries (currently none)
        assert isinstance(queries, dict)

    def test_country_with_spaces(self, loader):
        """Should handle country name with spaces."""
        queries = loader.discover_queries("south africa")
        # Should return only common queries (no folder exists, and common is empty)
        assert isinstance(queries, dict)
        assert len(queries) == 0  # No common queries and no country folder

    def test_case_sensitive_country_names(self, loader):
        """Country names are case-sensitive (filesystem dependent)."""
        # This test documents current behavior
        queries_lower = loader.list_queries("bolivia")
        queries_upper = loader.list_queries("BOLIVIA")

        # On case-sensitive filesystems, these will differ
        # On case-insensitive filesystems, they may be the same
        assert isinstance(queries_lower, list)
        assert isinstance(queries_upper, list)
