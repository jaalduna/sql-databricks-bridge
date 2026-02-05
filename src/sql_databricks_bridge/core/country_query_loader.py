"""Country-aware SQL query loader with override support."""

import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class QueryNotFoundError(Exception):
    """Raised when a requested query file is not found."""

    pass


class CountryAwareQueryLoader:
    """Loads SQL queries with country-specific override support.

    Directory structure:
        queries/
        ├── common/              # Shared queries for all countries
        │   ├── customers.sql
        │   └── products.sql
        └── countries/           # Country-specific queries & overrides
            ├── bolivia/
            │   ├── stores.sql   # Bolivia-only
            │   └── customers.sql # Overrides common/customers.sql
            └── chile/
                └── tax.sql

    Resolution strategy:
        1. Check countries/{country}/{query}.sql (country-specific)
        2. Fallback to common/{query}.sql (shared)
        3. Raise QueryNotFoundError if neither exists
    """

    def __init__(self, base_path: str | Path) -> None:
        """Initialize country-aware query loader.

        Args:
            base_path: Root path containing 'common/' and 'countries/' directories.

        Raises:
            FileNotFoundError: If base_path doesn't exist.
        """
        self.base_path = Path(base_path)
        if not self.base_path.exists():
            raise FileNotFoundError(f"Queries base path not found: {self.base_path}")

        self.common_path = self.base_path / "common"
        self.countries_path = self.base_path / "countries"

        # Cache: {country: {query_name: (sql_content, source_type)}}
        self._cache: dict[str, dict[str, tuple[str, str]]] = {}

    def _discover_common_queries(self) -> dict[str, str]:
        """Discover all queries in common/ directory.

        Returns:
            Dictionary mapping query names to SQL content.
        """
        queries = {}

        if not self.common_path.exists():
            logger.warning(f"Common queries path not found: {self.common_path}")
            return queries

        for sql_file in self.common_path.glob("*.sql"):
            query_name = sql_file.stem
            query_content = sql_file.read_text(encoding="utf-8")
            queries[query_name] = query_content
            logger.debug(f"Discovered common query: {query_name}")

        logger.info(f"Discovered {len(queries)} common queries")
        return queries

    def _discover_country_queries(self, country: str) -> dict[str, str]:
        """Discover country-specific queries.

        Args:
            country: Country name (e.g., 'bolivia', 'chile').

        Returns:
            Dictionary mapping query names to SQL content.
        """
        queries = {}
        country_path = self.countries_path / country

        if not country_path.exists():
            logger.debug(f"Country path not found: {country_path}")
            return queries

        if not country_path.is_dir():
            logger.warning(f"Country path is not a directory: {country_path}")
            return queries

        for sql_file in country_path.glob("*.sql"):
            query_name = sql_file.stem
            query_content = sql_file.read_text(encoding="utf-8")
            queries[query_name] = query_content
            logger.debug(f"Discovered {country} query: {query_name}")

        logger.info(f"Discovered {len(queries)} queries for {country}")
        return queries

    def discover_queries(self, country: str) -> dict[str, str]:
        """Discover all available queries for a country.

        Merges common queries with country-specific queries.
        Country-specific queries override common queries with the same name.

        Args:
            country: Country name (e.g., 'bolivia', 'chile').

        Returns:
            Dictionary mapping query names to SQL content.
        """
        # Load common queries
        common_queries = self._discover_common_queries()

        # Load country-specific queries
        country_queries = self._discover_country_queries(country)

        # Track sources for logging
        query_sources: dict[str, tuple[str, str]] = {}

        # Start with common queries
        for name, sql in common_queries.items():
            query_sources[name] = (sql, "common")

        # Override with country-specific queries
        for name, sql in country_queries.items():
            source_type = "override" if name in common_queries else "country-specific"
            query_sources[name] = (sql, source_type)

            if source_type == "override":
                logger.info(f"Query '{name}' overridden by {country}-specific version")
            else:
                logger.info(f"Query '{name}' is {country}-specific")

        # Cache for this country
        self._cache[country] = query_sources

        # Return just the SQL content
        return {name: sql for name, (sql, _) in query_sources.items()}

    def get_query(self, name: str, country: str) -> str:
        """Get a specific query for a country.

        Args:
            name: Query name (without .sql extension).
            country: Country name.

        Returns:
            SQL query content.

        Raises:
            QueryNotFoundError: If query doesn't exist for this country.
        """
        # Ensure country queries are discovered
        if country not in self._cache:
            self.discover_queries(country)

        if name not in self._cache[country]:
            raise QueryNotFoundError(
                f"Query '{name}' not found for country '{country}'. "
                f"Available queries: {list(self._cache[country].keys())}"
            )

        sql_content, source_type = self._cache[country][name]
        logger.debug(f"Retrieved query '{name}' for {country} (source: {source_type})")
        return sql_content

    def list_queries(self, country: str) -> list[str]:
        """List all available query names for a country.

        Args:
            country: Country name.

        Returns:
            Sorted list of query names.
        """
        if country not in self._cache:
            self.discover_queries(country)

        return sorted(self._cache[country].keys())

    def get_query_source(self, name: str, country: str) -> str:
        """Get the source type of a query.

        Args:
            name: Query name.
            country: Country name.

        Returns:
            Source type: 'common', 'override', or 'country-specific'.

        Raises:
            QueryNotFoundError: If query doesn't exist for this country.
        """
        if country not in self._cache:
            self.discover_queries(country)

        if name not in self._cache[country]:
            raise QueryNotFoundError(f"Query '{name}' not found for country '{country}'")

        _, source_type = self._cache[country][name]
        return source_type

    def reload(self, country: str | None = None) -> None:
        """Reload queries from disk.

        Args:
            country: Specific country to reload (None = reload all cached countries).
        """
        if country:
            if country in self._cache:
                del self._cache[country]
            self.discover_queries(country)
            logger.info(f"Reloaded queries for {country}")
        else:
            self._cache.clear()
            logger.info("Cleared all query cache")
