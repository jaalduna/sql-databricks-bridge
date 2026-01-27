"""SQL query file discovery and loading."""

import logging
import re
from pathlib import Path

logger = logging.getLogger(__name__)


class QueryNotFoundError(Exception):
    """Raised when a requested query file is not found."""

    pass


class QueryLoader:
    """Discovers and loads SQL query files from a directory."""

    def __init__(self, queries_path: str | Path) -> None:
        """Initialize query loader.

        Args:
            queries_path: Path to directory containing .sql files.
        """
        self.queries_path = Path(queries_path)
        self._cache: dict[str, str] = {}
        self._loaded = False

    def discover_queries(self) -> dict[str, str]:
        """Discover all .sql files and files without extensions in the queries path.

        Returns:
            Dictionary mapping query names (file stems) to SQL content.

        Raises:
            FileNotFoundError: If queries_path doesn't exist.
        """
        if not self.queries_path.exists():
            raise FileNotFoundError(f"Queries path not found: {self.queries_path}")

        if not self.queries_path.is_dir():
            raise ValueError(f"Queries path is not a directory: {self.queries_path}")

        queries = {}

        # First, discover .sql files
        for sql_file in self.queries_path.glob("*.sql"):
            query_name = sql_file.stem
            query_content = sql_file.read_text(encoding="utf-8")
            queries[query_name] = query_content
            logger.debug(f"Discovered query: {query_name}")

        # Then, discover files without extensions (common in some projects)
        for file_path in self.queries_path.iterdir():
            if file_path.is_file() and not file_path.suffix:
                query_name = file_path.name
                # Skip if already discovered with .sql extension
                if query_name not in queries:
                    query_content = file_path.read_text(encoding="utf-8")
                    queries[query_name] = query_content
                    logger.debug(f"Discovered query (no extension): {query_name}")

        self._cache = queries
        self._loaded = True
        logger.info(f"Discovered {len(queries)} queries in {self.queries_path}")

        return queries

    def get_query(self, name: str) -> str:
        """Get a specific query by name.

        Args:
            name: Query name (without .sql extension).

        Returns:
            SQL query content.

        Raises:
            QueryNotFoundError: If query doesn't exist.
        """
        if not self._loaded:
            self.discover_queries()

        if name not in self._cache:
            raise QueryNotFoundError(f"Query not found: {name}")

        return self._cache[name]

    def list_queries(self) -> list[str]:
        """List all available query names.

        Returns:
            List of query names.
        """
        if not self._loaded:
            self.discover_queries()

        return list(self._cache.keys())

    def get_query_parameters(self, query: str) -> list[str]:
        """Extract parameter placeholders from a query.

        Finds all {parameter} placeholders in the query.

        Args:
            query: SQL query content.

        Returns:
            List of parameter names found in the query.
        """
        # Match {parameter} but not {{escaped}}
        pattern = r"(?<!\{)\{([a-zA-Z_][a-zA-Z0-9_]*)\}(?!\})"
        matches = re.findall(pattern, query)
        return list(set(matches))

    def format_query(self, name: str, params: dict[str, str]) -> str:
        """Get and format a query with parameters.

        Args:
            name: Query name.
            params: Parameter values to substitute.

        Returns:
            Formatted SQL query.

        Raises:
            QueryNotFoundError: If query doesn't exist.
            KeyError: If a required parameter is missing.
        """
        query = self.get_query(name)
        required_params = self.get_query_parameters(query)

        # Check for missing parameters
        missing = [p for p in required_params if p not in params]
        if missing:
            raise KeyError(f"Missing parameters for query '{name}': {missing}")

        # Format query with parameters
        formatted = query
        for param, value in params.items():
            formatted = formatted.replace(f"{{{param}}}", str(value))

        return formatted

    def reload(self) -> None:
        """Force reload of all queries from disk."""
        self._cache.clear()
        self._loaded = False
        self.discover_queries()
