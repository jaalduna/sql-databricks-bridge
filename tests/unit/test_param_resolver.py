"""Unit tests for parameter resolver."""

import tempfile
from pathlib import Path

import pytest

from sql_databricks_bridge.core.param_resolver import (
    ConfigNotFoundError,
    ParamResolver,
    flatten_dict,
)


class TestFlattenDict:
    """Tests for flatten_dict function."""

    def test_flat_dict(self):
        """Test already flat dictionary."""
        result = flatten_dict({"a": 1, "b": 2})
        assert result == {"a": "1", "b": "2"}

    def test_nested_dict(self):
        """Test nested dictionary flattening."""
        result = flatten_dict({"a": {"b": 1, "c": 2}})
        assert result == {"a_b": "1", "a_c": "2"}

    def test_deeply_nested_dict(self):
        """Test deeply nested dictionary."""
        result = flatten_dict({"a": {"b": {"c": 1}}})
        assert result == {"a_b_c": "1"}

    def test_mixed_nesting(self):
        """Test mixed flat and nested keys."""
        result = flatten_dict({"a": 1, "b": {"c": 2, "d": {"e": 3}}})
        assert result == {"a": "1", "b_c": "2", "b_d_e": "3"}

    def test_none_values(self):
        """Test that None values become empty strings."""
        result = flatten_dict({"a": None, "b": 1})
        assert result == {"a": "", "b": "1"}

    def test_string_values(self):
        """Test string values are preserved."""
        result = flatten_dict({"a": "hello", "b": "world"})
        assert result == {"a": "hello", "b": "world"}


@pytest.fixture
def config_dir():
    """Create a temporary directory with sample YAML configs."""
    with tempfile.TemporaryDirectory() as tmpdir:
        config_path = Path(tmpdir)

        # Create common params
        (config_path / "common_params.yaml").write_text(
            """
flg_scanner: flg_scanner
factor: factor_rw1 as factor_rw
seqDom: ""
tables:
  default_schema: dbo
  default_limit: 1000
"""
        )

        # Create Colombia config
        (config_path / "Colombia.yaml").write_text(
            """
country_code: CO
database: KWP_Colombia
tables:
  purchases_table: J_AtosCompra_CO
  default_schema: colombia
REGION:
  region_id: idgrupo
  region: descricao
"""
        )

        # Create Brasil config
        (config_path / "Brasil.yaml").write_text(
            """
country_code: BR
database: KWP_Brasil
flg_scanner: flg_scanner_br
tables:
  purchases_table: J_AtosCompra_BR
"""
        )

        yield config_path


class TestParamResolver:
    """Tests for ParamResolver class."""

    def test_get_common_params(self, config_dir):
        """Test loading common parameters."""
        resolver = ParamResolver(config_dir)
        params = resolver.get_common_params()

        assert params["flg_scanner"] == "flg_scanner"
        assert params["factor"] == "factor_rw1 as factor_rw"

    def test_get_common_params_caching(self, config_dir):
        """Test that common params are cached."""
        resolver = ParamResolver(config_dir)

        params1 = resolver.get_common_params()
        params2 = resolver.get_common_params()

        assert params1 is params2  # Same object (cached)

    def test_get_country_params(self, config_dir):
        """Test loading country-specific parameters."""
        resolver = ParamResolver(config_dir)

        colombia = resolver.get_country_params("Colombia")
        assert colombia["country_code"] == "CO"
        assert colombia["database"] == "KWP_Colombia"

        brasil = resolver.get_country_params("Brasil")
        assert brasil["country_code"] == "BR"

    def test_get_country_params_not_found(self, config_dir):
        """Test missing country config returns empty dict."""
        resolver = ParamResolver(config_dir)

        params = resolver.get_country_params("NonexistentCountry")
        assert params == {}

    def test_resolve_params_merging(self, config_dir):
        """Test that country params override common params."""
        resolver = ParamResolver(config_dir)

        params = resolver.resolve_params("Brasil")

        # Common param that's overridden
        assert params["flg_scanner"] == "flg_scanner_br"

        # Common param not overridden
        assert params["factor"] == "factor_rw1 as factor_rw"

        # Country-specific param
        assert params["country_code"] == "BR"

    def test_resolve_params_flattening(self, config_dir):
        """Test that nested params are flattened."""
        resolver = ParamResolver(config_dir)

        params = resolver.resolve_params("Colombia")

        # Flattened nested keys
        assert params["tables_purchases_table"] == "J_AtosCompra_CO"
        assert params["REGION_region_id"] == "idgrupo"

    def test_deep_merge_nested_tables(self, config_dir):
        """Test that nested dicts are merged correctly."""
        resolver = ParamResolver(config_dir)

        params = resolver.resolve_params("Colombia")

        # Country overrides common in nested dict
        assert params["tables_default_schema"] == "colombia"

        # Common param preserved when not overridden
        assert params["tables_default_limit"] == "1000"

    def test_get_table_config(self, config_dir):
        """Test getting table-specific configuration."""
        resolver = ParamResolver(config_dir)

        config = resolver.get_table_config("Colombia", "purchases_table")
        assert config["purchases_table"] == "J_AtosCompra_CO"

    def test_reload(self, config_dir):
        """Test clearing cache for reload."""
        resolver = ParamResolver(config_dir)

        # Load and cache
        params1 = resolver.get_common_params()

        # Modify file
        (config_dir / "common_params.yaml").write_text("flg_scanner: new_value")

        # Still cached
        params2 = resolver.get_common_params()
        assert params2["flg_scanner"] == "flg_scanner"

        # After reload
        resolver.reload()
        params3 = resolver.get_common_params()
        assert params3["flg_scanner"] == "new_value"


class TestConfigNotFound:
    """Tests for missing configuration handling."""

    def test_missing_common_params(self):
        """Test that missing common_params.yaml uses empty defaults."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resolver = ParamResolver(tmpdir)
            params = resolver.get_common_params()
            assert params == {}

    def test_load_yaml_not_found(self):
        """Test explicit error for _load_yaml."""
        with tempfile.TemporaryDirectory() as tmpdir:
            resolver = ParamResolver(tmpdir)

            with pytest.raises(ConfigNotFoundError):
                resolver._load_yaml("nonexistent.yaml")
