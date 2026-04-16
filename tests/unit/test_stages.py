"""Unit tests for core.stages module."""

from datetime import date

import pytest

from sql_databricks_bridge.core.stages import build_tag, load_stages


class TestLoadStages:
    """Tests for load_stages."""

    def test_loads_from_yaml(self, tmp_path):
        """Reads stages from a YAML file."""
        yaml_file = tmp_path / "stages.yaml"
        yaml_file.write_text(
            "stages:\n"
            "  - code: precios\n"
            "    name: Precios\n"
            "  - code: mtr\n"
            "    name: MTR\n"
        )

        stages = load_stages(str(yaml_file))
        assert len(stages) == 2
        assert stages[0] == {"code": "precios", "name": "Precios"}
        assert stages[1] == {"code": "mtr", "name": "MTR"}

    def test_file_not_found_falls_back_to_bundled(self, tmp_path):
        """Falls back to bundled stages.yaml when override path doesn't exist."""
        stages = load_stages(str(tmp_path / "missing.yaml"))
        assert len(stages) > 0  # Falls back to bundled config

    def test_empty_yaml(self, tmp_path):
        """Returns empty list for YAML with no stages key."""
        yaml_file = tmp_path / "empty.yaml"
        yaml_file.write_text("---\n")

        stages = load_stages(str(yaml_file))
        assert stages == []

    def test_real_config_file(self):
        """The bundled stages.yaml loads correctly."""
        stages = load_stages()
        assert len(stages) == 6
        codes = [s["code"] for s in stages]
        assert "inicio" in codes
        assert "precios" in codes
        assert "calibracion" in codes
        assert "mtr" in codes


class TestBuildTag:
    """Tests for build_tag."""

    def test_format(self):
        """Tag has format {country}-{stage}-{YYYY-MM-DD}."""
        tag = build_tag("bolivia", "calibracion")
        today = date.today().isoformat()
        assert tag == f"bolivia-calibracion-{today}"

    def test_different_inputs(self):
        """Different inputs produce different tags."""
        tag1 = build_tag("chile", "mtr")
        tag2 = build_tag("peru", "precios")
        assert tag1 != tag2
        assert "chile-mtr-" in tag1
        assert "peru-precios-" in tag2
