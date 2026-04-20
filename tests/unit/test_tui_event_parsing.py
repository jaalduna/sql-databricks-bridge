"""Unit tests for the TUI event-row parsers (country, detail, target).

The TUI imports textual at module load.  Textual is a heavy GUI dep,
so these tests skip cleanly if it's not installed in the dev env.
"""
import pytest

textual = pytest.importorskip("textual")

from sql_databricks_bridge.tui.monitor import (  # noqa: E402
    _event_detail_summary,
    _event_target_short,
    _extract_country,
    _parse_json_field,
)


# ---------- _parse_json_field ----------


class TestParseJsonField:
    def test_none_returns_empty_dict(self):
        assert _parse_json_field(None) == {}

    def test_empty_string_returns_empty_dict(self):
        assert _parse_json_field("") == {}

    def test_dict_passes_through(self):
        d = {"a": 1, "b": "x"}
        assert _parse_json_field(d) == d

    def test_valid_json_string_decoded(self):
        assert _parse_json_field('{"PAIS":"CR"}') == {"PAIS": "CR"}

    def test_invalid_json_returns_empty(self):
        assert _parse_json_field("not json {{") == {}

    def test_json_array_not_a_dict_returns_empty(self):
        assert _parse_json_field("[1,2,3]") == {}


# ---------- _extract_country ----------


class TestExtractCountry:
    def test_country_from_filter_conditions_pais(self):
        ev = {
            "operation": "DELETE",
            "filter_conditions": '{"PAIS": "CR", "fecha_referencia": "2026-04-20"}',
        }
        assert _extract_country(ev) == "CR"

    def test_pais_already_uppercased_and_trimmed(self):
        ev = {"filter_conditions": {"PAIS": " ar "}}
        assert _extract_country(ev) == "AR"

    def test_lowercase_pais_key_supported(self):
        ev = {"filter_conditions": {"pais": "BO"}}
        assert _extract_country(ev) == "BO"

    def test_country_key_fallback(self):
        ev = {"filter_conditions": {"country": "CL"}}
        assert _extract_country(ev) == "CL"

    def test_insert_event_parses_from_source_schema_cam(self):
        ev = {
            "operation": "INSERT",
            "source_table": "005-purchasing-prd.gold_cam_cr.prediccion_compras_staging",
        }
        assert _extract_country(ev) == "CR"

    def test_insert_event_parses_from_source_schema_full_country_name(self):
        ev = {
            "operation": "INSERT",
            "source_table": "005-purchasing-prd.gold_ecuador.prediccion_compras_staging",
        }
        assert _extract_country(ev) == "ECU"

    @pytest.mark.parametrize("token,expected", [
        ("gold_argentina", "AR"),
        ("gold_bolivia", "BO"),
        ("gold_brasil", "BR"),
        ("gold_chile", "CL"),
        ("gold_colombia", "CO"),
        ("gold_mexico", "MX"),
        ("gold_peru", "PE"),
        ("gold_cam_gt", "GT"),
        ("gold_cam_sv", "SV"),
        ("gold_cam_hn", "HN"),
        ("gold_cam_ni", "NI"),
        ("gold_cam_pa", "PA"),
        ("gold_cam_do", "DO"),
        ("gold_cam", "CAM"),
    ])
    def test_country_token_map_full_coverage(self, token, expected):
        ev = {
            "operation": "INSERT",
            "source_table": f"005-purchasing-prd.{token}.prediccion_compras_staging",
        }
        assert _extract_country(ev) == expected

    def test_unknown_token_returns_uppercase_token(self):
        ev = {
            "operation": "INSERT",
            "source_table": "005-purchasing-prd.gold_xyz.prediccion_compras_staging",
        }
        assert _extract_country(ev) == "XYZ"

    def test_no_source_no_filter_returns_dash(self):
        assert _extract_country({"operation": "INSERT"}) == "-"

    def test_filter_pais_takes_priority_over_source(self):
        """Even if both are present (unusual), filter wins."""
        ev = {
            "operation": "DELETE",
            "filter_conditions": {"PAIS": "CR"},
            "source_table": "005-purchasing-prd.gold_argentina.prediccion_compras_staging",
        }
        assert _extract_country(ev) == "CR"


# ---------- _event_detail_summary ----------


class TestEventDetailSummary:
    def test_delete_shows_filter_conditions_excluding_pais(self):
        ev = {
            "operation": "DELETE",
            "filter_conditions": {
                "PAIS": "CR",
                "fecha_referencia": "2026-04-20",
                "Campania": "recuerdo_categorias",
                "Etapa": "Cliente",
            },
        }
        d = _event_detail_summary(ev)
        assert "PAIS" not in d
        assert "fecha_referencia=2026-04-20" in d
        assert "Campania=recuerdo_categorias" in d
        assert "Etapa=Cliente" in d

    def test_delete_with_no_filter_shows_target(self):
        ev = {"operation": "DELETE", "target_table": "dbo.X", "filter_conditions": None}
        assert "dbo.X" in _event_detail_summary(ev)
        assert "no filter" in _event_detail_summary(ev)

    def test_delete_with_only_pais_filter(self):
        ev = {"operation": "DELETE", "filter_conditions": {"PAIS": "CR"}}
        assert _event_detail_summary(ev) == "(only PAIS)"

    def test_insert_strips_catalog_prefix(self):
        ev = {
            "operation": "INSERT",
            "source_table": "005-purchasing-prd.gold_cam_cr.prediccion_compras_staging",
        }
        d = _event_detail_summary(ev)
        assert "005-purchasing-prd" not in d
        assert "gold_cam_cr.prediccion_compras_staging" in d

    def test_insert_with_no_source_returns_dash(self):
        ev = {"operation": "INSERT", "source_table": ""}
        assert _event_detail_summary(ev) == "-"


# ---------- _event_target_short ----------


class TestEventTargetShort:
    def test_with_metadata_strips_ktclsql_prefix(self):
        ev = {
            "metadata": '{"target_server":"KTCLSQL005","target_database":"LATAM_KWP"}',
            "target_table": "dbo.Prediccion_compras",
        }
        assert _event_target_short(ev) == "005/LATAM_KWP.dbo.Prediccion_compras"

    def test_with_metadata_dict_directly(self):
        ev = {
            "metadata": {"target_server": "KTCLSQL002", "target_database": "BO_KWP"},
            "target_table": "dbo.x",
        }
        assert _event_target_short(ev) == "002/BO_KWP.dbo.x"

    def test_no_metadata_falls_back_to_target_table(self):
        ev = {"metadata": None, "target_table": "dbo.X"}
        assert _event_target_short(ev) == "dbo.X"

    def test_no_target_returns_dash(self):
        assert _event_target_short({}) == "-"

    def test_partial_metadata_uses_fallback(self):
        """Only target_server but no database — fall back to plain target."""
        ev = {
            "metadata": {"target_server": "KTCLSQL005"},
            "target_table": "dbo.X",
        }
        # Uses fallback because db is missing
        assert _event_target_short(ev) == "dbo.X"

    def test_non_kt_server_left_intact(self):
        ev = {
            "metadata": {"target_server": "myserver.kantar.com", "target_database": "X"},
            "target_table": "dbo.t",
        }
        assert "myserver.kantar.com" in _event_target_short(ev)
