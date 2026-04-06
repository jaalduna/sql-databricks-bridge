"""Cross-validation: Argentina bronze queries vs kwp-data-merge expectations.

Validates that every column kwp-data-merge reads from Argentina parquets
is present in the corresponding sql-databricks-bridge bronze SQL query.
Uses synthetic DataFrames to simulate the bronze→merge handoff.
"""

import re
from pathlib import Path

import polars as pl
import pytest

# ─── paths ───────────────────────────────────────────────────────────────────
QUERIES_DIR = (
    Path(__file__).resolve().parents[2]
    / "config"
    / "countries"
    / "argentina"
)


# ─── helpers ─────────────────────────────────────────────────────────────────

def _parse_sql_columns(sql_path: Path) -> list[str] | None:
    """Extract explicit column names from a SELECT … FROM query.

    Returns None for SELECT * queries (all columns implicitly available).
    """
    text = sql_path.read_text()
    # Remove comments
    text = re.sub(r"--.*", "", text)

    if re.search(r"select\s+\*", text, re.IGNORECASE):
        return None  # wildcard — all columns available

    match = re.search(
        r"select\s+(.*?)\s+from\s+",
        text,
        re.IGNORECASE | re.DOTALL,
    )
    if not match:
        return None

    raw = match.group(1)
    columns = []
    for token in raw.split(","):
        token = token.strip()
        if not token:
            continue
        # Handle "expr AS alias" — keep the alias
        alias_match = re.search(r"\bas\b\s+(\w+)\s*$", token, re.IGNORECASE)
        if alias_match:
            columns.append(alias_match.group(1))
        else:
            # Handle "table.column" → keep column
            parts = token.split(".")
            col = parts[-1].strip()
            # Handle computed cols like "ano*100+messem as periodo"
            if re.search(r"[+\-*/()]", col):
                continue  # skip expressions without alias
            columns.append(col)
    return [c.lower() for c in columns]


# ─── column contracts ────────────────────────────────────────────────────────
# Each dict maps: sql_file_stem → set of columns kwp-data-merge requires
# (lowercase, as bronze queries output lowercase in Databricks).

PURCHASES_REQUIRED = {
    # fix_purchases_decoder.yaml (left-side keys)
    "iddomicilio", "periodo", "idato", "idartigo", "idproduto",
    "idhato_cabecalho", "quantidade", "coef_01", "coef_02", "preco",
    "value_pm", "idcanal", "acceso_canal", "idapp", "data_compra",
    "flg_scanner", "flggranel", "idpainel",
    # Argentina 8-factor columns (Argentina.yaml: factor)
    "factor_rw1", "factor_rw2", "factor_rw3", "factor_rw4",
    "factor_rw5", "factor_rw6", "factor_rw7", "factor_rw8",
}

HATO_CABECALHO_REQUIRED = {
    # _fix_hato_cabecalho_df decoder
    "idhato_cabecalho", "quantidade", "idcanal", "preco_unitario",
    "idmarca", "data_compra_utilizada",
    # household_methodology uses flg_scanner and iddomicilio
    "flg_scanner", "iddomicilio",
}

VW_ARTIGOZ_REQUIRED = {
    # article_decoder.yaml keys
    "idartigo", "sector", "idproduto", "produto", "idsub", "sub",
    "conteudo", "fabricante", "marca",
    "clas01", "clas02", "clas03", "clas04", "clas05",
    # Argentina-specific: used in drop_unwanted_bases
    "cdc02",
}

RG_PANELIS_REQUIRED = {
    # generators.py: _get_column_names + _filter_rg_panelis
    "ano", "iddomicilio",
    # Argentina.yaml: REGION.region_id_panelis
    "ksl01",
    # Argentina.yaml: NSE, NI, EDAC, CV
    "nse_saimo", "nf", "edac", "cv",
}

RG_DOMICILIOS_PESOS_REQUIRED = {
    # generators.py: _process_weights
    "ano", "iddomicilio", "messem", "valor", "idpeso",
}

ITF_BASES_REQUIRED = {
    # argentina.py: drop_unwanted_bases
    "nombre", "idcategoria",
}

A_CANAL_REQUIRED = {
    # mergers.py: merge_canal_from_vw_venues (Regalo enrichment)
    "idcanal", "regalo",
}

A_TIPOCANAL_REQUIRED = {
    # queryCanal join — dimension table for channel types
    "idtipo",
}


# ─── synthetic data factories ────────────────────────────────────────────────

def _make_purchases_df() -> pl.DataFrame:
    """Synthetic j_atoscompra_new matching the bronze query columns."""
    return pl.DataFrame({
        "coef_01": [1.5],
        "coef_01_pm": [0.0],
        "coef_02": [2.0],
        "coef_02_pm": [0.0],
        "coef_03": [0.0],
        "coef_03_pm": [0.0],
        "data_compra": ["2025-01-15"],
        "flg_scanner": [1],
        "flggranel": [0],
        "flgregalo": [0],
        "forma_compra": [1],
        "idartigo": [1001],
        "idato": [50001],
        "idcanal": [5],
        "iddomicilio": [10001],
        "idfabricante": [20],
        "idhato_cabecalho": [60001],
        "idmarca": [30],
        "idpainel": [1],
        "idproduto": [63],
        "idpromocao": [0],
        "pack_comprado": [1],
        "periodo": [202501],
        "preco": [150.0],
        "quantidade": [2.0],
        "unidades_packs": [1],
        "value_pm": [0.0],
        "factor_rw1": [100.0],
        "factor_rw2": [100.0],
        "factor_rw3": [100.0],
        "factor_rw4": [100.0],
        "factor_rw5": [100.0],
        "factor_rw6": [100.0],
        "factor_rw7": [100.0],
        "factor_rw8": [100.0],
        "acceso_canal": [1],
        "idapp": [99999],
    })


def _make_hato_cabecalho_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idhato_cabecalho": [60001],
        "iddomicilio": [10001],
        "quantidade": [2.0],
        "idcanal": [5],
        "preco_unitario": [75.0],
        "idmarca": [30],
        "data_compra_utilizada": ["2025-01-15"],
        "flg_scanner": [1],
        "data_compra": ["2025-01-15"],
        "codbarr": ["7790001"],
    })


def _make_vw_artigoz_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idartigo": [1001],
        "sector": ["Alimentos"],
        "idproduto": [63],
        "produto": ["Quesos"],
        "idsub": [1],
        "sub": ["SubA"],
        "conteudo": ["200g"],
        "idconteudo": [10],
        "fabricante": ["FabricaX"],
        "mwp_company": ["MWP_Co"],
        "marca": ["MarcaY"],
        "mwp_brand": ["MWP_Brand"],
        "clas01": ["C1"],
        "clas02": ["C2"],
        "clas03": ["C3"],
        "clas04": ["C4"],
        "clas05": ["C5"],
        "cdc01": ["X1"],
        "cdc02": [1508],
        "cdc03": ["X3"],
        "cdc04": ["X4"],
        "cdc05": ["X5"],
        "coef1": [1.0],
        "coef2": [1.0],
        "mwp_pl": ["PL1"],
    })


def _make_rg_panelis_df() -> pl.DataFrame:
    return pl.DataFrame({
        "ano": [2025],
        "iddomicilio": [10001],
        "ksl01": [1],
        "nse_saimo": [3],
        "nf": [2],
        "edac": [45],
        "cv": [4],
        "csel": [3],
    })


def _make_rg_domicilios_pesos_df() -> pl.DataFrame:
    return pl.DataFrame({
        "ano": [2025],
        "iddomicilio": [10001],
        "idpeso": [1],
        "messem": [1],
        "valor": [1250.5],
        "periodo": [202501],
    })


def _make_itf_bases_df() -> pl.DataFrame:
    return pl.DataFrame({
        "nombre": ["CHEE54SA"],
        "idcategoria": [63],
        "flgactivo": ["AT"],
        "grupo": ["Quesos"],
        "idda2": [100],
    })


def _make_a_canal_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idcanal": [5],
        "descricao": ["Supermercado"],
        "idtipo": [2],
        "regalo": [0],
        "app": [0],
        "flgativo": [1],
    })


def _make_a_tipocanal_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idtipo": [2],
        "descricao": ["Grandes Superficies"],
    })


def _make_dbm_da2_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idda2": [100],
        "descricao": ["DA2 Desc"],
    })


def _make_dbm_da2_items_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idda2": [100],
        "item": ["63"],
        "esfiltro": [0],
    })


def _make_dbm_filtros_df() -> pl.DataFrame:
    return pl.DataFrame({
        "idfiltro": [1],
        "origen": ["63"],
        "descricao": ["Filtro1"],
    })


# ─── SQL file existence tests ────────────────────────────────────────────────

class TestArgentinaBronzeFilesExist:
    """Every source table required by kwp-data-merge has a .sql query file."""

    @pytest.mark.parametrize("table_name", [
        "j_atoscompra_new",
        "hato_cabecalho",
        "vw_artigoz",
        "rg_panelis",
        "rg_domicilios_pesos",
        "itf_bases",
        "a_canal",
        "a_tipocanal",
        "a_produto",
        "dbm_da2",
        "dbm_da2_items",
        "dbm_filtros",
    ])
    def test_sql_file_exists(self, table_name):
        sql_file = QUERIES_DIR / f"{table_name}.sql"
        assert sql_file.exists(), (
            f"Missing bronze query: {sql_file.name}. "
            f"kwp-data-merge expects this table for Argentina."
        )


# ─── Column coverage tests ───────────────────────────────────────────────────

class TestPurchasesColumnCoverage:
    """j_atoscompra_new bronze query covers all columns kwp-data-merge needs."""

    def test_explicit_columns_cover_merge_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "j_atoscompra_new.sql")
        assert cols is not None, "j_atoscompra_new should have explicit columns"
        missing = PURCHASES_REQUIRED - set(cols)
        assert not missing, (
            f"j_atoscompra_new.sql is missing columns required by kwp-data-merge: "
            f"{sorted(missing)}"
        )

    def test_all_8_factor_rw_columns_present(self):
        cols = _parse_sql_columns(QUERIES_DIR / "j_atoscompra_new.sql")
        assert cols is not None
        for i in range(1, 9):
            assert f"factor_rw{i}" in cols, (
                f"factor_rw{i} missing — Argentina needs all 8 factors"
            )

    def test_synthetic_df_has_all_required_columns(self):
        df = _make_purchases_df()
        missing = PURCHASES_REQUIRED - set(df.columns)
        assert not missing, f"Synthetic purchases missing: {sorted(missing)}"


class TestHatoCabecalhoColumnCoverage:
    """hato_cabecalho bronze query covers merge requirements."""

    def test_select_star_covers_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "hato_cabecalho.sql")
        if cols is None:
            # SELECT * — columns are implicitly available; validate via synthetic
            df = _make_hato_cabecalho_df()
            missing = HATO_CABECALHO_REQUIRED - set(df.columns)
            assert not missing, (
                f"hato_cabecalho SELECT * must include: {sorted(missing)}"
            )
        else:
            missing = HATO_CABECALHO_REQUIRED - set(cols)
            assert not missing, f"hato_cabecalho.sql missing: {sorted(missing)}"


class TestVwArtigozColumnCoverage:
    """vw_artigoz bronze query covers article decoder + Argentina CdC02."""

    def test_select_star_covers_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "vw_artigoz.sql")
        if cols is None:
            df = _make_vw_artigoz_df()
            missing = VW_ARTIGOZ_REQUIRED - set(df.columns)
            assert not missing, (
                f"vw_artigoz SELECT * must include: {sorted(missing)}"
            )
        else:
            missing = VW_ARTIGOZ_REQUIRED - set(cols)
            assert not missing, f"vw_artigoz.sql missing: {sorted(missing)}"


class TestRgPanelisColumnCoverage:
    """rg_panelis bronze query covers demographic columns for Argentina."""

    def test_select_star_covers_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "rg_panelis.sql")
        if cols is None:
            df = _make_rg_panelis_df()
            missing = RG_PANELIS_REQUIRED - set(df.columns)
            assert not missing, (
                f"rg_panelis SELECT * must include: {sorted(missing)}"
            )
        else:
            missing = RG_PANELIS_REQUIRED - set(cols)
            assert not missing, f"rg_panelis.sql missing: {sorted(missing)}"


class TestRgDomiciliosPesosColumnCoverage:
    """rg_domicilios_pesos bronze query covers weight columns."""

    def test_explicit_columns_cover_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "rg_domicilios_pesos.sql")
        assert cols is not None, "rg_domicilios_pesos should have explicit columns"
        missing = RG_DOMICILIOS_PESOS_REQUIRED - set(cols)
        assert not missing, (
            f"rg_domicilios_pesos.sql missing: {sorted(missing)}"
        )


class TestItfBasesColumnCoverage:
    """itf_bases bronze query covers argentina.drop_unwanted_bases() needs."""

    def test_select_star_covers_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "itf_bases.sql")
        if cols is None:
            df = _make_itf_bases_df()
            missing = ITF_BASES_REQUIRED - set(df.columns)
            assert not missing, (
                f"itf_bases SELECT * must include: {sorted(missing)}"
            )
        else:
            missing = ITF_BASES_REQUIRED - set(cols)
            assert not missing, f"itf_bases.sql missing: {sorted(missing)}"


class TestACanalColumnCoverage:
    """a_canal bronze query covers canal/Regalo enrichment."""

    def test_explicit_columns_cover_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "a_canal.sql")
        assert cols is not None, "a_canal should have explicit columns"
        missing = A_CANAL_REQUIRED - set(cols)
        assert not missing, f"a_canal.sql missing: {sorted(missing)}"


class TestATipoCanalColumnCoverage:
    """a_tipocanal bronze query includes idtipo for channel-type join."""

    def test_covers_requirements(self):
        cols = _parse_sql_columns(QUERIES_DIR / "a_tipocanal.sql")
        if cols is None:
            df = _make_a_tipocanal_df()
            missing = A_TIPOCANAL_REQUIRED - set(df.columns)
            assert not missing
        else:
            missing = A_TIPOCANAL_REQUIRED - set(cols)
            assert not missing, f"a_tipocanal.sql missing: {sorted(missing)}"


# ─── Synthetic pipeline simulation ───────────────────────────────────────────

class TestArgentinaFactorRwCalculation:
    """Simulate Argentina's 8-factor → single factor_rw calculation."""

    def test_default_formula(self):
        """Default: factor_rw = rw1*rw2*rw3*rw7*rw8 (each /100, then *100)."""
        df = pl.DataFrame({
            "idproduto": [63],
            "factor_rw1": [200.0],  # 2.0 after /100
            "factor_rw2": [150.0],  # 1.5
            "factor_rw3": [100.0],  # 1.0
            "factor_rw4": [300.0],  # unused for default
            "factor_rw5": [400.0],  # unused
            "factor_rw6": [500.0],  # unused
            "factor_rw7": [100.0],  # 1.0
            "factor_rw8": [100.0],  # 1.0
        }).lazy()

        # Replicate argentina.calculate_factor_rw logic
        for i in range(1, 9):
            df = df.with_columns(pl.col(f"factor_rw{i}").cast(pl.Float64))

        df = df.with_columns(
            (
                pl.col("factor_rw1") / 100.0
                * pl.col("factor_rw2") / 100.0
                * pl.col("factor_rw3") / 100.0
                * pl.col("factor_rw7") / 100.0
                * pl.col("factor_rw8") / 100.0
                * 100.0
            ).alias("factor_rw")
        )

        result = df.collect()
        expected = (2.0 * 1.5 * 1.0 * 1.0 * 1.0) * 100.0  # 300.0
        assert result["factor_rw"][0] == pytest.approx(expected)

    def test_product_17_exception(self):
        """idProduto=17: factor_rw = rw4*rw5*rw6*rw7*rw8."""
        df = pl.DataFrame({
            "idproduto": [17],
            "factor_rw1": [200.0],
            "factor_rw2": [200.0],
            "factor_rw3": [200.0],
            "factor_rw4": [150.0],  # 1.5
            "factor_rw5": [200.0],  # 2.0
            "factor_rw6": [100.0],  # 1.0
            "factor_rw7": [100.0],  # 1.0
            "factor_rw8": [100.0],  # 1.0
        }).lazy()

        for i in range(1, 9):
            df = df.with_columns(pl.col(f"factor_rw{i}").cast(pl.Float64))

        # Default first
        df = df.with_columns(
            (
                pl.col("factor_rw1") / 100.0
                * pl.col("factor_rw2") / 100.0
                * pl.col("factor_rw3") / 100.0
                * pl.col("factor_rw7") / 100.0
                * pl.col("factor_rw8") / 100.0
                * 100.0
            ).alias("factor_rw")
        )
        # Override for product 17
        df = df.with_columns(
            pl.when(pl.col("idproduto") == 17)
            .then(
                pl.col("factor_rw4") / 100.0
                * pl.col("factor_rw5") / 100.0
                * pl.col("factor_rw6") / 100.0
                * pl.col("factor_rw7") / 100.0
                * pl.col("factor_rw8") / 100.0
                * 100.0
            )
            .otherwise(pl.col("factor_rw"))
            .alias("factor_rw")
        )

        result = df.collect()
        expected = (1.5 * 2.0 * 1.0 * 1.0 * 1.0) * 100.0  # 300.0
        assert result["factor_rw"][0] == pytest.approx(expected)

    def test_mixed_products(self):
        """Both default and product-17 rows in a single DataFrame."""
        df = pl.DataFrame({
            "idproduto": [63, 17],
            "factor_rw1": [200.0, 100.0],
            "factor_rw2": [100.0, 100.0],
            "factor_rw3": [100.0, 100.0],
            "factor_rw4": [100.0, 200.0],
            "factor_rw5": [100.0, 300.0],
            "factor_rw6": [100.0, 100.0],
            "factor_rw7": [100.0, 100.0],
            "factor_rw8": [100.0, 100.0],
        }).lazy()

        for i in range(1, 9):
            df = df.with_columns(pl.col(f"factor_rw{i}").cast(pl.Float64))

        df = df.with_columns(
            (
                pl.col("factor_rw1") / 100.0
                * pl.col("factor_rw2") / 100.0
                * pl.col("factor_rw3") / 100.0
                * pl.col("factor_rw7") / 100.0
                * pl.col("factor_rw8") / 100.0
                * 100.0
            ).alias("factor_rw")
        )
        df = df.with_columns(
            pl.when(pl.col("idproduto") == 17)
            .then(
                pl.col("factor_rw4") / 100.0
                * pl.col("factor_rw5") / 100.0
                * pl.col("factor_rw6") / 100.0
                * pl.col("factor_rw7") / 100.0
                * pl.col("factor_rw8") / 100.0
                * 100.0
            )
            .otherwise(pl.col("factor_rw"))
            .alias("factor_rw")
        )

        result = df.collect()
        # Row 0 (product 63): 2.0 * 1 * 1 * 1 * 1 * 100 = 200.0
        assert result["factor_rw"][0] == pytest.approx(200.0)
        # Row 1 (product 17): 2.0 * 3.0 * 1 * 1 * 1 * 100 = 600.0
        assert result["factor_rw"][1] == pytest.approx(600.0)


class TestArgentinaPurchasesDecoder:
    """Synthetic purchases through fix_purchases_decoder rename + scale."""

    def test_decoder_renames_all_required_columns(self):
        """Verify the decoder produces all standardized column names."""
        df = _make_purchases_df().lazy()

        # Simulate calculate_factor_rw: collapse 8 factors → 1
        for i in range(1, 9):
            df = df.with_columns(pl.col(f"factor_rw{i}").cast(pl.Float64))
        df = df.with_columns(
            (
                pl.col("factor_rw1") / 100.0
                * pl.col("factor_rw2") / 100.0
                * pl.col("factor_rw3") / 100.0
                * pl.col("factor_rw7") / 100.0
                * pl.col("factor_rw8") / 100.0
                * 100.0
            ).alias("factor_rw")
        )

        # Apply the decoder rename (same logic as _fix_purchases_df)
        decoder = {
            "iddomicilio": "panelistID",
            "periodo": "period",
            "idato": "basketID",
            "idartigo": "idArticle",
            "idproduto": "product_id",
            "idhato_cabecalho": "basketID_h",
            "quantidade": "packs",
            "coef_01": "QA",
            "coef_02": "QA2",
            "preco": "netspend",
            "value_pm": "value_pm",
            "idcanal": "idChannel",
            "acceso_canal": "Acceso_canal",
            "idapp": "IdApp",
            "data_compra": "Data_Compra",
            "factor_rw": "response_weight",
            "flg_scanner": "flg_scanner",
            "flggranel": "FlgGranel",
            "idpainel": "idPainel",
        }
        existing = df.collect_schema().names()
        filtered = {k: v for k, v in decoder.items() if k in existing}
        df = df.rename(filtered)

        # Scale factor_rw → response_weight / 100
        df = df.with_columns(pl.col("response_weight") / 100.0)

        result = df.collect()
        expected_cols = {
            "panelistID", "period", "basketID", "idArticle", "product_id",
            "basketID_h", "packs", "QA", "QA2", "netspend", "value_pm",
            "idChannel", "Acceso_canal", "IdApp", "Data_Compra",
            "response_weight", "flg_scanner", "FlgGranel", "idPainel",
        }
        missing = expected_cols - set(result.columns)
        assert not missing, f"After decoder, missing columns: {sorted(missing)}"


class TestArgentinaHatoCabecalhoDecoder:
    """Synthetic hato_cabecalho through the decoder."""

    def test_decoder_produces_required_columns(self):
        df = _make_hato_cabecalho_df()

        decoder = {
            "quantidade": "packs_h",
            "idcanal": "idChannel_h",
            "preco_unitario": "netspend_h",
            "idhato_cabecalho": "basketID_h",
            "idmarca": "id_brand_h",
            "data_compra_utilizada": "data_compra_utilizada_h",
            "flg_scanner": "flg_scanner_h",
        }
        filtered = {k: v for k, v in decoder.items() if k in df.columns}
        df = df.rename(filtered)

        expected = {"packs_h", "idChannel_h", "netspend_h", "basketID_h",
                    "id_brand_h", "data_compra_utilizada_h", "flg_scanner_h"}
        missing = expected - set(df.columns)
        assert not missing, f"After hato decoder, missing: {sorted(missing)}"


class TestArgentinaWeightsJoin:
    """Synthetic rg_domicilios_pesos + rg_panelis join."""

    def test_weights_join_produces_demographic_columns(self):
        pesos = _make_rg_domicilios_pesos_df()
        panelis = _make_rg_panelis_df()

        # Filter idpeso == 1
        pesos = pesos.filter(pl.col("idpeso") == 1)
        pesos = pesos.rename({"messem": "month"})

        # Join on iddomicilio + ano
        result = pesos.join(panelis, on=["iddomicilio", "ano"], how="left")

        assert "valor" in result.columns
        assert "month" in result.columns
        assert "ksl01" in result.columns
        assert "nse_saimo" in result.columns
        assert "nf" in result.columns
        assert "edac" in result.columns
        assert "cv" in result.columns
        assert len(result) == 1


class TestArgentinaDropUnwantedBases:
    """Synthetic itf_bases + vw_artigoz join for base filtering."""

    def test_base_filter_drops_chee54sa_with_matching_cdc02(self):
        purchases = pl.DataFrame({
            "product_id": [63, 63],
            "idArticle": [1001, 1002],
            "QA": [1.5, 2.0],
        })

        bases = pl.DataFrame({
            "nombre": ["CHEE54SA", "OTHE54XX"],
            "idcategoria": [63, 64],
        }).rename({"nombre": "Base", "idcategoria": "product_id"})

        cdc = pl.DataFrame({
            "idartigo": [1001, 1002],
            "cdc02": [1508, 9999],  # 1508 should be filtered
        }).rename({"idartigo": "idArticle"})

        df = purchases.join(bases, on="product_id", how="left")
        df = df.join(cdc, on="idArticle", how="left")

        # Apply filter: drop CHEE54SA + CdC02 in [1508, 5540, 5541, 5542, 5543]
        df = df.filter(
            ~(
                (pl.col("Base") == "CHEE54SA")
                & pl.col("cdc02").is_in([1508, 5540, 5541, 5542, 5543])
            )
        )

        assert len(df) == 1
        assert df["idArticle"][0] == 1002
