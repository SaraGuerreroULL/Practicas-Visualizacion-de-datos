import os
from pathlib import Path
import json
import re
from typing import Optional, List
from mizani.bounds import squish
import pandas as pd
from plotnine import *
from dagster import ( AssetSelection, asset, asset_check, AssetCheckResult, Definitions, MaterializeResult, 
                      MetadataValue, define_asset_job, sensor, RunRequest, SkipReason)


# ================================================================
# CONFIGURACIÓN DE RUTAS
# ================================================================

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
CARTO_DIR = DATA_DIR / "cartografia-secciones"
VIS_DIR = BASE_DIR / "visualizaciones"

GEOJSON_POR_año = {
    2021: CARTO_DIR / "secciones_20210101_tenerife.json",
    2022: CARTO_DIR / "secciones_20220101_tenerife.json",
    2023: CARTO_DIR / "secciones_20230101_tenerife.json",
    2024: CARTO_DIR / "secciones_20240101_tenerife.json",
}

# ================================================================
# FUNCIONES AUXILIARES
# ================================================================

def _extract_section_key(code_series: pd.Series) -> pd.Series:
    """Extrae 'codmun_Ddd_Ssss' de un TERRITORIO_CODE/geocode completo.

    Ejemplo: '20220101_38001_D01_S001' → '38001_D01_S001'
    La clave resultante es estable entre años y compatible con todas las fuentes.
    """
    return code_series.astype(str).str.split("_", n=1).str[1]


def _normalizar_columnas(df: pd.DataFrame) -> pd.DataFrame:
    """Elimina espacios extremos en nombres de columna."""
    df.columns = [c.strip() for c in df.columns]
    return df


def _normalizar_municipio_ine(nombre: str) -> str:
    """Normaliza nombres de municipios según convenciones del INE"""
    if pd.isna(nombre):
        return nombre

    nombre = str(nombre).strip()
    nombre = re.sub(r"\s+", " ", nombre)

    if not nombre:
        return nombre

    if "," in nombre:
        base, articulo = [p.strip() for p in nombre.split(",", 1)]

        if articulo in ["El", "La", "Los", "Las"]:
            nombre = f"{articulo} {base}"

    # Normaliza casos concretos de municipios
    reemplazos = {
        "Fuencaliente de la Palma": "Fuencaliente de La Palma",
        "Santa Cruz de la Palma": "Santa Cruz de La Palma",
        "Puerto de la Cruz": "Puerto de La Cruz",
        "San Sebastián de la Gomera": "San Sebastián de La Gomera",
    }

    nombre = reemplazos.get(nombre, nombre)

    return nombre


def _fortify_geojson_secciones_tenerife(geojson_path: Path) -> pd.DataFrame:
    """Convierte un GeoJSON de secciones de Tenerife al formato long de plotnine:
    columnas geocode, long, lat, group. Solo extrae el anillo exterior de cada parte.
    """
    with open(geojson_path, encoding="utf-8") as f:
        gj = json.load(f)

    rows = []
    for feature in gj["features"]:
        props = feature.get("properties", {})

        if props.get("gcd_isla") != "ES709":
            continue

        if props.get("granularidad") != "SECCIONES":
            continue

        geocode = props.get("geocode", "")
        geom = feature.get("geometry", {})
        geom_type = geom.get("type")

        if geom_type == "Polygon":
            rings = [geom["coordinates"][0]]
        elif geom_type == "MultiPolygon":
            rings = [poly[0] for poly in geom["coordinates"]]
        else:
            continue

        for part_idx, ring in enumerate(rings):
            group_id = f"{geocode}_{part_idx}"
            for lon, lat in ring:
                rows.append({"geocode": geocode, "long": lon, "lat": lat, "group": group_id})

    return pd.DataFrame(rows)


def _fortify_geojson_municipios_tenerife(geojson_path: Path) -> pd.DataFrame:
    """Convierte un GeoJSON de municipios de Tenerife al formato long de plotnine:
    columnas geocode, municipio, long, lat, group. Solo extrae el anillo exterior de cada parte.
    """
    with open(geojson_path, encoding="utf-8") as f:
        gj = json.load(f)

    rows = []
    for feature in gj["features"]:
        props = feature.get("properties", {})

        if props.get("gcd_isla") != "ES709":
            continue

        if props.get("granularidad") not in ["MUNICIPIOS", "MUNICIPIO"]:
            continue

        geocode = props.get("geocode", props.get("gcd_municipio", ""))
        municipio = props.get("municipio", props.get("nombre", props.get("etiqueta", "")))
        geom = feature.get("geometry", {})
        geom_type = geom.get("type")

        if geom_type == "Polygon":
            rings = [geom["coordinates"][0]]
        elif geom_type == "MultiPolygon":
            rings = [poly[0] for poly in geom["coordinates"]]
        else:
            continue

        for part_idx, ring in enumerate(rings):
            group_id = f"{geocode}_{part_idx}"
            for lon, lat in ring:
                rows.append({
                    "geocode": geocode,
                    "municipio": municipio,
                    "long": lon,
                    "lat": lat,
                    "group": group_id,
                })

    return pd.DataFrame(rows)


def _calcular_quintiles(df: pd.DataFrame, variable: str, labels: Optional[List[str]] = None) -> pd.DataFrame:
    """Añade columna 'quintil' calculada sobre la variable dada."""
    if labels is None:
        labels = ["Q1", "Q2", "Q3", "Q4", "Q5"]
    df = df.dropna(subset=[variable]).copy()
    df["quintil"] = pd.qcut(df[variable], q=5, labels=labels, duplicates="drop").astype(str)
    return df


# ================================================================
# CAPA 1: CARGA
# ================================================================

@asset(
    group_name="carga",
    description="Carga el CSV de renta media y mediana por sección censal de Santa Cruz de Tenerife (2021-2023).",
)
def raw_rentamedia() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "rentamedia-sc-3.csv")


@asset(
    group_name="carga",
    description=(
        "Carga el CSV de distribución de renta por fuente de ingresos a nivel sección censal "
        "(2021-2023). Cubre ambas provincias canarias; se filtrará a la provincia 38 en limpieza."
    ),
)
def raw_distribucion() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "distribucion-renta-ingresos.csv")


@asset(
    group_name="carga",
    description=(
        "Carga el CSV de relación con la actividad económica por sección censal, "
        "Santa Cruz de Tenerife (2021-2023), desagregado por sexo y sector."
    ),
)
def raw_actividad() -> pd.DataFrame:
    return pd.read_csv(DATA_DIR / "actividad-sc-3.csv")


# ================================================================
# CAPA 2: LIMPIEZA
# ================================================================

@asset(
    group_name="limpieza",
    description=(
        "Limpia el dataset de renta media: normaliza nombres de columna, "
        "extrae section_key del TERRITORIO_CODE, tipifica OBS_VALUE como numérico "
        "y elimina nulos en claves primarias."
    ),
)
def df_rentamedia(raw_rentamedia: pd.DataFrame) -> pd.DataFrame:
    df = _normalizar_columnas(raw_rentamedia.copy())

    df = df.rename(columns={
        "año": "año",
        "MEDIDAS_CODE": "medida_code",
        "MEDIDAS#es": "medida",
        "TERRITORIO_CODE": "territorio_code",
        "OBS_VALUE": "valor",
    })

    df["section_key"] = _extract_section_key(df["territorio_code"])
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    df["valor"] = pd.to_numeric(df["valor"], errors="coerce")

    df = df.dropna(subset=["año", "section_key", "valor"])
    df = df.drop_duplicates()
    # df = df.drop(columns=["medida_code"])
    return df.reset_index(drop=True)


@asset(
    group_name="limpieza",
    description=(
        "Limpia el dataset de distribución de renta: normaliza columnas, convierte OBS_VALUE "
        "(coma decimal → punto), filtra a municipios de la provincia 38 (Santa Cruz de Tenerife) "
        "y extrae section_key."
    ),
)
def df_distribucion(raw_distribucion: pd.DataFrame) -> pd.DataFrame:
    df = _normalizar_columnas(raw_distribucion.copy())

    df = df.rename(columns={
        "año": "año",
        "MEDIDAS_CODE": "medida_code",
        "MEDIDAS#es": "medida",
        "TERRITORIO_CODE": "territorio_code",
        "OBS_VALUE": "valor",
    })

    df["section_key"] = _extract_section_key(df["territorio_code"])

    # Solo municipios de Santa Cruz de Tenerife (código INE 38xxx)
    df = df[df["section_key"].str.startswith("38", na=False)].copy()

    # OBS_VALUE viene con coma decimal; convertir a float
    df["valor"] = (
        df["valor"].astype(str)
        .str.replace(",", ".", regex=False)
        .pipe(pd.to_numeric, errors="coerce")
    )

    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    df = df.dropna(subset=["año", "section_key", "valor"])
    df = df.drop_duplicates()
    # df = df.drop(columns=["medida_code"])
    return df.reset_index(drop=True)


@asset(
    group_name="limpieza",
    description=(
        "Limpia el dataset de actividad: normaliza columnas, extrae section_key del geocode, "
        "convierte num_casos a numérico (NaN → 0) y agrega ambos sexos."
    ),
)
def df_actividad(raw_actividad: pd.DataFrame) -> pd.DataFrame:
    df = _normalizar_columnas(raw_actividad.copy())

    rename = { "Actividad económica": "actividad",
               "Periodo": "año"}
    df = df.rename(columns=rename)

    df["section_key"] = _extract_section_key(df["geocode"])
    df["año"] = pd.to_numeric(df["año"], errors="coerce").astype("Int64")
    df["num_casos"] = pd.to_numeric(df["num_casos"], errors="coerce").fillna(0).astype(int)
    df["municipio"] = df["municipio"].astype(str).str.strip()
    df["municipio"] = df["municipio"].apply(_normalizar_municipio_ine)

    # Agregar ambos sexos
    df = (
        df.groupby(["año", "section_key", "municipio", "actividad"], as_index=False)["num_casos"]
        .sum()
    )

    df = df.dropna(subset=["año", "section_key"])
    # df = df.drop(columns=["cod_provincia", "provincia", "cod_municipio"])
    return df.reset_index(drop=True)


# ================================================================
# CAPA 3: TRANSFORMACIÓN E INTEGRACIÓN
# ================================================================

@asset(
    group_name="transformacion",
    description=(
        "Unifica renta media, distribución de ingresos y actividad económica "
        "en una sola base por año, sección censal y municipio."
    ),
)
def base_unificada(df_rentamedia: pd.DataFrame, df_distribucion: pd.DataFrame,
                                   df_actividad: pd.DataFrame) -> pd.DataFrame:
    claves = ["año", "section_key", "municipio"]

    # Renta media
    df = df_rentamedia.copy()

    renta_wide = (
        df.pivot_table(index=claves, columns="medida", values="valor", aggfunc="first")
        .reset_index()
        .rename(columns={
            "Renta bruta media por hogar": "renta_bruta_media_hogar",
            "Renta bruta media por persona": "renta_bruta_media_persona",
            "Renta neta media por hogar": "renta_neta_media_hogar",
            "Renta neta media por persona": "renta_neta_media_persona",
            "Media de la renta por unidad de consumo": "renta_media_unidad_consumo",
            "Mediana de la renta por unidad de consumo": "renta_mediana_unidad_consumo",
        })
    )

    renta_wide.columns.name = None

    # Distribución de ingresos
    df = df_distribucion.copy()

    distribucion_wide = (
        df.pivot_table(index=claves, columns="medida", values="valor", aggfunc="first")
        .reset_index()
        .rename(columns={
            "Otras prestaciones": "pct_otras_prestaciones",
            "Otros ingresos": "pct_otros_ingresos",
            "Pensiones": "pct_pensiones",
            "Prestaciones por desempleo": "pct_prestaciones_desempleo",
            "Sueldos y salarios": "pct_sueldos_salarios",
        })
    )

    distribucion_wide.columns.name = None

    # Actividad económica
    df = df_actividad.copy()

    actividad_wide = (
        df.pivot_table(
            index=claves,
            columns="actividad",
            values="num_casos",
            aggfunc="sum",
            fill_value=0,
        )
        .reset_index()
        .rename(columns={
            "Agricultura, ganadería y pesca": "actividad_agricultura_ganaderia_pesca",
            "Construcción": "actividad_construccion",
            "Industria": "actividad_industria",
            "No consta": "actividad_no_consta",
            "Servicios": "actividad_servicios",
        })
    )

    actividad_wide.columns.name = None

    # -----------------------------
    # Base unificada
    # -----------------------------
    base = (
        renta_wide
        .merge(distribucion_wide, on=claves, how="outer", validate="one_to_one")
        .merge(actividad_wide, on=claves, how="outer", validate="one_to_one")
        .sort_values(["año", "municipio", "section_key"])
        .reset_index(drop=True)
    )

    return base


@asset(
    group_name="transformacion",
    description=(
        "Filtra la base unificada para conservar únicamente los municipios pertenecientes a la isla de Tenerife"
    ),
)
def df_filtro_tenerife(base_unificada: pd.DataFrame) -> pd.DataFrame:

    municipios_tenerife = [
        "Adeje", "Arafo", "Arico", "Arona", "Buenavista del Norte", "Candelaria", "El Rosario", "El Sauzal", 
        "El Tanque", "Fasnia", "Garachico", "Granadilla de Abona", "La Guancha", "Guía de Isora", "Güímar", 
        "Icod de los Vinos", "San Cristóbal de La Laguna", "La Matanza de Acentejo", "La Orotava", 
        "La Victoria de Acentejo", "Los Realejos", "Los Silos", "Puerto de La Cruz", "San Juan de la Rambla", 
        "San Miguel de Abona", "Santa Cruz de Tenerife", "Santa Úrsula", "Santiago del Teide", "Tacoronte", 
        "Tegueste", "Vilaflor de Chasna"
    ]

    df = base_unificada.copy()

    df = df[df["municipio"].isin(municipios_tenerife)].copy()

    df = df.sort_values(["año", "municipio", "section_key"]).reset_index(drop=True)

    return df


# ================================================================
# CAPA 4: VISUALIZACIÓN
# ================================================================

@asset(
    group_name="visualizacion",
    description="Mapa coroplético seccional de la mediana de renta por unidad de consumo en Tenerife (2023).",
)
def graph_01(df_filtro_tenerife: pd.DataFrame) -> MaterializeResult:
    df_2023 = df_filtro_tenerife[df_filtro_tenerife["año"] == 2023].copy()

    map_df = _fortify_geojson_secciones_tenerife(GEOJSON_POR_año[2024])
    map_df["section_key"] = _extract_section_key(map_df["geocode"])

    df_plot = map_df.merge(
        df_2023[["section_key", "renta_mediana_unidad_consumo"]],
        on="section_key",
        how="left",
    )

    plot = (
        ggplot(df_plot, aes(x="long", y="lat", group="group", fill="renta_mediana_unidad_consumo"))
        + geom_polygon(color="white", size=0.15)
        + coord_equal()
        + scale_fill_cmap(cmap_name="YlOrRd", na_value="#999999")
        + labs(
            title="Mediana de renta por unidad de consumo en Tenerife, 2023",
            subtitle="Por sección censal",
            fill="Euros (€)",
            caption="Las secciones en gris no tienen dato disponible."
        )
        + theme_bw()
        + theme(
            figure_size=(14, 10),
            plot_title=element_text(weight="bold", size=18),
            plot_subtitle=element_text(size=15, color="#666666"),
            legend_position="right",
            legend_title=element_text(size=15),
            legend_text=element_text(size=14),
            axis_title=element_blank(),
            axis_text=element_blank(),
            axis_ticks=element_blank(),
            panel_grid=element_blank(),
            panel_border=element_rect(fill=None, size=1),
            plot_caption=element_text(size=15)
        )
    )

    path = VIS_DIR / "graph_01_mapa_renta_seccional_2023.png"
    plot.save(path, width=14, height=10, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "año": MetadataValue.int(2023),
            "variable": MetadataValue.text("renta_mediana_unidad_consumo"),
            "secciones_con_dato": MetadataValue.int(int(df_2023["renta_mediana_unidad_consumo"].notna().sum())),
        }
    )


@asset(
    group_name="visualizacion",
    description="Mapa de cambio porcentual de la mediana de renta por unidad de consumo entre 2021 y 2023, por sección censal en Tenerife.",
)
def graph_02(df_filtro_tenerife: pd.DataFrame) -> MaterializeResult:
    df_2021 = (
        df_filtro_tenerife[df_filtro_tenerife["año"] == 2021][["section_key", "renta_mediana_unidad_consumo"]]
        .rename(columns={"renta_mediana_unidad_consumo": "renta_2021"})
    )
    df_2023 = (
        df_filtro_tenerife[df_filtro_tenerife["año"] == 2023][["section_key", "renta_mediana_unidad_consumo"]]
        .rename(columns={"renta_mediana_unidad_consumo": "renta_2023"})
    )

    df_cambio = df_2021.merge(df_2023, on="section_key", how="inner")
    df_cambio["cambio_pct"] = (
        (df_cambio["renta_2023"] - df_cambio["renta_2021"]) / df_cambio["renta_2021"]
    ) * 100
    df_cambio = df_cambio.dropna(subset=["cambio_pct"])

    map_df = _fortify_geojson_secciones_tenerife(GEOJSON_POR_año[2024])
    map_df["section_key"] = _extract_section_key(map_df["geocode"])

    df_plot = map_df.merge(df_cambio[["section_key", "cambio_pct"]], on="section_key", how="left")

    plot = (
        ggplot(df_plot, aes(x="long", y="lat", group="group", fill="cambio_pct"))
        + geom_polygon(color="white", size=0.15)
        + coord_equal()
        + scale_fill_gradient2(
            low="#B2182B",
            mid="#F9D677",
            high="#1A9850",
            midpoint=0,
            limits=(-40, 40),
            oob=squish,
            na_value="#999999",
            labels=lambda x: [f"{v:.1f}%" for v in x],
        )
        + labs(
            title="Cambio porcentual de la mediana de renta por unidad de consumo, 2021–2023",
            subtitle="Por sección censal en Tenerife",
            fill="Cambio (%)",
            caption="Las secciones en gris no tienen dato disponible."
        )
        + theme_bw()
        + theme(
            figure_size=(14, 10),
            plot_title=element_text(weight="bold", size=18),
            plot_subtitle=element_text(size=15, color="#666666"),
            legend_position="right",
            legend_title=element_text(size=15),
            legend_text=element_text(size=14),
            axis_title=element_blank(),
            axis_text=element_blank(),
            axis_ticks=element_blank(),
            panel_grid=element_blank(),
            panel_border=element_rect(fill=None, size=1),
            plot_caption=element_text(size=15)
        )
    )

    path = VIS_DIR / "graph_02_mapa_cambio_renta_2021_2023.png"
    plot.save(path, width=14, height=10, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "variable": MetadataValue.text("cambio_pct_renta_mediana_unidad_consumo"),
            "secciones_con_dato": MetadataValue.int(len(df_cambio)),
            "cambio_pct_mediano": MetadataValue.float(round(float(df_cambio["cambio_pct"].median()), 2)),
        }
    )


@asset(
    group_name="visualizacion",
    description="Barras horizontales ordenadas de municipios de Tenerife por mediana de renta por unidad de consumo (2023).",
)
def graph_03(df_filtro_tenerife: pd.DataFrame) -> MaterializeResult:
    df_2023 = df_filtro_tenerife[df_filtro_tenerife["año"] == 2023].dropna(subset=["renta_mediana_unidad_consumo"]).copy()

    df_mun = (
        df_2023.groupby("municipio")["renta_mediana_unidad_consumo"]
        .median()
        .reset_index()
        .sort_values("renta_mediana_unidad_consumo")
    )
    df_mun["municipio"] = pd.Categorical(
        df_mun["municipio"], categories=df_mun["municipio"].tolist(), ordered=True
    )
    df_mun["label"] = df_mun["renta_mediana_unidad_consumo"].apply(lambda v: f"{v:,.0f} €")
    mediana_insular = float(df_mun["renta_mediana_unidad_consumo"].median())

    plot = (
        ggplot(df_mun, aes(x="municipio", y="renta_mediana_unidad_consumo"))
        + geom_col(fill="#7EC8C8", width=0.75)
        + geom_text(aes(label="label"), ha="right", nudge_y=-600, size=9, color="#333333")
        + geom_hline(yintercept=mediana_insular, linetype="dashed", color="#555555", size=0.7)
        + coord_flip()
        + labs(
            title="Ranking municipal de renta por unidad de consumo en Tenerife",
            subtitle=f"Renta municipal calculada como mediana no ponderada de sus secciones censales · 2023 | Línea: mediana de los valores municipales ({mediana_insular:,.0f} €)",
            x="Municipio",
            y="Mediana de renta por unidad de consumo (€)",
        )
        + theme_bw()
        + theme(
            figure_size=(12, 10),
            plot_title=element_text(weight="bold", size=16),
            plot_subtitle=element_text(size=9, color="#666666"),
            axis_text_x=element_text(size=11),
            axis_text_y=element_text(size=11),
            axis_title=element_text(size=13),
            panel_grid_minor=element_blank(),
        )
    )

    path = VIS_DIR / "graph_03_ranking_municipal_renta_2023.png"
    plot.save(path, width=12, height=10, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "año": MetadataValue.int(2023),
            "n_municipios": MetadataValue.int(len(df_mun)),
            "mediana_municipal": MetadataValue.float(round(mediana_insular, 2)),
        }
    )


@asset(
    group_name="visualizacion",
    description="Scatter plot de renta mediana municipal vs ratio P90/P10 de desigualdad interna por municipio en Tenerife (2023).",
)
def graph_04(df_filtro_tenerife: pd.DataFrame) -> MaterializeResult:
    df_2023 = df_filtro_tenerife[df_filtro_tenerife["año"] == 2023].dropna(subset=["renta_mediana_unidad_consumo"]).copy()

    df_mediana = (
        df_2023.groupby("municipio")["renta_mediana_unidad_consumo"]
        .median()
        .reset_index()
        .rename(columns={"renta_mediana_unidad_consumo": "renta_mediana"})
    )

    q = (
        df_2023.groupby("municipio")["renta_mediana_unidad_consumo"]
        .quantile([0.1, 0.9])
        .unstack()
        .reset_index()
    )
    q.columns = ["municipio", "p10", "p90"]
    q["ratio_p90_p10"] = q["p90"] / q["p10"]

    df_mun = (
        df_mediana
        .merge(q[["municipio", "ratio_p90_p10"]], on="municipio")
        .dropna(subset=["ratio_p90_p10"])
    )

    mediana_x = float(df_mun["renta_mediana"].median())
    mediana_y = float(df_mun["ratio_p90_p10"].median())

    plot = (
        ggplot(df_mun, aes(x="renta_mediana", y="ratio_p90_p10"))
        + geom_vline(xintercept=mediana_x, linetype="dashed", color="#aaaaaa", size=0.7)
        + geom_hline(yintercept=mediana_y, linetype="dashed", color="#aaaaaa", size=0.7)
        + geom_point(color="#FD814E", size=5, alpha=0.85)
        + geom_text(aes(label="municipio"), size=9, nudge_y=0.02, ha="center", va="bottom")
        + labs(
            title="Nivel de renta y desigualdad interna por municipio en Tenerife, 2023",
            subtitle="Renta municipal calculada como mediana no ponderada de sus secciones censales. |  Líneas: mediana de los valores municipales",
            x="Mediana de renta por unidad de consumo (€)",
            y="Desigualdad interna (Ratio P90/P10)",
        )
        + theme_bw()
        + theme(
            figure_size=(14, 9),
            plot_title=element_text(weight="bold", size=16),
            plot_subtitle=element_text(size=12, color="#666666"),
            axis_text_x=element_text(size=11),
            axis_text_y=element_text(size=11),
            axis_title=element_text(size=13),
            panel_grid_minor=element_blank(),
        )
    )

    path = VIS_DIR / "graph_04_scatter_renta_vs_desigualdad_2023.png"
    plot.save(path, width=14, height=9, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "año": MetadataValue.int(2023),
            "mediana_insular_renta": MetadataValue.float(round(mediana_x, 2)),
            "mediana_insular_ratio_p90_p10": MetadataValue.float(round(mediana_y, 4)),
        }
    )


@asset(
    group_name="visualizacion",
    description="Mapa seccional con quintiles de renta mediana por unidad de consumo en Tenerife (2023).",
)
def graph_05(df_filtro_tenerife: pd.DataFrame) -> MaterializeResult:
    df_2023 = _calcular_quintiles(
        df_filtro_tenerife[df_filtro_tenerife["año"] == 2023].copy(),
        variable="renta_mediana_unidad_consumo",
    )

    map_df = _fortify_geojson_secciones_tenerife(GEOJSON_POR_año[2023])
    map_df["section_key"] = _extract_section_key(map_df["geocode"])

    df_plot = map_df.merge(df_2023[["section_key", "quintil"]], on="section_key", how="left")

    quintil_colors = {
        "Q1": "#E8D2CA",
        "Q2": "#CFA1AD",
        "Q3": "#B57C9F",
        "Q4": "#93558F",
        "Q5": "#2B203F",
    }

    plot = (
        ggplot(df_plot, aes(x="long", y="lat", group="group", fill="quintil"))
        + geom_polygon(color="white", size=0.15)
        + coord_equal()
        + scale_fill_manual(values=quintil_colors, 
                            breaks=["Q1", "Q2", "Q3", "Q4", "Q5"],
                            labels=["Q1 menor renta", "Q2", "Q3", "Q4", "Q5 mayor renta"],
                            na_value="#999999", na_translate=False)
        + labs(
            title="Quintiles seccionales de renta bruta en Tenerife, 2023",
            subtitle="Quintiles calculados con renta bruta media por persona",
            fill="Quintil de renta",
        )
        + theme_bw()
        + theme(
            figure_size=(14, 10),
            plot_title=element_text(weight="bold", size=18),
            plot_subtitle=element_text(size=15, color="#666666"),
            legend_position="right",
            legend_title=element_text(size=15),
            legend_text=element_text(size=14),
            axis_title=element_blank(),
            axis_text=element_blank(),
            axis_ticks=element_blank(),
            panel_grid=element_blank(),
            panel_border=element_rect(fill=None, size=1),
        )
    )

    path = VIS_DIR / "graph_05_mapa_quintiles_renta_2023.png"
    plot.save(path, width=14, height=10, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "año": MetadataValue.int(2023),
            "variable_quintil": MetadataValue.text("renta_mediana_unidad_consumo"),
            "n_secciones": MetadataValue.int(len(df_2023)),
        }
    )


@asset(
    group_name="visualizacion",
    description="Barras apiladas de composición de renta por quintil territorial y fuente de ingresos, facetado por año (2021–2023).",
)
def graph_06(df_filtro_tenerife: pd.DataFrame):
    pct_cols = [
        "pct_sueldos_salarios",
        "pct_pensiones",
        "pct_prestaciones_desempleo",
        "pct_otras_prestaciones",
        "pct_otros_ingresos",
    ]

    df = df_filtro_tenerife.dropna(subset=["renta_bruta_media_persona"] + pct_cols).copy()

    df = df.groupby("año", group_keys=False).apply(
        lambda g: _calcular_quintiles(g, variable="renta_bruta_media_persona")
    )

    df_agg = (
        df.groupby(["año", "quintil"])[pct_cols]
        .mean()
        .reset_index()
    )

    fuente_labels = {
        "pct_sueldos_salarios": "Sueldos y salarios",
        "pct_pensiones": "Pensiones",
        "pct_prestaciones_desempleo": "Prestaciones por desempleo",
        "pct_otras_prestaciones": "Otras prestaciones",
        "pct_otros_ingresos": "Otros ingresos",
    }
    fuente_colors = {
        "Sueldos y salarios": "#FD814E",
        "Pensiones": "#FCBC52",
        "Prestaciones por desempleo": "#A4D984",
        "Otras prestaciones": "#F26386",
        "Otros ingresos": "#F588AF",
    }

    df_long = (
        df_agg
        .melt(id_vars=["año", "quintil"], value_vars=pct_cols, var_name="fuente", value_name="porcentaje")
    )
    df_long["fuente"] = df_long["fuente"].map(fuente_labels)
    df_long["año"] = df_long["año"].astype(str)

    plot = (
        ggplot(df_long, aes(x="quintil", y="porcentaje", fill="fuente"))
        + geom_col(position="stack", width=0.75)
        + scale_y_continuous(labels=lambda x: [f"{v:.0f}%" for v in x])
        + scale_fill_manual(values=fuente_colors)
        + facet_wrap("~año", nrow=1)
        + labs(
            title="Composición de la renta bruta por quintil seccional en Tenerife, 2021-2023",
            subtitle="Promedio por fuente de ingresos · Quintiles calculados con renta bruta media por persona",
            x="Quintil de renta",
            y="Distribución media de la renta bruta (%)",
            fill="Fuente de ingresos",
        )
        + theme_bw()
        + theme(
            figure_size=(15, 7),
            plot_title=element_text(weight="bold", size=16),
            plot_subtitle=element_text(size=14, color="#666666"),
            axis_text_x=element_text(size=11),
            axis_text_y=element_text(size=11),
            axis_title=element_text(size=13),
            legend_position="bottom",
            legend_direction="horizontal",
            legend_title=element_text(size=12, weight="bold"),
            legend_text=element_text(size=11),
            strip_text=element_text(size=11),
            panel_grid_minor=element_blank(),
        )
    )

    path = VIS_DIR / "graph_06_composicion_renta_quintil_2021_2023.png"
    plot.save(path, width=15, height=7, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "años": MetadataValue.text("2021, 2022, 2023"),
            "variable_quintil": MetadataValue.text("renta_bruta_media_persona"),
        }
    )


@asset(
    group_name="visualizacion",
    description="Barras apiladas de composición sectorial del empleo por quintil seccional de renta, facetado por año (2021–2023).",
)
def graph_07(df_filtro_tenerife: pd.DataFrame) -> MaterializeResult:
    actividad_cols = {
        "actividad_no_consta": "No consta",
        "actividad_servicios": "Servicios",
        "actividad_industria": "Industria",
        "actividad_construccion": "Construcción",
        "actividad_agricultura_ganaderia_pesca": "Agricultura, ganadería y pesca",
    }

    df = df_filtro_tenerife.copy()

    df = (df.groupby("año", group_keys=False)
        .apply(lambda g: _calcular_quintiles(g, variable="renta_mediana_unidad_consumo"))
        .rename(columns={"quintil": "quintil_renta"})
    )

    df["total_actividad"] = df[list(actividad_cols.keys())].sum(axis=1)

    df_long = df.melt(
        id_vars=["año", "section_key", "municipio", "quintil_renta", "total_actividad"],
        value_vars=list(actividad_cols.keys()),
        var_name="sector_col",
        value_name="num_trabajadores",
    )

    df_long["sector_economico"] = df_long["sector_col"].map(actividad_cols)
    df_long["pct_trabajadores"] = df_long["num_trabajadores"] / df_long["total_actividad"] * 100

    df_resumen = (
        df_long
        .groupby(["año", "quintil_renta", "sector_economico"], as_index=False)["pct_trabajadores"]
        .mean()
        .rename(columns={"pct_trabajadores": "porcentaje_medio_trabajadores"})
    )

    df_resumen["año"] = df_resumen["año"].astype(str)

    sector_colors = {
        "Servicios": "#7EC8C8",
        "Construcción": "#FD814E",
        "Industria": "#B39DDB",
        "Agricultura, ganadería y pesca": "#A4D984",
        "No consta": "#FCBC52",
    }

    plot = (
        ggplot(df_resumen, aes(x="quintil_renta", y="porcentaje_medio_trabajadores", fill="sector_economico"))
        + geom_col(width=0.75, color="white", size=0.4)
        + scale_y_continuous(labels=lambda x: [f"{v:.0f}%" for v in x])
        + scale_fill_manual(values=sector_colors)
        + facet_wrap("~año", nrow=1)
        + labs(
            title="Composición sectorial del empleo por quintil seccional de renta en Tenerife, 2021-2023",
            subtitle="Promedio por sección censal · Quintiles calculados con renta bruta media por persona",
            x="Quintil de renta",
            y="Distribución media del empleo por sector (%)",
            fill="Sector económico",
        )
        + theme_bw()
        + theme(
            figure_size=(14, 6),
            plot_title=element_text(weight="bold", size=16),
            plot_subtitle=element_text(size=14, color="#666666"),
            axis_text_x=element_text(size=11),
            axis_text_y=element_text(size=11),
            axis_title=element_text(size=12),
            legend_position="bottom",
            legend_direction="horizontal",
            legend_title=element_text(size=12, weight="bold"),
            legend_text=element_text(size=11),
            strip_text=element_text(size=12),
            panel_grid_minor=element_blank(),
        )
    )

    path = VIS_DIR / "graph_07_composicion_sectorial_quintil_2021_2023.png"
    plot.save(path, width=14, height=6, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "años": MetadataValue.text("2021, 2022, 2023"),
            "variable_quintil": MetadataValue.text("renta_mediana_unidad_consumo"),
            "n_secciones": MetadataValue.int(df["section_key"].nunique()),
        }
    )


# ================================================================
# JOB Y SENSOR
# ================================================================

pipeline_proyecto_job = define_asset_job(
    name="pipeline_proyecto_job",
    selection=AssetSelection.all()
)

@sensor(job=pipeline_proyecto_job)
def sensor_cambios_datos_renta(context):
    archivos_vigilados = [
        DATA_DIR / "actividad-sc-3.csv",
        DATA_DIR / "distribucion-renta-ingresos.csv",
        DATA_DIR / "rentamedia-sc-3.csv",
    ]

    estado_anterior = json.loads(context.cursor) if context.cursor else {}
    estado_actual = {}

    for ruta in archivos_vigilados:
        ruta_str = str(ruta)

        if ruta.exists():
            estado_actual[ruta_str] = ruta.stat().st_mtime
        else:
            estado_actual[ruta_str] = None

    if estado_actual != estado_anterior:
        cursor_nuevo = json.dumps(estado_actual, ensure_ascii=False, sort_keys=True)
        context.update_cursor(cursor_nuevo)

        yield RunRequest(
            run_key=cursor_nuevo,
            run_config={},
        )
    else:
        yield SkipReason("No hubo cambios en los archivos de datos.")