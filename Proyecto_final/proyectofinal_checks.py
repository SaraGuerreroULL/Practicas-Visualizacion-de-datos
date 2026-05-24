import os
from pathlib import Path
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue, AssetIn

from Proyecto_final import proyectofinal_assets as assets


BASE_DIR = Path(__file__).resolve().parent
VIS_DIR = BASE_DIR / "visualizaciones"


# ================================================================
# FUNCIONES AUXILIARES PARA CHECKS
# ================================================================

def _columnas_md(df: pd.DataFrame) -> MetadataValue:
    """Devuelve nombres y tipos de columnas como tabla Markdown."""
    tabla = pd.DataFrame({
        "columna": df.columns,
        "tipo": [str(dtype) for dtype in df.dtypes],
    })
    return MetadataValue.md(tabla.to_markdown(index=False))


def _faltantes(df: pd.DataFrame, columnas: list[str]) -> list[str]:
    """Lista columnas esperadas que no aparecen en el DataFrame."""
    return [c for c in columnas if c not in df.columns]


def _nulos_claves(df: pd.DataFrame, claves: list[str]) -> int:
    """Cuenta nulos en columnas clave."""
    claves_existentes = [c for c in claves if c in df.columns]
    if not claves_existentes:
        return -1
    return int(df[claves_existentes].isna().sum().sum())


def _metadata_basica_df(df: pd.DataFrame) -> dict:
    """Metadata general reutilizable para DataFrames."""
    return {
        "filas": MetadataValue.int(int(len(df))),
        "columnas": MetadataValue.int(int(df.shape[1])),
        "nombres_columnas": _columnas_md(df),
    }


def _check_archivo_visualizacion(path: Path, min_bytes: int = 10_000) -> AssetCheckResult:
    """Verifica que una visualización exista y tenga un tamaño razonable."""
    existe = path.exists()
    size_bytes = path.stat().st_size if existe else 0
    passed = existe and size_bytes >= min_bytes

    return AssetCheckResult(
        passed=passed,
        metadata={
            "path": MetadataValue.path(path),
            "existe": MetadataValue.bool(existe),
            "size_bytes": MetadataValue.int(int(size_bytes)),
            "min_bytes_esperado": MetadataValue.int(min_bytes),
        },
    )


# ================================================================
# CAPA 1: CARGA
# ================================================================

@asset_check(
    asset=assets.raw_rentamedia,
    description=(
        "Verifica que el archivo bruto de renta media se haya cargado con filas, "
        "columnas esperadas y sin columnas completamente vacías."
    ),
)
def check_raw_rentamedia_estructura(raw_rentamedia: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = [
        "año",
        "municipio",
        "MEDIDAS_CODE",
        "MEDIDAS#es",
        "TERRITORIO_CODE",
        "OBS_VALUE",
    ]

    faltan = _faltantes(raw_rentamedia, columnas_esperadas)
    columnas_vacias = raw_rentamedia.columns[raw_rentamedia.isna().all()].tolist()

    passed = (
        len(raw_rentamedia) > 0
        and len(faltan) == 0
        and len(columnas_vacias) == 0
    )

    metadata = _metadata_basica_df(raw_rentamedia)
    metadata.update({
        "columnas_esperadas": MetadataValue.md("\n".join(f"- `{c}`" for c in columnas_esperadas)),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
        "columnas_completamente_vacias": MetadataValue.md("\n".join(f"- `{c}`" for c in columnas_vacias) or "Ninguna"),
        "municipios_raw": MetadataValue.int(int(raw_rentamedia["municipio"].nunique())) if "municipio" in raw_rentamedia.columns else MetadataValue.int(0),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=assets.raw_distribucion,
    description=(
        "Verifica que el archivo bruto de distribución de ingresos se haya cargado "
        "con las columnas necesarias para limpieza y transformación."
    ),
)
def check_raw_distribucion_estructura(raw_distribucion: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = [
        "año",
        "municipio",
        "MEDIDAS_CODE",
        "MEDIDAS#es",
        "TERRITORIO_CODE",
        "OBS_VALUE",
    ]

    faltan = _faltantes(raw_distribucion, columnas_esperadas)
    columnas_vacias = raw_distribucion.columns[raw_distribucion.isna().all()].tolist()

    passed = (
        len(raw_distribucion) > 0
        and len(faltan) == 0
        and len(columnas_vacias) == 0
    )

    metadata = _metadata_basica_df(raw_distribucion)
    metadata.update({
        "columnas_esperadas": MetadataValue.md("\n".join(f"- `{c}`" for c in columnas_esperadas)),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
        "columnas_completamente_vacias": MetadataValue.md("\n".join(f"- `{c}`" for c in columnas_vacias) or "Ninguna"),
        "municipios_raw": MetadataValue.int(int(raw_distribucion["municipio"].nunique())) if "municipio" in raw_distribucion.columns else MetadataValue.int(0),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=assets.raw_actividad,
    description=(
        "Verifica que el archivo bruto de actividad económica contenga las columnas "
        "necesarias para construir la base por año, sección, municipio y sector."
    ),
)
def check_raw_actividad_estructura(raw_actividad: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = [
        "Periodo",
        "municipio",
        "geocode",
        "Actividad económica",
        "num_casos",
    ]

    faltan = _faltantes(raw_actividad, columnas_esperadas)
    columnas_vacias = raw_actividad.columns[raw_actividad.isna().all()].tolist()

    passed = (
        len(raw_actividad) > 0
        and len(faltan) == 0
        and len(columnas_vacias) == 0
    )

    metadata = _metadata_basica_df(raw_actividad)
    metadata.update({
        "columnas_esperadas": MetadataValue.md("\n".join(f"- `{c}`" for c in columnas_esperadas)),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
        "columnas_completamente_vacias": MetadataValue.md("\n".join(f"- `{c}`" for c in columnas_vacias) or "Ninguna"),
        "municipios_raw": MetadataValue.int(int(raw_actividad["municipio"].nunique())) if "municipio" in raw_actividad.columns else MetadataValue.int(0),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


# ================================================================
# CAPA 2: LIMPIEZA
# ================================================================

@asset_check(
    asset=assets.df_rentamedia,
    additional_ins={"raw_rentamedia": AssetIn()},
    description=(
        "Verifica la limpieza de renta media: columnas esperadas, claves no nulas, "
        "valores numéricos y cambio en número de municipios antes/después."
    ),
)
def check_df_rentamedia_limpieza(
    raw_rentamedia: pd.DataFrame,
    df_rentamedia: pd.DataFrame,
) -> AssetCheckResult:
    columnas_esperadas = [
        "año",
        "medida_code",
        "medida",
        "territorio_code",
        "valor",
        "section_key",
        "municipio",
    ]

    faltan = _faltantes(df_rentamedia, columnas_esperadas)
    nulos_claves = _nulos_claves(df_rentamedia, ["año", "section_key", "municipio", "valor"])
    duplicados = int(df_rentamedia.duplicated().sum())

    municipios_raw = int(raw_rentamedia["municipio"].nunique()) if "municipio" in raw_rentamedia.columns else 0
    municipios_limpios = int(df_rentamedia["municipio"].nunique())

    años = sorted(df_rentamedia["año"].dropna().astype(int).unique().tolist())
    valores_no_numericos = int(pd.to_numeric(df_rentamedia["valor"], errors="coerce").isna().sum())

    passed = (
        len(df_rentamedia) > 0
        and len(faltan) == 0
        and nulos_claves == 0
        and duplicados == 0
        and valores_no_numericos == 0
        and set(años) == {2021, 2022, 2023}
    )

    metadata = _metadata_basica_df(df_rentamedia)
    metadata.update({
        "municipios_antes_limpieza": MetadataValue.int(municipios_raw),
        "municipios_despues_limpieza": MetadataValue.int(municipios_limpios),
        "años_detectados": MetadataValue.text(", ".join(map(str, años))),
        "nulos_en_claves": MetadataValue.int(nulos_claves),
        "duplicados": MetadataValue.int(duplicados),
        "valores_no_numericos": MetadataValue.int(valores_no_numericos),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=assets.df_distribucion,
    additional_ins={"raw_distribucion": AssetIn()},
    description=(
        "Verifica la limpieza de distribución de ingresos: filtro a provincia 38, "
        "conversión numérica, claves válidas y cambio de municipios antes/después."
    ),
)
def check_df_distribucion_limpieza(
    raw_distribucion: pd.DataFrame,
    df_distribucion: pd.DataFrame,
) -> AssetCheckResult:
    columnas_esperadas = [
        "año",
        "medida_code",
        "medida",
        "territorio_code",
        "valor",
        "section_key",
        "municipio",
    ]

    faltan = _faltantes(df_distribucion, columnas_esperadas)
    nulos_claves = _nulos_claves(df_distribucion, ["año", "section_key", "municipio", "valor"])
    duplicados = int(df_distribucion.duplicated().sum())

    municipios_raw = int(raw_distribucion["municipio"].nunique()) if "municipio" in raw_distribucion.columns else 0
    municipios_limpios = int(df_distribucion["municipio"].nunique())

    años = sorted(df_distribucion["año"].dropna().astype(int).unique().tolist())
    valores_no_numericos = int(pd.to_numeric(df_distribucion["valor"], errors="coerce").isna().sum())

    section_keys_fuera_provincia_38 = int(
        (~df_distribucion["section_key"].astype(str).str.startswith("38", na=False)).sum()
    )

    passed = (
        len(df_distribucion) > 0
        and len(faltan) == 0
        and nulos_claves == 0
        and duplicados == 0
        and valores_no_numericos == 0
        and section_keys_fuera_provincia_38 == 0
        and set(años) == {2021, 2022, 2023}
    )

    metadata = _metadata_basica_df(df_distribucion)
    metadata.update({
        "municipios_antes_limpieza": MetadataValue.int(municipios_raw),
        "municipios_despues_limpieza": MetadataValue.int(municipios_limpios),
        "años_detectados": MetadataValue.text(", ".join(map(str, años))),
        "nulos_en_claves": MetadataValue.int(nulos_claves),
        "duplicados": MetadataValue.int(duplicados),
        "valores_no_numericos": MetadataValue.int(valores_no_numericos),
        "section_keys_fuera_provincia_38": MetadataValue.int(section_keys_fuera_provincia_38),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=assets.df_actividad,
    additional_ins={"raw_actividad": AssetIn()},
    description=(
        "Verifica la limpieza de actividad económica: agregación de ambos sexos, "
        "claves válidas, sectores esperados y conteos no negativos."
    ),
)
def check_df_actividad_limpieza(
    raw_actividad: pd.DataFrame,
    df_actividad: pd.DataFrame,
) -> AssetCheckResult:
    columnas_esperadas = [
        "año",
        "section_key",
        "municipio",
        "actividad",
        "num_casos",
    ]

    actividades_esperadas = {
        "Agricultura, ganadería y pesca",
        "Construcción",
        "Industria",
        "No consta",
        "Servicios",
    }

    faltan = _faltantes(df_actividad, columnas_esperadas)
    nulos_claves = _nulos_claves(df_actividad, ["año", "section_key", "municipio", "actividad"])
    duplicados_clave = int(df_actividad.duplicated(subset=["año", "section_key", "municipio", "actividad"]).sum())

    municipios_raw = int(raw_actividad["municipio"].nunique()) if "municipio" in raw_actividad.columns else 0
    municipios_limpios = int(df_actividad["municipio"].nunique())

    años = sorted(df_actividad["año"].dropna().astype(int).unique().tolist())
    actividades_detectadas = set(df_actividad["actividad"].dropna().unique().tolist())
    actividades_faltantes = sorted(actividades_esperadas - actividades_detectadas)
    actividades_extra = sorted(actividades_detectadas - actividades_esperadas)

    casos_negativos = int((df_actividad["num_casos"] < 0).sum())

    passed = (
        len(df_actividad) > 0
        and len(faltan) == 0
        and nulos_claves == 0
        and duplicados_clave == 0
        and casos_negativos == 0
        and len(actividades_faltantes) == 0
        and set(años) == {2021, 2022, 2023}
    )

    metadata = _metadata_basica_df(df_actividad)
    metadata.update({
        "municipios_antes_limpieza": MetadataValue.int(municipios_raw),
        "municipios_despues_limpieza": MetadataValue.int(municipios_limpios),
        "años_detectados": MetadataValue.text(", ".join(map(str, años))),
        "nulos_en_claves": MetadataValue.int(nulos_claves),
        "duplicados_por_año_seccion_municipio_actividad": MetadataValue.int(duplicados_clave),
        "casos_negativos": MetadataValue.int(casos_negativos),
        "actividades_detectadas": MetadataValue.md("\n".join(f"- {a}" for a in sorted(actividades_detectadas))),
        "actividades_faltantes": MetadataValue.md("\n".join(f"- {a}" for a in actividades_faltantes) or "Ninguna"),
        "actividades_extra": MetadataValue.md("\n".join(f"- {a}" for a in actividades_extra) or "Ninguna"),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


# ================================================================
# CAPA 3: TRANSFORMACIÓN
# ================================================================

@asset_check(
    asset=assets.base_unificada,
    description=(
        "Verifica que la base unificada tenga la estructura esperada: "
        "2046 filas, 54 municipios, 3 años, columnas integradas de renta, "
        "distribución de ingresos y actividad económica."
    ),
)
def check_base_unificada_dimensiones(base_unificada: pd.DataFrame) -> AssetCheckResult:
    columnas_esperadas = [
        "año",
        "section_key",
        "municipio",
        "renta_bruta_media_hogar",
        "renta_bruta_media_persona",
        "renta_neta_media_hogar",
        "renta_neta_media_persona",
        "renta_media_unidad_consumo",
        "renta_mediana_unidad_consumo",
        "pct_otras_prestaciones",
        "pct_otros_ingresos",
        "pct_pensiones",
        "pct_prestaciones_desempleo",
        "pct_sueldos_salarios",
        "actividad_agricultura_ganaderia_pesca",
        "actividad_construccion",
        "actividad_industria",
        "actividad_no_consta",
        "actividad_servicios",
    ]

    filas_esperadas = 2046
    municipios_esperados = 54
    años_esperados = {2021, 2022, 2023}

    faltan = _faltantes(base_unificada, columnas_esperadas)
    filas = int(len(base_unificada))
    municipios = int(base_unificada["municipio"].nunique())
    años = sorted(base_unificada["año"].dropna().astype(int).unique().tolist())

    duplicados_clave = int(base_unificada.duplicated(subset=["año", "section_key", "municipio"]).sum())
    nulos_claves = _nulos_claves(base_unificada, ["año", "section_key", "municipio"])

    passed = (
        filas == filas_esperadas
        and municipios == municipios_esperados
        and set(años) == años_esperados
        and len(faltan) == 0
        and duplicados_clave == 0
        and nulos_claves == 0
    )

    metadata = _metadata_basica_df(base_unificada)
    metadata.update({
        "filas_esperadas": MetadataValue.int(filas_esperadas),
        "filas_observadas": MetadataValue.int(filas),
        "municipios_esperados": MetadataValue.int(municipios_esperados),
        "municipios_observados": MetadataValue.int(municipios),
        "años_esperados": MetadataValue.text(", ".join(map(str, sorted(años_esperados)))),
        "años_observados": MetadataValue.text(", ".join(map(str, años))),
        "duplicados_por_año_seccion_municipio": MetadataValue.int(duplicados_clave),
        "nulos_en_claves": MetadataValue.int(nulos_claves),
        "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan) or "Ninguna"),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


@asset_check(
    asset=assets.base_unificada,
    additional_ins={
        "df_rentamedia": AssetIn(),
        "df_distribucion": AssetIn(),
        "df_actividad": AssetIn(),
    },
    description=(
        "Verifica que las tres fuentes limpias compartan el mismo universo de municipios "
        "antes de integrarse en la base unificada."
    ),
)
def check_base_unificada_municipios_consistentes_entre_fuentes(
    df_rentamedia: pd.DataFrame,
    df_distribucion: pd.DataFrame,
    df_actividad: pd.DataFrame,
    base_unificada: pd.DataFrame,
) -> AssetCheckResult:
    municipios_renta = set(df_rentamedia["municipio"].dropna().unique())
    municipios_distribucion = set(df_distribucion["municipio"].dropna().unique())
    municipios_actividad = set(df_actividad["municipio"].dropna().unique())
    municipios_base = set(base_unificada["municipio"].dropna().unique())

    union_fuentes = municipios_renta | municipios_distribucion | municipios_actividad
    interseccion_fuentes = municipios_renta & municipios_distribucion & municipios_actividad

    faltan_en_renta = sorted(union_fuentes - municipios_renta)
    faltan_en_distribucion = sorted(union_fuentes - municipios_distribucion)
    faltan_en_actividad = sorted(union_fuentes - municipios_actividad)
    faltan_en_base = sorted(union_fuentes - municipios_base)

    passed = (
        municipios_renta == municipios_distribucion
        and municipios_renta == municipios_actividad
        and municipios_base == union_fuentes
    )

    return AssetCheckResult(
        passed=passed,
        metadata={
            "n_municipios_rentamedia": MetadataValue.int(len(municipios_renta)),
            "n_municipios_distribucion": MetadataValue.int(len(municipios_distribucion)),
            "n_municipios_actividad": MetadataValue.int(len(municipios_actividad)),
            "n_municipios_base_unificada": MetadataValue.int(len(municipios_base)),
            "n_municipios_interseccion_fuentes": MetadataValue.int(len(interseccion_fuentes)),
            "faltan_en_rentamedia": MetadataValue.md("\n".join(f"- {m}" for m in faltan_en_renta) or "Ninguno"),
            "faltan_en_distribucion": MetadataValue.md("\n".join(f"- {m}" for m in faltan_en_distribucion) or "Ninguno"),
            "faltan_en_actividad": MetadataValue.md("\n".join(f"- {m}" for m in faltan_en_actividad) or "Ninguno"),
            "faltan_en_base_unificada": MetadataValue.md("\n".join(f"- {m}" for m in faltan_en_base) or "Ninguno"),
        },
    )


@asset_check(
    asset=assets.base_unificada,
    description=(
        "Verifica que las variables porcentuales de fuentes de ingresos tengan valores "
        "razonables y que su suma por sección se aproxime al 100%."
    ),
)
def check_base_unificada_porcentajes_ingresos(base_unificada: pd.DataFrame) -> AssetCheckResult:
    pct_cols = [
        "pct_sueldos_salarios",
        "pct_pensiones",
        "pct_prestaciones_desempleo",
        "pct_otras_prestaciones",
        "pct_otros_ingresos",
    ]

    faltan = _faltantes(base_unificada, pct_cols)

    if faltan:
        return AssetCheckResult(
            passed=False,
            metadata={
                "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan)),
            },
        )

    df_pct = base_unificada[pct_cols].copy()
    valores_fuera_rango = int(((df_pct < 0) | (df_pct > 100)).sum().sum())

    filas_sin_distribucion = int(df_pct.isna().all(axis=1).sum())
    filas_distribucion_parcial = int(df_pct.isna().any(axis=1).sum() - filas_sin_distribucion)

    suma_pct = df_pct.sum(axis=1, min_count=len(pct_cols))

    filas_con_suma_calculable = suma_pct.notna()
    filas_suma_fuera_tolerancia = int(
        (~suma_pct[filas_con_suma_calculable].between(99, 101)).sum()
    )

    passed = (
        valores_fuera_rango == 0
        and filas_distribucion_parcial == 0
        and filas_suma_fuera_tolerancia == 0
    )

    return AssetCheckResult(
        passed=passed,
        metadata={
            "columnas_pct": MetadataValue.md("\n".join(f"- `{c}`" for c in pct_cols)),
            "valores_fuera_0_100": MetadataValue.int(valores_fuera_rango),
            "filas_suma_pct_fuera_99_101": MetadataValue.int(filas_suma_fuera_tolerancia),
            "filas_sin_distribucion": MetadataValue.int(filas_sin_distribucion),
            "filas_distribucion_parcial": MetadataValue.int(filas_distribucion_parcial),
            "filas_con_suma_calculable": MetadataValue.int(int(filas_con_suma_calculable.sum())),
            "suma_pct_min": MetadataValue.float(round(float(suma_pct.min()), 4)),
            "suma_pct_max": MetadataValue.float(round(float(suma_pct.max()), 4)),
            "suma_pct_media": MetadataValue.float(round(float(suma_pct.mean()), 4)),
        },
    )


@asset_check(
    asset=assets.base_unificada,
    description=(
        "Verifica que las variables de renta sean numéricas, no negativas "
        "y tengan cobertura suficiente en la base unificada."
    ),
)
def check_base_unificada_variables_renta(base_unificada: pd.DataFrame) -> AssetCheckResult:
    renta_cols = [
        "renta_bruta_media_hogar",
        "renta_bruta_media_persona",
        "renta_neta_media_hogar",
        "renta_neta_media_persona",
        "renta_media_unidad_consumo",
        "renta_mediana_unidad_consumo",
    ]

    faltan = _faltantes(base_unificada, renta_cols)

    if faltan:
        return AssetCheckResult(
            passed=False,
            metadata={
                "columnas_faltantes": MetadataValue.md("\n".join(f"- `{c}`" for c in faltan)),
            },
        )

    df_renta = base_unificada[renta_cols].copy()
    valores_negativos = int((df_renta < 0).sum().sum())
    nulos = int(df_renta.isna().sum().sum())

    cobertura_por_columna = (
        df_renta.notna()
        .mean()
        .mul(100)
        .round(2)
        .rename("cobertura_pct")
        .reset_index()
        .rename(columns={"index": "variable"})
    )

    cobertura_minima = float(cobertura_por_columna["cobertura_pct"].min())

    passed = (
        valores_negativos == 0
        and cobertura_minima >= 95
    )

    return AssetCheckResult(
        passed=passed,
        metadata={
            "variables_renta": MetadataValue.md("\n".join(f"- `{c}`" for c in renta_cols)),
            "valores_negativos": MetadataValue.int(valores_negativos),
            "nulos_variables_renta": MetadataValue.int(nulos),
            "cobertura_minima_pct": MetadataValue.float(round(cobertura_minima, 2)),
            "cobertura_por_variable": MetadataValue.md(cobertura_por_columna.to_markdown(index=False)),
        },
    )


@asset_check(
    asset=assets.df_filtro_tenerife,
    additional_ins={"base_unificada": AssetIn()},
    description=(
        "Verifica que el filtro de Tenerife conserve exactamente los 31 municipios "
        "de la isla y mantenga años y claves válidas."
    ),
)
def check_df_filtro_tenerife_municipios(
    base_unificada: pd.DataFrame,
    df_filtro_tenerife: pd.DataFrame,
) -> AssetCheckResult:
    municipios_tenerife_esperados = {
        "Adeje", "Arafo", "Arico", "Arona", "Buenavista del Norte", "Candelaria", "El Rosario", "El Sauzal", 
        "El Tanque", "Fasnia", "Garachico", "Granadilla de Abona", "La Guancha", "Guía de Isora", "Güímar", 
        "Icod de los Vinos", "San Cristóbal de La Laguna", "La Matanza de Acentejo", "La Orotava", 
        "La Victoria de Acentejo", "Los Realejos", "Los Silos", "Puerto de La Cruz", "San Juan de la Rambla", 
        "San Miguel de Abona", "Santa Cruz de Tenerife", "Santa Úrsula", "Santiago del Teide", "Tacoronte", 
        "Tegueste", "Vilaflor de Chasna"
    }

    municipios_observados = set(df_filtro_tenerife["municipio"].dropna().unique())
    municipios_faltantes = sorted(municipios_tenerife_esperados - municipios_observados)
    municipios_extra = sorted(municipios_observados - municipios_tenerife_esperados)

    n_municipios_base = int(base_unificada["municipio"].nunique())
    n_municipios_tenerife = int(df_filtro_tenerife["municipio"].nunique())
    años = sorted(df_filtro_tenerife["año"].dropna().astype(int).unique().tolist())

    duplicados_clave = int(df_filtro_tenerife.duplicated(subset=["año", "section_key", "municipio"]).sum())
    nulos_claves = _nulos_claves(df_filtro_tenerife, ["año", "section_key", "municipio"])

    passed = (
        n_municipios_tenerife == 31
        and municipios_observados == municipios_tenerife_esperados
        and set(años) == {2021, 2022, 2023}
        and duplicados_clave == 0
        and nulos_claves == 0
    )

    metadata = _metadata_basica_df(df_filtro_tenerife)
    metadata.update({
        "municipios_base_unificada": MetadataValue.int(n_municipios_base),
        "municipios_tenerife_esperados": MetadataValue.int(31),
        "municipios_tenerife_observados": MetadataValue.int(n_municipios_tenerife),
        "años_observados": MetadataValue.text(", ".join(map(str, años))),
        "municipios_faltantes": MetadataValue.md("\n".join(f"- {m}" for m in municipios_faltantes) or "Ninguno"),
        "municipios_extra": MetadataValue.md("\n".join(f"- {m}" for m in municipios_extra) or "Ninguno"),
        "duplicados_por_año_seccion_municipio": MetadataValue.int(duplicados_clave),
        "nulos_en_claves": MetadataValue.int(nulos_claves),
    })

    return AssetCheckResult(passed=passed, metadata=metadata)


# ================================================================
# CAPA 4: VISUALIZACIÓN
# ================================================================

@asset_check(
    asset=assets.graph_01,
    description=(
        "Verifica que se haya generado el mapa coroplético seccional de renta "
        "para Tenerife en 2023."
    ),
)
def check_graph_01_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_01_mapa_renta_seccional_2023.png"
    return _check_archivo_visualizacion(path)


@asset_check(
    asset=assets.graph_02,
    description=(
        "Verifica que se haya generado el mapa de cambio porcentual de renta "
        "entre 2021 y 2023."
    ),
)
def check_graph_02_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_02_mapa_cambio_renta_2021_2023.png"
    return _check_archivo_visualizacion(path)


@asset_check(
    asset=assets.graph_03,
    description=(
        "Verifica que se haya generado el ranking municipal de renta para 2023."
    ),
)
def check_graph_03_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_03_ranking_municipal_renta_2023.png"
    return _check_archivo_visualizacion(path)


@asset_check(
    asset=assets.graph_04,
    description=(
        "Verifica que se haya generado el scatter plot de renta municipal "
        "vs desigualdad interna."
    ),
)
def check_graph_04_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_04_scatter_renta_vs_desigualdad_2023.png"
    return _check_archivo_visualizacion(path)


@asset_check(
    asset=assets.graph_05,
    description=(
        "Verifica que se haya generado el mapa de quintiles seccionales de renta."
    ),
)
def check_graph_05_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_05_mapa_quintiles_renta_2023.png"
    return _check_archivo_visualizacion(path)


@asset_check(
    asset=assets.graph_06,
    description=(
        "Verifica que se haya generado el gráfico de composición de renta "
        "por quintil seccional."
    ),
)
def check_graph_06_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_06_composicion_renta_quintil_2021_2023.png"
    return _check_archivo_visualizacion(path)


@asset_check(
    asset=assets.graph_07,
    description=(
        "Verifica que se haya generado el gráfico de composición sectorial "
        "por quintil seccional de renta."
    ),
)
def check_graph_07_archivo_generado() -> AssetCheckResult:
    path = VIS_DIR / "graph_07_composicion_sectorial_quintil_2021_2023.png"
    return _check_archivo_visualizacion(path)