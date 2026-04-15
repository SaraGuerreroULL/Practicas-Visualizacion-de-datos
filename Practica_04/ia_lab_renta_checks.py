import os
from pathlib import Path
import pandas as pd
from dagster import asset_check, AssetCheckResult, MetadataValue

from Practica_04 import ia_lab_renta_assets


# ================================================================
# CAPA 1: CARGA
# ================================================================

@asset_check(
    asset=ia_lab_renta_assets.raw_renta,
    description="Verifica que el dataset bruto de renta se haya cargado correctamente y contenga registros."
)
def check_raw_renta_no_vacio(raw_renta: pd.DataFrame) -> AssetCheckResult:
    filas, columnas = raw_renta.shape
    passed = filas > 0 and columnas > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": MetadataValue.int(filas),
            "columnas": MetadataValue.int(columnas),
            "principio_gestalt": "Figura-fondo (Sin datos no hay visualización interpretable)",
            "mensaje": "El dataset base no puede estar vacío."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.raw_nombres,
    description="Verifica que la tabla bruta de municipios e islas se haya cargado correctamente y contenga registros."
)
def check_raw_nombres_no_vacio(raw_nombres: pd.DataFrame) -> AssetCheckResult:
    filas, columnas = raw_nombres.shape
    passed = filas > 0 and columnas > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": MetadataValue.int(filas),
            "columnas": MetadataValue.int(columnas),
            "principio_gestalt": "Conectividad (Sin tabla de apoyo no se pueden vincular territorios e islas)",
            "mensaje": "La tabla de nombres debe cargarse correctamente."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.raw_estudios,
    description="Verifica que el dataset bruto de estudios se haya cargado correctamente y contenga registros."
)
def check_raw_estudios_no_vacio(raw_estudios: pd.DataFrame) -> AssetCheckResult:
    filas, columnas = raw_estudios.shape
    passed = filas > 0 and columnas > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "filas": MetadataValue.int(filas),
            "columnas": MetadataValue.int(columnas),
            "principio_gestalt": "Figura-fondo (La fuente educativa debe existir antes del cruce)",
            "mensaje": "El dataset de estudios no puede estar vacío."
        }
    )


# ================================================================
# CAPA 2: LIMPIEZA
# ================================================================

@asset_check(
    asset=ia_lab_renta_assets.df_renta,
    description="Verifica que el dataset limpio de renta no contenga valores nulos en las variables esenciales del análisis."
)
def check_df_renta_sin_nulos_clave(df_renta: pd.DataFrame) -> AssetCheckResult:
    columnas = ["Territorio", "Código territorial", "Año", "Medida", "Valor (%)"]
    nulos = int(df_renta[columnas].isna().sum().sum())
    nulos_por_columna = df_renta[columnas].isna().sum()
    columnas_afectadas = nulos_por_columna[nulos_por_columna > 0].index.tolist()
    
    passed = nulos == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_detectados": MetadataValue.int(nulos),
            "columnas_revisadas": MetadataValue.text(", ".join(columnas)),
            "columnas_afectadas": MetadataValue.text(", ".join(columnas_afectadas)),
            "principio_gestalt": "Buena continuidad (Los cortes en variables clave rompen el análisis)",
            "mensaje": "No debe haber nulos en las variables esenciales."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_renta,
    description="Verifica que el dataset limpio de renta no contenga registros duplicados."
)
def check_df_renta_sin_duplicados(df_renta: pd.DataFrame) -> AssetCheckResult:
    mask_duplicados = df_renta.duplicated()
    duplicados = int(mask_duplicados.sum())
    filas_afectadas = df_renta[mask_duplicados].index.tolist()

    passed = duplicados == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "duplicados_detectados": MetadataValue.int(duplicados),
            "filas_afectadas": MetadataValue.json(filas_afectadas),
            "principio_gestalt": "Similitud (Los duplicados alteran el peso visual de las categorías)",
            "mensaje": "Los registros de renta deben ser únicos."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_renta,
    description="Verifica que los valores porcentuales de renta estén expresados en una escala válida entre 0 y 100."
)
def check_df_renta_porcentajes_validos(df_renta: pd.DataFrame) -> AssetCheckResult:
    fuera_rango = int(((df_renta["Valor (%)"] < 0) | (df_renta["Valor (%)"] > 100)).sum())
    valor_min = float(df_renta["Valor (%)"].min())
    valor_max = float(df_renta["Valor (%)"].max())
    passed = fuera_rango == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_de_rango": MetadataValue.int(fuera_rango),
            "rango_valores": MetadataValue.text(f"{valor_min} - {valor_max}"),
            "principio_gestalt": "Proporción (Una escala inválida distorsiona la comparación visual)",
            "mensaje": "Los porcentajes de renta deben estar entre 0 y 100."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_renta,
    description="Verifica que los nombres de territorios y medidas estén estandarizados para evitar fragmentación de categorías en las visualizaciones."
)
def check_df_renta_estandarizacion_texto(df_renta: pd.DataFrame) -> AssetCheckResult:
    originales_territorio = df_renta["Territorio"].nunique()
    normalizadas_territorio = (
        df_renta["Territorio"].astype(str).str.strip().str.title().nunique()
    )

    originales_medida = df_renta["Medida"].nunique()
    normalizadas_medida = (
        df_renta["Medida"].astype(str).str.strip().str.capitalize().nunique()
    )

    passed = (
        originales_territorio == normalizadas_territorio and
        originales_medida == normalizadas_medida
    )

    return AssetCheckResult(
        passed=passed,
        metadata={
            "territorios_detectados": MetadataValue.int(originales_territorio),
            "territorios_normalizados": MetadataValue.int(normalizadas_territorio),
            "medidas_detectadas": MetadataValue.int(originales_medida),
            "medidas_normalizadas": MetadataValue.int(normalizadas_medida),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "mensaje": "Nombres inconsistentes crearían categorías duplicadas."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_nombres,
    description="Verifica que cada código territorial identifique de forma única a un municipio o territorio."
)
def check_df_nombres_codigo_unico(df_nombres: pd.DataFrame) -> AssetCheckResult:
    duplicados = int(df_nombres["Código territorial"].duplicated().sum())
    mask_duplicados = df_nombres["Código territorial"].duplicated(keep=False)
    codigos_duplicados = (
        df_nombres.loc[mask_duplicados, "Código territorial"]
        .unique()
        .tolist())
    passed = duplicados == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "codigos_duplicados": MetadataValue.int(duplicados),
            "valores_duplicados": MetadataValue.json(codigos_duplicados),
            "principio_gestalt": "Conectividad (Cada territorio debe enlazar con una única referencia)",
            "mensaje": "Cada código territorial debe ser único."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_nombres,
    description="Verifica que el dataset limpio de nombres no contenga nulos en las columnas necesarias para el cruce territorial."
)
def check_df_nombres_sin_nulos_clave(df_nombres: pd.DataFrame) -> AssetCheckResult:
    columnas = ["Código territorial", "Municipio", "Isla"]
    nulos = int(df_nombres[columnas].isna().sum().sum())
    nulos_por_columna = df_nombres[columnas].isna().sum()
    columnas_afectadas = nulos_por_columna[nulos_por_columna > 0].index.tolist()

    passed = nulos == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_detectados": MetadataValue.int(nulos),
            "columnas_revisadas": MetadataValue.text(", ".join(columnas)),
            "columnas_afectadas": MetadataValue.text(", ".join(columnas_afectadas)),
            "principio_gestalt": "Conectividad (Sin claves completas el merge pierde correspondencias)",
            "mensaje": "No debe haber nulos en código, municipio o isla."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_nombres,
    description="Verifica que los nombres de isla estén estandarizados para evitar categorías duplicadas en el análisis territorial."
)
def check_df_nombres_estandarizacion_islas(df_nombres: pd.DataFrame) -> AssetCheckResult:
    originales = df_nombres["Isla"].nunique()
    normalizadas = df_nombres["Isla"].astype(str).str.strip().str.title().nunique()
    passed = originales == normalizadas

    return AssetCheckResult(
        passed=passed,
        metadata={
            "categorias_detectadas": MetadataValue.int(originales),
            "categorias_normalizadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (Evitar fragmentación visual)",
            "mensaje": "Nombres de isla inconsistentes duplican categorías."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_estudios,
    description="Verifica que el dataset limpio de estudios no contenga valores nulos en las variables clave del análisis educativo."
)
def check_df_estudios_sin_nulos_clave(df_estudios: pd.DataFrame) -> AssetCheckResult:
    columnas = ["Código territorial", "Municipio", "Año", "Nivel de estudios en curso", "Porcentaje (%)"]
    nulos = int(df_estudios[columnas].isna().sum().sum())
    nulos_por_columna = df_estudios[columnas].isna().sum()
    columnas_afectadas = nulos_por_columna[nulos_por_columna > 0].index.tolist()

    passed = nulos == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_detectados": MetadataValue.int(nulos),
            "columnas_revisadas": MetadataValue.text(", ".join(columnas)),
            "columnas_afectadas": MetadataValue.text(", ".join(columnas_afectadas)),
            "principio_gestalt": "Buena continuidad (Sin estas variables no hay lectura consistente)",
            "mensaje": "No debe haber nulos en las variables educativas clave."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_estudios,
    description="Verifica que los porcentajes educativos estén expresados en una escala válida entre 0 y 1."
)
def check_df_estudios_porcentajes_validos(df_estudios: pd.DataFrame) -> AssetCheckResult:
    fuera_rango = int(((df_estudios["Porcentaje (%)"] < 0) | (df_estudios["Porcentaje (%)"] > 1)).sum())
    valor_min = float(df_estudios["Porcentaje (%)"].min())
    valor_max = float(df_estudios["Porcentaje (%)"].max())
    passed = fuera_rango == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_de_rango": MetadataValue.int(fuera_rango),
            "rango_valores": MetadataValue.text(f"{valor_min} - {valor_max}"),
            "principio_gestalt": "Proporción (Los porcentajes educativos deben respetar su escala)",
            "mensaje": "Los porcentajes educativos deben estar entre 0 y 1."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_estudios,
    description="Verifica que la distribución de niveles educativos por municipio y año cierre aproximadamente al 100%."
)
def check_df_estudios_suma_por_municipio_ano(df_estudios: pd.DataFrame) -> AssetCheckResult:
    suma = (
        df_estudios.groupby(["Código territorial", "Año"])["Porcentaje (%)"]
        .sum()
        .round(4)
    )

    inconsistentes = int((suma.sub(1).abs() > 0.01).sum())
    passed = inconsistentes == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "grupos_inconsistentes": MetadataValue.int(inconsistentes),
            "grupos_revisados": MetadataValue.int(len(suma)),
            "principio_gestalt": "Cierre (La distribución educativa debe completar el 100%)",
            "mensaje": "Los porcentajes por municipio y año deben sumar aproximadamente 1."
        }
    )


# ================================================================
# CAPA 3: TRANSFORMACIÓN
# ================================================================

@asset_check(
    asset=ia_lab_renta_assets.df_renta_isla,
    description="Verifica que el cruce territorial haya asignado una isla a cada registro del dataset de renta enriquecido."
)
def check_df_renta_isla_sin_islas_nulas(df_renta_isla: pd.DataFrame) -> AssetCheckResult:
    nulos = int(df_renta_isla["Isla"].isna().sum())
    filas_afectadas = df_renta_isla[df_renta_isla["Isla"].isna()].index.tolist()
    territorios_afectados = []
    if "Territorio" in df_renta_isla.columns:
        territorios_afectados = (
            df_renta_isla.loc[df_renta_isla["Isla"].isna(), "Territorio"]
            .astype(str)
            .unique()
            .tolist()
        )

    passed = nulos == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "nulos_en_isla": MetadataValue.int(nulos),
            "filas_afectadas": MetadataValue.json(filas_afectadas),
            "territorios_afectados": MetadataValue.json(territorios_afectados),
            "principio_gestalt": "Conectividad (Cada observación debe quedar ligada a una isla)",
            "mensaje": "El merge no debe dejar territorios sin isla."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_renta_isla,
    description="Verifica que la clasificación territorial resultante solo contenga los tipos previstos por el pipeline."
)
def check_df_renta_isla_tipos_validos(df_renta_isla: pd.DataFrame) -> AssetCheckResult:
    esperados = {"Municipio", "Isla", "Provincia", "Comunidad"}
    detectados = set(df_renta_isla["Tipo"].dropna().unique())
    invalidos = sorted(detectados - esperados)
    passed = len(invalidos) == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "tipos_detectados": MetadataValue.text(", ".join(sorted(detectados))),
            "tipos_invalidos": MetadataValue.text(", ".join(invalidos) if invalidos else "Ninguno"),
            "principio_gestalt": "Similitud (La clasificación territorial debe ser coherente)",
            "mensaje": "Solo deben existir los tipos territoriales previstos."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.df_renta_edu,
    description="Verifica que el porcentaje de educación superior resultante del cruce esté expresado en una escala válida entre 0 y 100."
)
def check_df_renta_edu_porcentajes_validos(df_renta_edu: pd.DataFrame) -> AssetCheckResult:
    serie = df_renta_edu["Educación superior (%)"].dropna()
    fuera_rango = int(((serie < 0) | (serie > 100)).sum())
    valor_min = float(serie.min())
    valor_max = float(serie.max())
    passed = fuera_rango == 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "valores_fuera_de_rango": MetadataValue.int(fuera_rango),
            "rango_valores": MetadataValue.text(f"{valor_min} - {valor_max}"),
            "principio_gestalt": "Proporción (Las variables cuantitativas deben compartir escalas válidas)",
            "mensaje": "La educación superior debe estar entre 0 y 100."
        }
    )


# ================================================================
# CAPA 4: GENERACIÓN DE CÓDIGO
# ================================================================

@asset_check(
    asset=ia_lab_renta_assets.codigo_generado_ia_graph_01,
    description="Verifica que el código generado para el gráfico 1 contenga las estructuras básicas de un gráfico Plotnine."
)
def check_codigo_graph_01(codigo_generado_ia_graph_01):
    tiene_plot = "plot =" in codigo_generado_ia_graph_01
    usa_ggplot = "ggplot" in codigo_generado_ia_graph_01

    passed = tiene_plot and usa_ggplot

    return AssetCheckResult(
        passed=passed,
        metadata={
            "contiene_plot": MetadataValue.bool(tiene_plot),
            "contiene_ggplot": MetadataValue.bool(usa_ggplot),
            "mensaje": "El código debe definir 'plot =' y usar ggplot."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.codigo_generado_ia_graph_02,
    description="Verifica que el código generado para el gráfico 2 contenga las estructuras básicas de un gráfico Plotnine."
)
def check_codigo_graph_02(codigo_generado_ia_graph_02):
    tiene_plot = "plot =" in codigo_generado_ia_graph_02
    usa_ggplot = "ggplot" in codigo_generado_ia_graph_02

    passed = tiene_plot and usa_ggplot

    return AssetCheckResult(
        passed=passed,
        metadata={
            "contiene_plot": MetadataValue.bool(tiene_plot),
            "contiene_ggplot": MetadataValue.bool(usa_ggplot),
            "mensaje": "El código debe definir 'plot =' y usar ggplot."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.codigo_generado_ia_graph_03,
    description="Verifica que el código generado para el gráfico 3 contenga las estructuras básicas de un gráfico Plotnine."
)
def check_codigo_graph_03(codigo_generado_ia_graph_03):
    tiene_plot = "plot =" in codigo_generado_ia_graph_03
    usa_ggplot = "ggplot" in codigo_generado_ia_graph_03

    passed = tiene_plot and usa_ggplot

    return AssetCheckResult(
        passed=passed,
        metadata={
            "contiene_plot": MetadataValue.bool(tiene_plot),
            "contiene_ggplot": MetadataValue.bool(usa_ggplot),
            "mensaje": "El código debe definir 'plot =' y usar ggplot."
        }
    )


# ================================================================
# CAPA 5: VISUALIZACIÓN
# ================================================================

VIS_DIR = VIS_DIR = Path("C:/Users/Usuario/Documents/Aaa ULL/Visualización/Practicas-Visualizacion-de-datos/Practica_04/visualizaciones")

@asset_check(
    asset=ia_lab_renta_assets.graph_01,
    description="Verifica que el gráfico apilado de distribución de renta por isla se haya exportado correctamente."
)
def check_graph_01_exportado() -> AssetCheckResult:
    path = VIS_DIR / "ia_graph_01_distribucion_renta.png"
    existe = path.exists()
    tamano = path.stat().st_size if existe else 0

    return AssetCheckResult(
        passed=existe and tamano > 0,
        metadata={
            "archivo": MetadataValue.path(str(path)),
            "tamano_bytes": tamano,
            "principio_gestalt": "Figura-fondo (sin exportación no hay salida visual evaluable)",
            "mensaje": "El gráfico 1 debe guardarse correctamente."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.graph_02,
    description="Verifica que el gráfico de líneas sobre La Gomera se haya exportado correctamente."
)
def check_graph_02_exportado() -> AssetCheckResult:
    path = VIS_DIR / "ia_graph_02_linea_gomera_sueldos_salarios.png"
    existe = path.exists()
    tamano = path.stat().st_size if existe else 0

    return AssetCheckResult(
        passed=existe and tamano > 0,
        metadata={
            "archivo": MetadataValue.path(str(path)),
            "tamano_bytes": tamano,
            "principio_gestalt": "Buena continuidad (la serie temporal debe poder materializarse)",
            "mensaje": "El gráfico 2 debe guardarse correctamente."
        }
    )


@asset_check(
    asset=ia_lab_renta_assets.graph_03,
    description="Verifica que el gráfico de dispersión sobre educación superior y salarios en La Palma se haya exportado correctamente."
)
def check_graph_03_exportado() -> AssetCheckResult:
    path = VIS_DIR / "ia_graph_03_scatter_palma_salario_educación_2023.png"
    existe = path.exists()
    tamano = path.stat().st_size if existe else 0

    return AssetCheckResult(
        passed=existe and tamano > 0,
        metadata={
            "archivo": MetadataValue.path(str(path)),
            "tamano_bytes": tamano,
            "principio_gestalt": "Proporción (la relación entre variables debe quedar materializada)",
            "mensaje": "El gráfico 3 debe guardarse correctamente."
        }
    )