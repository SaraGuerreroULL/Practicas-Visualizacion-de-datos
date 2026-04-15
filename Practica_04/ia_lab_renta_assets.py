import requests
import re
import pandas as pd
import os
import json
import subprocess
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon
from plotnine import *

from dagster import (
    asset, Definitions, MaterializeResult, MetadataValue, Output,
    define_asset_job, AssetSelection, sensor, RunRequest, SkipReason
)

# ================================================================
# CAPA 1: CARGA
# ================================================================

@asset(group_name="carga",
       description="Carga el dataset de distribución de renta bruta media en Canarias.")
def raw_renta() -> pd.DataFrame:
    return pd.read_csv("C:\\Users\\Usuario\\Documents\\Aaa ULL\\Visualización\\Practicas-Visualizacion-de-datos\\Practica_02\\data\\distribucion-renta-canarias.csv")


@asset(group_name="carga",
       description="Carga el dataset de códigos y nombres de municipios e islas de Canarias.")
def raw_nombres() -> pd.DataFrame:
    return pd.read_csv("C:\\Users\\Usuario\\Documents\\Aaa ULL\\Visualización\\Practicas-Visualizacion-de-datos\\Practica_02\\data\\codislas.csv", sep=";", encoding="WINDOWS-1252")


@asset(group_name="carga",
       description="Carga el dataset de nivel de estudios por municipio.")
def raw_estudios() -> pd.DataFrame:
    return pd.read_excel("C:\\Users\\Usuario\\Documents\\Aaa ULL\\Visualización\\Practicas-Visualizacion-de-datos\\Practica_02\\data\\nivelestudios.xlsx")


@asset(group_name="carga",
       description="Carga el fichero GeoJSON de municipios para su uso en visualizaciones geográficas.")
def raw_municipios_geo() -> pd.DataFrame:
    return gpd.read_file("C:\\Users\\Usuario\\Documents\\Aaa ULL\\Visualización\\Practicas-Visualizacion-de-datos\\Practica_04\\data\\Municipios-2024_geo.json")


# ================================================================
# CAPA 2: LIMPIEZA
# ================================================================

@asset(group_name="limpieza",
       description="Limpia y tipifica el dataset de renta: renombra columnas, elimina nulos y duplicados.")
def df_renta(raw_renta: pd.DataFrame) -> pd.DataFrame:
    df = raw_renta.copy()

    # Eliminar columnas completamente vacías
    df = df.dropna(axis=1, how="all")

    # Eliminar columnas redundantes
    df = df.drop(columns=["TIME_PERIOD_CODE", "MEDIDAS_CODE"])

    # Renombrar columnas
    df = df.rename(columns={
        "TERRITORIO#es": "Territorio",
        "TERRITORIO_CODE": "Código territorial",
        "TIME_PERIOD#es": "Año",
        "MEDIDAS#es": "Medida",
        "OBS_VALUE": "Valor (%)"
    })

    # Eliminar espacios en blanco en columnas de texto
    df["Territorio"] = df["Territorio"].str.strip()
    df["Medida"] = df["Medida"].str.strip()
    df["Código territorial"] = df["Código territorial"].str.split("_").str[0]

    # Asegurar tipos correctos
    df["Año"] = pd.to_numeric(df["Año"], errors="coerce").astype(int)
    df["Valor (%)"] = pd.to_numeric(df["Valor (%)"], errors="coerce")

    # Eliminar filas con valores nulos importantes y duplicados
    df = df.dropna(subset=["Territorio", "Año", "Medida", "Valor (%)"])
    df = df.drop_duplicates()

    return df


@asset(group_name="limpieza",
       description="Limpia el dataset de nombres de municipios: construye el código territorial, corrige artículos y elimina duplicados.")
def df_nombres(raw_nombres: pd.DataFrame) -> pd.DataFrame:
    df = raw_nombres.copy()

    # Quitar filas con nulos en lo esencial
    df = df.dropna(subset=["CPRO", "CMUN", "ISLA"])

    # Asegurar que CPRO y CMUN son numéricos
    df["CPRO"] = pd.to_numeric(df["CPRO"], errors="coerce")
    df["CMUN"] = pd.to_numeric(df["CMUN"], errors="coerce")
    df = df.dropna(subset=["CPRO", "CMUN"])

    # Construir código territorial
    df["Código territorial"] = (
        df["CPRO"].astype("Int64").astype(str).str.zfill(2)
        + df["CMUN"].astype("Int64").astype(str).str.zfill(3)
    )

    df["NOMBRE"] = df["NOMBRE"].astype(str).str.strip()
    df["ISLA"] = df["ISLA"].astype(str).str.strip()

    # Arreglo automático de nombres (artículos al inicio)
    df["NOMBRE"] = df["NOMBRE"].str.replace(
        r"^(.+),\s*(La|El|Los|Las)$", r"\2 \1", regex=True
    )
    df["NOMBRE"] = df["NOMBRE"].str.replace(r"\s+", " ", regex=True)

    df["ISLA"] = df["ISLA"].str.replace(
        r"^(.+),\s*(La|El)$", r"\2 \1", regex=True
    )
    df["ISLA"] = df["ISLA"].str.replace(r"\s+", " ", regex=True)

    # Quitar duplicados y columnas innecesarias
    df = df.drop_duplicates(subset=["Código territorial"], keep="first").reset_index(drop=True)
    df = df.drop(columns=["CISLA", "DC", "CPRO", "CMUN"], errors="ignore")
    df = df.rename(columns={"NOMBRE": "Municipio", "ISLA": "Isla"})

    return df


@asset(group_name="limpieza",
       description="Limpia el dataset de estudios: extrae año, filtra por sexo total, agrupa por municipio y calcula porcentajes por nivel educativo.")
def df_estudios(raw_estudios: pd.DataFrame) -> pd.DataFrame:
    df = raw_estudios.copy()

    # Separar código municipal y nombre
    df[["Código territorial", "Municipio"]] = (
        df["Municipios de 500 habitantes o más"].str.split(" ", n=1, expand=True)
    )

    # Extraer año del periodo
    df["Año"] = df["Periodo"].dt.year

    # Filtrar Sexo = Total y excluir nivel educativo Total
    df = df[df["Sexo"] == "Total"]
    df = df[df["Nivel de estudios en curso"] != "Total"]

    # Agrupar eliminando Nacionalidad
    df = (
        df.groupby(
            ["Código territorial", "Municipio", "Año", "Nivel de estudios en curso"],
            as_index=False
        )["Total"].sum()
    )

    # Calcular porcentaje por municipio y año
    df["Porcentaje (%)"] = (
        df["Total"] /
        df.groupby(["Código territorial", "Año"])["Total"].transform("sum")
    )

    df = df.sort_values(["Municipio", "Año"])

    return df


# ================================================================
# CAPA 3: TRANSFORMACIÓN / MERGE
# ================================================================

@asset(group_name="transformacion",
       description="Enriquece df_renta con la isla y el tipo de territorio (Municipio, Isla, Provincia, Comunidad) mediante un merge con df_nombres.")
def df_renta_isla(df_renta: pd.DataFrame, df_nombres: pd.DataFrame) -> pd.DataFrame:
    # Merge para traer la isla a df_renta
    df = df_renta.merge(df_nombres, on="Código territorial", how="left")

    # Para territorios agregados, Isla = Territorio
    df["Isla"] = df["Isla"].fillna(df["Territorio"])

    # Columna Tipo para diferenciar municipios, islas, provincias y comunidad autónoma
    df["Tipo"] = "Municipio"
    df.loc[df["Territorio"] == "Canarias", "Tipo"] = "Comunidad"
    df.loc[
        df["Territorio"].isin(["Las Palmas", "Santa Cruz de Tenerife"]), "Tipo"
    ] = "Provincia"
    df.loc[
        (df["Isla"] == df["Territorio"]) & (df["Tipo"] == "Municipio"), "Tipo"
    ] = "Isla"

    return df[["Territorio", "Isla", "Tipo", "Código territorial", "Año", "Medida", "Valor (%)"]]


@asset(group_name="transformacion",
       description="Cruza df_renta_isla con el porcentaje de educación superior por municipio y año, generando el dataset base para análisis socioeconómico.")
def df_renta_edu(df_renta_isla: pd.DataFrame, df_estudios: pd.DataFrame) -> pd.DataFrame:
    # Extraer solo Educación superior
    df_superior = df_estudios[
        df_estudios["Nivel de estudios en curso"] == "Educación superior"
    ][["Código territorial", "Año", "Porcentaje (%)"]].copy()

    df_superior["Educación superior (%)"] = (df_superior["Porcentaje (%)"] * 100).round(1)
    df_superior = df_superior.drop(columns=["Porcentaje (%)"])

    # Merge para traer el porcentaje de educación superior a df_renta_isla
    return df_renta_isla.merge(df_superior, on=["Código territorial", "Año"], how="left")


@asset(group_name="transformacion",
       description="Une el GeoJSON municipal con el dataset de renta mediante el código territorial para generar la base cartográfica de rentas.")
def df_renta_mapa(raw_municipios_geo: pd.DataFrame, df_renta: pd.DataFrame) -> pd.DataFrame:
    # Normalizar el código territorial del GeoJSON
    raw_municipios_geo["Código territorial"] = raw_municipios_geo["geocode"].astype(str).str.zfill(5)

    # Conservar solo lo necesario para el mapa
    columnas_mapa = ["Código territorial", "geometry"]
    gdf = raw_municipios_geo[columnas_mapa]

    # Normalizar el código territorial de renta por seguridad
    df_renta["Código territorial"] = df_renta["Código territorial"].astype(str).str.zfill(5)

    # Merge: solo quedarán los municipios con geometría disponible
    gdf_renta = gdf.merge(df_renta, on="Código territorial", how="inner")

    return gdf_renta


# ================================================================
# CAPA 4: GENERACIÓN DE CÓDIGO
# ================================================================

@asset(
    group_name="generacion_codigo",
    description="Construye el prompt para que la IA genere el bloque plotnine del gráfico 01."
)
def template_ia_graph_01(df_renta: pd.DataFrame):
    columnas = ", ".join(df_renta.columns)

    orden_islas = [
        "Canarias", "El Hierro", "La Palma", "La Gomera",
        "Tenerife", "Gran Canaria", "Fuerteventura", "Lanzarote",
    ]

    orden_medidas = [
        "Sueldos y salarios", "Pensiones", "Prestaciones por desempleo",
        "Otras prestaciones", "Otros ingresos",
    ]

    paleta = [
        "#FD814E", "#FCBC52", "#A4D984", "#F26386",
        "#F588AF", "#7EC8C8", "#B39DDB", "#90CAF9"
    ]

    template_tecnico = """
        # Devuelve exclusivamente el bloque de código que define la variable plot.
        # No definas funciones.
        # No hagas imports.
        # No leas archivos.
        # No filtres ni transformes el dataframe.
        # Usa exactamente esta estructura general:

        plot = (
            ggplot(df, aes(...))
            + ...
        )
        """

    system_content = (
        "Eres un experto en gramática de gráficos y Plotnine. "
        "Tu tarea es traducir una especificación técnica en un bloque de código Python ejecutable. "
        "Debes devolver exclusivamente código Python, sin explicaciones, sin markdown y sin bloques ```python. "
        "No hagas imports. "
        "No definas funciones. "
        "No leas archivos. "
        "No guardes imágenes. "
        "No imprimas nada. "
        "No transformes el dataframe. "
        "Debes respetar exactamente el estilo indicado. "
        f"Usa esta plantilla:\n{template_tecnico}"
    )

    user_content = f"""
        Genera el código de un gráfico usando Plotnine a partir de un DataFrame ya preparado llamado df.

        Columnas disponibles: {columnas}

        El dataframe ya está filtrado y ordenado correctamente, así que no debes modificarlo ni transformarlo.

        Queremos representar la distribución de la renta bruta media según fuente de ingresos en Canarias entre 2015 y 2023.

        Descripción del gráfico:
        - Es un gráfico de barras apiladas donde cada barra representa un año.
        - El eje X debe mostrar los años como categorías (x="factor(Año)").
        - El eje Y representa el porcentaje de la renta ("Valor (%)").
        - Cada barra se descompone por tipo de ingreso ("Medida"), usando colores.

        - El gráfico debe dividirse en múltiples paneles, uno por territorio, organizados en 4 columnas.

        Escalas y formato:
        - El eje Y debe ir de 0 a 101.
        - Las etiquetas del eje Y deben mostrarse como porcentajes enteros (ejemplo: 25%).
        - Usa una paleta de colores fija en este orden exacto:
        ["#FD814E", "#FCBC52", "#A4D984", "#F26386", "#F588AF"]

        Etiquetas:
        - Título: "Distribución de la renta bruta media según fuente de ingresos en Canarias (2015-2023)"
        - Eje X: "Año"
        - Eje Y: "Peso en la renta bruta media (%)"
        - La leyenda no debe tener título.

        Estilo:
        - Usa siempre theme_bw().
        - El título debe aparecer en negrita.
        - El eje X es categórico, así que rota las etiquetas 45 grados y alínealas a la derecha.
        - La leyenda debe colocarse en la parte inferior y en disposición horizontal.
        - Usa un tamaño de figura amplio (aproximadamente 16x8).
        - Mantén tamaños de texto legibles (alrededor de 10–13 para ejes y leyenda).

        Importante:
        - Usa barras apiladas (geom_col con posición "stack").
        - Usa facet por territorio.
        - Usa scale_fill_manual con la paleta indicada.
        - Devuelve únicamente el bloque de código que define la variable plot.
        - No escribas funciones, ni imports, ni comentarios, ni texto adicional.
        """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset(
    group_name="generacion_codigo",
    description="Envía el prompt del gráfico 01 al servicio de IA y limpia el código devuelto."
)
def codigo_generado_ia_graph_01(context, template_ia_graph_01):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-1234"
    }

    try:
        response = requests.post(url, json=template_ia_graph_01, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json["choices"][0]["message"]["content"]

        match = re.search(r"```python\s*(.*?)\s*```", codigo_raw, re.DOTALL)

        if match:
            codigo_final = match.group(1).strip()
        else:
            lineas = codigo_raw.split("\n")
            lineas_validas = []

            for l in lineas:
                texto = l.strip()
                if not texto.startswith("```"):
                    lineas_validas.append(l)

            codigo_final = "\n".join(lineas_validas).strip()

        if "plot =" not in codigo_final:
            raise ValueError("La IA no devolvió un bloque válido que defina la variable 'plot'.")

        return Output(
            value=codigo_final,
            metadata={
                "codigo_generado": MetadataValue.md(f"```python\n{codigo_final}\n```"),
                "lineas_codigo": MetadataValue.int(len(codigo_final.splitlines()))
            }
        )

    except Exception as e:
        context.log.error(f"Error al generar el código del graph_01: {e}")
        raise


@asset(
    group_name="generacion_codigo",
    description="Construye el prompt para que la IA genere el bloque plotnine del gráfico 02."
)
def template_ia_graph_02(df_renta_isla: pd.DataFrame):
    columnas = ", ".join(df_renta_isla.columns)

    paleta = [
        "#FD814E", "#FCBC52", "#A4D984", "#F26386",
        "#F588AF", "#7EC8C8", "#B39DDB", "#90CAF9"
    ]

    template_tecnico = """
        # Devuelve exclusivamente el bloque de código que define la variable plot.
        # No definas funciones.
        # No hagas imports.
        # No leas archivos.
        # No filtres ni transformes el dataframe.
        # Usa exactamente esta estructura general:

        plot = (
            ggplot(df, aes(...))
            + ...
        )
        """

    system_content = (
        "Eres un experto en gramática de gráficos y Plotnine. "
        "Tu tarea es traducir una especificación técnica en un bloque de código Python ejecutable. "
        "Debes devolver exclusivamente código Python, sin explicaciones, sin markdown y sin bloques ```python. "
        "No hagas imports. "
        "No definas funciones. "
        "No leas archivos. "
        "No guardes imágenes. "
        "No imprimas nada. "
        "No transformes el dataframe. "
        "Debes respetar exactamente el estilo indicado. "
        f"Usa esta plantilla:\n{template_tecnico}"
    )

    user_content = f"""
        Genera el código de un gráfico usando Plotnine a partir de un DataFrame ya preparado llamado df.

        Columnas disponibles: {columnas}

        El dataframe ya está filtrado y ordenado correctamente, así que no debes modificarlo ni transformarlo.

        Queremos representar la evolución del peso de los sueldos y salarios en la renta bruta media por persona
        para los municipios de La Gomera entre 2015 y 2023.

        Descripción del gráfico:
        - Es un gráfico de líneas. El linetype debe mapearse a "Linea".
        - El eje X representa el año.
        - El eje Y representa el porcentaje de la renta ("Valor (%)").
        - Cada territorio aparece como una línea distinta y también como una serie de puntos.
        - El color representa el territorio.
        - Para municipio usa línea solida, para isla usa línea discontinua. Usa scale_linetype_manual.
        - No uses enteros en scale_linetype_manual, usa asignación con diccionario.

        Escalas y formato:
        - El eje X debe mostrar todos los años disponibles.
        - Usa esta paleta de colores en este orden exacto:
        {paleta}. Usa scale_color_manual(values=[...]) con la paleta indicada.

        Etiquetas:
        - Título: "Evolución del peso de sueldos y salarios en la renta bruta media por persona"
        - Subtítulo: "Municipios de La Gomera (2015-2023)"
        - Eje X: "Año"
        - Eje Y: "Peso en la renta bruta media (%)"
        - La leyenda del color debe llamarse "Territorio"
        - La leyenda del tipo de línea no debe tener título

        Estilo:
        - Usa siempre theme_bw().
        - El título debe aparecer en negrita.
        - El subtítulo debe aparecer en gris.
        - La leyenda debe colocarse a la derecha.

        Importante:
        - Usa líneas y puntos.
        - Usa una línea continua para los municipios y una línea discontinua para la isla.
        - Usa scale_linetype_manual para definir esos tipos de línea.
        - Usa scale_color_manual con la paleta indicada.
        - Devuelve únicamente el bloque de código que define la variable plot.
        - No escribas funciones, ni imports, ni comentarios, ni texto adicional.
        """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset(
    group_name="generacion_codigo",
    description="Envía el prompt del gráfico 02 al servicio de IA y limpia el código devuelto."
)
def codigo_generado_ia_graph_02(context, template_ia_graph_02):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-1234"
    }

    try:
        response = requests.post(url, json=template_ia_graph_02, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json["choices"][0]["message"]["content"]

        match = re.search(r"```python\s*(.*?)\s*```", codigo_raw, re.DOTALL)

        if match:
            codigo_final = match.group(1).strip()
        else:
            lineas = codigo_raw.split("\n")
            lineas_validas = []

            for l in lineas:
                texto = l.strip()
                if not texto.startswith("```"):
                    lineas_validas.append(l)

            codigo_final = "\n".join(lineas_validas).strip()

        if "plot =" not in codigo_final:
            raise ValueError("La IA no devolvió un bloque válido que defina la variable 'plot'.")

        return Output(
            value=codigo_final,
            metadata={
                "codigo_generado": MetadataValue.md(f"```python\n{codigo_final}\n```"),
                "lineas_codigo": MetadataValue.int(len(codigo_final.splitlines()))
            }
        )

    except Exception as e:
        context.log.error(f"Error al generar el código del graph_02: {e}")
        raise


@asset(
    group_name="generacion_codigo",
    #group_name="Generacion_de_codigo_con_IA",
    description="Construye el prompt para que la IA genere el bloque plotnine del gráfico 03."
)
def template_ia_graph_03(df_renta_edu: pd.DataFrame):
    columnas = ", ".join(df_renta_edu.columns)

    template_tecnico = """
        # Devuelve exclusivamente el bloque de código que define la variable plot.
        # No definas funciones.
        # No hagas imports.
        # No leas archivos.
        # No filtres ni transformes el dataframe.
        # Usa exactamente esta estructura general:

        plot = (
            ggplot(df, aes(...))
            + ...
        )
        """

    system_content = (
        "Eres un experto en gramática de gráficos y Plotnine. "
        "Tu tarea es traducir una especificación técnica en un bloque de código Python ejecutable. "
        "Debes devolver exclusivamente código Python, sin explicaciones, sin markdown y sin bloques ```python. "
        "Usa la sintaxis de Plotnine en Python correctamente."
        "No hagas imports. "
        "No definas funciones."
        "No leas archivos. "
        "No guardes imágenes. "
        "No imprimas nada. "
        "No transformes el dataframe. "
        "Debes respetar exactamente el estilo indicado. "
        f"Usa esta plantilla:\n{template_tecnico}"
    )

    user_content = f"""
        Genera el código de un gráfico usando Plotnine a partir de un DataFrame ya preparado llamado df.

        Columnas disponibles: {columnas}

        El dataframe ya está filtrado y ordenado correctamente, así que no debes modificarlo ni transformarlo.

        Queremos analizar la relación entre el nivel de educación superior y el peso de los sueldos y salarios
        en la renta bruta media de los municipios de La Palma en 2023.

        Descripción del gráfico:
        - Es un gráfico de dispersión.
        - El eje X representa 'Educación superior (%)'.
        - El eje Y representa 'Valor (%)'.
        - Cada punto representa un municipio.
        - Cada punto debe llevar una etiqueta con el nombre del municipio, usando la columna 'Territorio'.
        - Oculta la leyenda.
        - Debe incluir una línea de tendencia lineal con suavizado.

        Escalas y formato:
        - El eje X debe mostrarse como porcentaje entero.
        - El eje Y debe mostrarse como porcentaje entero.
        - No pongas limites fijos en los ejes, pero asegúrate de que las etiquetas sean legibles y no se amontonen.
        - La línea de tendencia debe ser color #b3b3b3.
        - El área de confianza de la regresión debe ser gris muy claro.
        - Los puntos deben tener color "#FD814E".
        - Las etiquetas de texto deben ir en gris oscuro.

        Etiquetas:
        - Título: "Relación entre educación superior y peso de salarios en la renta"
        - Subtítulo: "Municipios de La Palma (2023)"
        - Eje X: "Proporción de población con educación superior (%)"
        - Eje Y: "Porcentaje de sueldos y salarios en la renta bruta media (%)"

        Estilo:
        - Usa siempre theme_bw().
        - El título debe aparecer en negrita.
        - El subtítulo debe aparecer en gris.
        - Usa un tamaño de figura aproximadamente 8x6.
        - Mantén tamaños de texto legibles.

        Importante:
        - Usa exactamente estos nombres de columnas, sin cambiar NADA:
            x = 'Educación superior (%)'
            y = 'Valor (%)'
            label = 'Territorio'
        - Usa geom_point para los puntos.
        - Usa geom_smooth(method="lm") para la recta de tendencia.
        - Usa geom_text para etiquetar los municipios.
        - Desplaza ligeramente las etiquetas en vertical para que no queden encima del punto.
        - Devuelve únicamente el bloque de código que define la variable plot.
        - No escribas funciones, ni imports, ni comentarios, ni texto adicional.
        """

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset(
    group_name="generacion_codigo",
    #group_name="Generacion_de_codigo_con_IA",
    description="Envía el prompt del gráfico 03 al servicio de IA y limpia el código devuelto."
)
def codigo_generado_ia_graph_03(context, template_ia_graph_03):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-1234"
    }

    try:
        response = requests.post(url, json=template_ia_graph_03, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json["choices"][0]["message"]["content"]

        match = re.search(r"```python\s*(.*?)\s*```", codigo_raw, re.DOTALL)

        if match:
            codigo_final = match.group(1).strip()
        else:
            lineas = codigo_raw.split("\n")
            lineas_validas = []

            for l in lineas:
                texto = l.strip()
                if not texto.startswith("```"):
                    lineas_validas.append(l)

            codigo_final = "\n".join(lineas_validas).strip()

        # --- Limpieza de nombres de columnas mal generados ---
        reemplazos_columnas = {
            "Educación superior  (%)": "Educación superior (%)",
            "Valor  (%)": "Valor (%)"
        }

        if "plot =" not in codigo_final:
            raise ValueError("La IA no devolvió un bloque válido que defina la variable 'plot'.")

        for mal, bien in reemplazos_columnas.items():
            codigo_final = codigo_final.replace(mal, bien)

        return Output(
            value=codigo_final,
            metadata={
                "codigo_generado": MetadataValue.md(f"```python\n{codigo_final}\n```")
            }
        )

    except Exception as e:
        context.log.error(f"Error al generar el código del graph_03: {e}")
        raise


# ================================================================
# CAPA 5: VISUALIZACIÓN
# ================================================================

@asset(
    group_name="visualizacion",
    description=(
        "Genera el gráfico 01 a partir de código Plotnine producido por IA. "
        "Genera un gráfico de barras apiladas con la distribución de la renta bruta media por fuente de ingresos, con facet por isla (2015-2023)."
    )
)
def graph_01(df_renta: pd.DataFrame, codigo_generado_ia_graph_01: str) -> MaterializeResult:
    import subprocess

    orden_islas = [
        "Canarias", "El Hierro", "La Palma", "La Gomera",
        "Tenerife", "Gran Canaria", "Fuerteventura", "Lanzarote",
    ]

    orden_medidas = [
        "Sueldos y salarios", "Pensiones", "Prestaciones por desempleo",
        "Otras prestaciones", "Otros ingresos",
    ]

    df = df_renta[df_renta["Territorio"].isin(orden_islas)].copy()
    df["Territorio"] = pd.Categorical(df["Territorio"], categories=orden_islas, ordered=True)
    df["Medida"] = pd.Categorical(df["Medida"], categories=orden_medidas, ordered=True)
    df = df.sort_values(["Territorio", "Medida", "Año"])

    entorno_ejecucion = globals().copy()
    entorno_ejecucion["pd"] = pd
    entorno_ejecucion["df"] = df

    try:
        exec(codigo_generado_ia_graph_01, entorno_ejecucion)

        if "plot" not in entorno_ejecucion:
            raise ValueError("No se encontró la variable 'plot' tras ejecutar el código generado.")

        plot = entorno_ejecucion["plot"]

        path = "Practica_04/visualizaciones/ia_graph_01_distribucion_renta.png"
        plot.save(path, width=16, height=8, dpi=300)

        # Subida automática a GitHub
        subprocess.run(["git", "add", path], check=False)
        subprocess.run(
            ["git", "commit", "-m", "Actualización automática de ia_graph_01_distribucion_renta.png"],
            check=False
        )
        subprocess.run(["git", "push"], check=False)

        return MaterializeResult(
            metadata={
                "url_publica": MetadataValue.url("https://saraguerreroull.github.io/Practicas-Visualizacion-de-datos/Practica_04/visualizaciones/ia_graph_01_distribucion_renta.png"),
                "path": MetadataValue.path(path),
                "años": MetadataValue.text("2015-2023"),
                "territorios": MetadataValue.int(len(orden_islas)),
                "medidas": MetadataValue.text(", ".join(orden_medidas)),
                "codigo_generado": MetadataValue.md(f"```python\n{codigo_generado_ia_graph_01}\n```")
            }
        )

    except Exception as e:
        raise RuntimeError(f"Error al renderizar graph_01 con código IA: {e}")


@asset(
    group_name="visualizacion",
    description=(
        "Genera el gráfico 02 a partir de código Plotnine producido por IA. "
        "Genera un gráfico de líneas con la evolución del peso de sueldos y salarios "
        "en la renta bruta media para los municipios de La Gomera (2015-2023)."
    )
)
def graph_02(df_renta_isla: pd.DataFrame, codigo_generado_ia_graph_02: str) -> MaterializeResult:
    import subprocess

    df = df_renta_isla[
        (df_renta_isla["Isla"] == "La Gomera") &
        (df_renta_isla["Medida"] == "Sueldos y salarios")
    ].copy()

    df["Linea"] = df["Tipo"].apply(
        lambda x: "Isla" if x == "Isla" else "Municipio"
    )

    entorno_ejecucion = globals().copy()
    entorno_ejecucion["pd"] = pd
    entorno_ejecucion["df"] = df

    try:
        exec(codigo_generado_ia_graph_02, entorno_ejecucion)

        if "plot" not in entorno_ejecucion:
            raise ValueError("No se encontró la variable 'plot' tras ejecutar el código generado.")

        plot = entorno_ejecucion["plot"]

        path = "Practica_04/visualizaciones/ia_graph_02_linea_gomera_sueldos_salarios.png"
        plot.save(path, width=12, height=7, dpi=300)

        subprocess.run(["git", "add", path], check=False)
        subprocess.run(
            ["git", "commit", "-m", "Actualización automática de ia_graph_02_linea_gomera_sueldos_salarios.png"],
            check=False
        )
        subprocess.run(["git", "push"], check=False)

        return MaterializeResult(
            metadata={
                "url_publica": MetadataValue.url(
                    "https://saraguerreroull.github.io/Practicas-Visualizacion-de-datos/Practica_04/visualizaciones/ia_graph_02_linea_gomera_sueldos_salarios.png"
                ),
                "path": MetadataValue.path(path),
                "isla": MetadataValue.text("La Gomera"),
                "medida": MetadataValue.text("Sueldos y salarios"),
                "años": MetadataValue.text("2015-2023"),
                "codigo_generado": MetadataValue.md(f"```python\n{codigo_generado_ia_graph_02}\n```")
            }
        )

    except Exception as e:
        raise RuntimeError(f"Error al renderizar graph_02 con código IA: {e}")


@asset(
    group_name="visualizacion",
    #group_name="Graficos_generados_por_IA",
    description=(
        "Genera el gráfico 03 a partir de código Plotnine producido por IA. "
        "Genera un scatter plot para analizar la relación entre el nivel de educación superior "
        "y el peso de los sueldos en la renta bruta media de los municipios de La Palma (2023)."
    )
)
def graph_03(df_renta_edu: pd.DataFrame, codigo_generado_ia_graph_03: str) -> MaterializeResult:
    import subprocess

    df = df_renta_edu[
        (df_renta_edu["Isla"] == "La Palma") &
        (df_renta_edu["Tipo"] == "Municipio") &
        (df_renta_edu["Medida"] == "Sueldos y salarios") &
        (df_renta_edu["Año"] == 2023)
    ].dropna(subset=["Educación superior (%)"]).copy()

    entorno_ejecucion = globals().copy()
    entorno_ejecucion["pd"] = pd
    entorno_ejecucion["df"] = df

    try:
        exec(codigo_generado_ia_graph_03, entorno_ejecucion)

        if "plot" not in entorno_ejecucion:
            raise ValueError("No se encontró la variable 'plot' tras ejecutar el código generado.")

        plot = entorno_ejecucion["plot"]

        path = "Practica_04/visualizaciones/ia_graph_03_scatter_palma_salario_educación_2023.png"
        plot.save(path, width=8, height=6, dpi=300)

        subprocess.run(["git", "add", path], check=False)
        subprocess.run(
            ["git", "commit", "-m", "Actualización automática de ia_graph_03_scatter_palma_salario_educación_2023.png"],
            check=False
        )
        subprocess.run(["git", "push"], check=False)

        return MaterializeResult(
            metadata={
                "url_publica": MetadataValue.url(
                    "https://saraguerreroull.github.io/Practicas-Visualizacion-de-datos/Practica_04/visualizaciones/ia_graph_03_scatter_palma_salario_educación_2023.png"
                ),
                "path": MetadataValue.path(path),
                "isla": MetadataValue.text("La Palma"),
                "medida": MetadataValue.text("Sueldos y salarios"),
                "año": MetadataValue.int(2023),
                "municipios_analizados": MetadataValue.int(len(df)),
                "codigo_generado": MetadataValue.md(f"```python\n{codigo_generado_ia_graph_03}\n```")
            }
        )

    except Exception as e:
        raise RuntimeError(f"Error al renderizar graph_03 con código IA: {e}")
    

@asset(
    group_name="visualizacion",
    description=(
        "Genera un mapa coroplético del peso de sueldos y salarios en la renta bruta media por municipio de Canarias (2015) " \
        "usando Plotnine."
        )
)
def graph_04(df_renta_mapa: pd.DataFrame) -> MaterializeResult:
    gdf = df_renta_mapa[
        (df_renta_mapa["Año"] == 2015) &
        (df_renta_mapa["Medida"] == "Sueldos y salarios")
    ].copy()

    n_municipios = len(gdf)

    # Convertir geometrías a dataframe de coordenadas para plostnine
    filas = []

    for idx, row in gdf.iterrows():
        geom = row["geometry"]

        if geom is None:
            continue

        polygons = geom.geoms if isinstance(geom, MultiPolygon) else [geom]

        for poly_idx, poly in enumerate(polygons):
            if not isinstance(poly, Polygon):
                continue

            # Exterior del polígono
            x, y = poly.exterior.xy
            grupo = f"{idx}_{poly_idx}"

            for xi, yi in zip(x, y):
                filas.append({
                    "long": xi,
                    "lat": yi,
                    "group": grupo,
                    "Territorio": row["Territorio"],
                    "Valor (%)": row["Valor (%)"],
                    "Año": row["Año"],
                    "Medida": row["Medida"],
                })

    df_plot = pd.DataFrame(filas)

    plot = (
        ggplot(df_plot, aes(x="long", y="lat", group="group", fill="Valor (%)"))
        + geom_polygon(color="white", size=0.15)
        + coord_equal()
        + scale_fill_cmap(
            cmap_name="OrRd",
            labels=lambda x: [f"{v:.0f}%" for v in x]
        )
        + labs(
            title="Peso de sueldos y salarios en la renta bruta media por municipio",
            subtitle="Municipios de Canarias (2015)",
            fill="Peso en renta\nbruta media (%)"
        )
        + theme_bw()
        + theme(
            figure_size=(10, 10),
            plot_title=element_text(weight="bold", size=14),
            plot_subtitle=element_text(size=11, color="#666666"),
            legend_position="right",
            legend_title=element_text(size=11),
            legend_text=element_text(size=10),
            axis_title=element_blank(),
            axis_text=element_blank(),
            axis_ticks=element_blank(),
            panel_grid=element_blank(),
            panel_border=element_rect(fill=None, size=1),
        )
    )

    path = "Practica_04/visualizaciones/graph_04_mapa_municipios_salarios_2015.png"
    plot.save(path, width=10, height=5, dpi=300)

    subprocess.run(["git", "add", path], check=False)
    subprocess.run(
        ["git", "commit", "-m", "Actualización automática de graph_04_mapa_municipios_salarios_2015.png"],
        check=False
    )
    subprocess.run(["git", "push"], check=False)

    return MaterializeResult(
        metadata={
            "url_publica": MetadataValue.url(
                "https://saraguerreroull.github.io/Practicas-Visualizacion-de-datos/Practica_04/visualizaciones/graph_04_mapa_municipios_salarios_2015.png"
            ),
            "path": MetadataValue.path(path),
            "medida": MetadataValue.text("Sueldos y salarios"),
            "año": MetadataValue.int(2015),
            "municipios_analizados": MetadataValue.int(n_municipios),
        }
    )


# ================================================================
# JOB Y SENSOR
# ================================================================

pipeline_renta_job = define_asset_job(
    name="pipeline_renta_job",
    selection=AssetSelection.all()
)

@sensor(job=pipeline_renta_job)
def sensor_cambios_datos_renta(context):
    archivos_vigilados = [
        r"C:\Users\Usuario\Documents\Aaa ULL\Visualización\Practicas-Visualizacion-de-datos\Practica_02\data\distribucion-renta-canarias.csv",
        r"C:\Users\Usuario\Documents\Aaa ULL\Visualización\Practicas-Visualizacion-de-datos\Practica_02\data\codislas.csv",
        r"C:\Users\Usuario\Documents\Aaa ULL\Visualización\Practicas-Visualizacion-de-datos\Practica_02\data\nivelestudios.xlsx",
        r"C:\Users\Usuario\Documents\Aaa ULL\Visualización\Practicas-Visualizacion-de-datos\Practica_04\data\Municipios-2024_geo.json"
    ]

    estado_anterior = json.loads(context.cursor) if context.cursor else {}
    estado_actual = {}

    for ruta in archivos_vigilados:
        if os.path.exists(ruta):
            estado_actual[ruta] = os.path.getmtime(ruta)
        else:
            estado_actual[ruta] = None

    if estado_actual != estado_anterior:
        context.update_cursor(json.dumps(estado_actual, ensure_ascii=False))
        yield RunRequest(
            run_key=str(estado_actual),
            run_config={}
        )
    else:
        yield SkipReason("No hubo cambios en los archivos de datos.")