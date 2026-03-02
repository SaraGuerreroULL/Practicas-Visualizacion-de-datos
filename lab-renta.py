import pandas as pd
from plotnine import *
from dagster import asset, Definitions, MaterializeResult, MetadataValue


# ================================================================
# CAPA 1: CARGA
# ================================================================

@asset(key_prefix="carga",
       description="Carga el dataset de distribución de renta bruta media en Canarias.")
def raw_renta() -> pd.DataFrame:
    return pd.read_csv("data/distribucion-renta-canarias.csv")


@asset(key_prefix="carga",
       description="Carga el dataset de códigos y nombres de municipios e islas de Canarias.")
def raw_nombres() -> pd.DataFrame:
    return pd.read_csv("data/codislas.csv", sep=";", encoding="WINDOWS-1252")


@asset(key_prefix="carga",
       description="Carga el dataset de nivel de estudios por municipio.")
def raw_estudios() -> pd.DataFrame:
    return pd.read_excel("data/nivelestudios.xlsx")


# ================================================================
# CAPA 2: LIMPIEZA
# ================================================================

@asset(key_prefix="limpieza",
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


@asset(key_prefix="limpieza",
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


@asset(key_prefix="limpieza",
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

@asset(key_prefix="transformacion",
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


@asset(key_prefix="transformacion",
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


# ================================================================
# CAPA 4: VISUALIZACIÓN
# ================================================================

@asset(key_prefix="visualizacion",
       description="Genera un gráfico de barras apiladas con la distribución de la renta bruta media por fuente de ingresos, con facet por isla (2015-2023).")
def graph_01(df_renta: pd.DataFrame) -> MaterializeResult:
    
    orden_islas = [
        "Canarias", "El Hierro", "La Palma", "La Gomera",
        "Tenerife", "Gran Canaria", "Fuerteventura", "Lanzarote",
    ]
    orden_medidas = [
        "Sueldos y salarios", "Pensiones", "Prestaciones por desempleo",
        "Otras prestaciones", "Otros ingresos",
    ]
    paleta = ["#FD814E", "#FCBC52", "#A4D984", "#F26386", "#F588AF"]

    df_islas = df_renta[df_renta["Territorio"].isin(orden_islas)].copy()
    df_islas["Territorio"] = pd.Categorical(df_islas["Territorio"], categories=orden_islas, ordered=True)
    df_islas["Medida"] = pd.Categorical(df_islas["Medida"], categories=orden_medidas, ordered=True)
    df_islas = df_islas.sort_values(["Territorio", "Medida", "Año"])

    plot = (
        ggplot(df_islas, aes(x="factor(Año)", y="Valor (%)", fill="Medida"))
        + geom_col(position="stack", width=0.9)
        + facet_wrap("~Territorio", ncol=4)
        + scale_y_continuous(limits=(0, 101), labels=lambda x: [f"{v:.0f}%" for v in x])
        + scale_fill_manual(values=paleta)
        + labs(
            title="Distribución de la renta bruta media según fuente de ingresos en Canarias (2015-2023)",
            x="Año",
            y="Peso en la renta bruta media (%)",
            fill=""
        )
        + theme_bw()
        + theme(
            figure_size=(16, 8),
            axis_text_x=element_text(rotation=45, ha="right", size=10),
            axis_text_y=element_text(size=10),
            axis_title=element_text(size=13),
            legend_position="bottom",
            legend_direction="horizontal",
            legend_text=element_text(size=13),
            legend_title=element_text(size=13),
            plot_title=element_text(weight="bold", size=16),
            strip_text=element_text(size=13)
        )
    )

    path = "graph_01_distribucion_renta.png"
    plot.save(path, width=16, height=8, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "islas": MetadataValue.int(len(orden_islas)),
            "medidas": MetadataValue.text(", ".join(orden_medidas)),
            "años": MetadataValue.text("2015-2023"),
        }
    )


@asset(key_prefix="visualizacion",
       description="Genera un gráfico de líneas con la evolución del peso de sueldos y salarios en la renta bruta media para los municipios de La Gomera (2015-2023).")
def graph_02(df_renta_isla: pd.DataFrame) -> MaterializeResult:
    # Isla: La Gomera | Medida: Sueldos y salarios
    df_gomera_line = df_renta_isla[
        (df_renta_isla["Isla"] == "La Gomera") &
        (df_renta_isla["Medida"] == "Sueldos y salarios")
    ].copy()

    df_gomera_line["Linea"] = df_gomera_line["Tipo"].apply(
        lambda x: "Isla" if x == "Isla" else "Municipio"
    )

    paleta = ["#FD814E", "#FCBC52", "#A4D984", "#F26386", "#F588AF",
              "#7EC8C8", "#B39DDB", "#90CAF9"]

    n_municipios = df_gomera_line[df_gomera_line["Tipo"] == "Municipio"]["Territorio"].nunique()

    plot = (
        ggplot(df_gomera_line, aes(x="Año", y="Valor (%)", color="Territorio",
                                   linetype="Linea", group="Territorio"))
        + geom_line(size=1.2)
        + geom_point(size=2.5)
        + scale_linetype_manual(values={"Municipio": "solid", "Isla": "dashed"})
        + scale_color_manual(values=paleta)
        + scale_x_continuous(breaks=sorted(df_gomera_line["Año"].unique()))
        + scale_y_continuous(labels=lambda x: [f"{v:.0f}%" for v in x])
        + labs(
            title="Evolución del peso de sueldos y salarios en la renta bruta media por persona",
            subtitle="Municipios de La Gomera (2015-2023)",
            x="Año",
            y="Peso en la renta bruta media (%)",
            color="Territorio",
            linetype=""
        )
        + theme_bw()
        + theme(
            figure_size=(12, 7),
            axis_text_x=element_text(size=10),
            axis_text_y=element_text(size=10),
            axis_title=element_text(size=12),
            legend_position="right",
            legend_text=element_text(size=11),
            legend_title=element_text(size=12, weight="bold"),
            plot_title=element_text(weight="bold", size=14),
            plot_subtitle=element_text(size=12, color="#666666")
        )
    )

    path = "graph_02_linea_gomera_sueldos_salarios.png"
    plot.save(path, width=12, height=7, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "isla": MetadataValue.text("La Gomera"),
            "medida": MetadataValue.text("Sueldos y salarios"),
            "municipios": MetadataValue.int(n_municipios),
            "años": MetadataValue.text("2015-2023"),
        }
    )


@asset(key_prefix="visualizacion",
       description="Genera un heatmap del peso de las prestaciones por desempleo en la renta bruta media para los municipios de La Palma (2015-2023).")
def graph_03(df_renta_isla: pd.DataFrame) -> MaterializeResult:
    # Isla: La Palma (municipios) | Medida: Prestaciones por desempleo
    df_palma_heat = df_renta_isla[
        (df_renta_isla["Isla"] == "La Palma") &
        (df_renta_isla["Tipo"] == "Municipio") &
        (df_renta_isla["Medida"] == "Prestaciones por desempleo")
    ].copy()

    # Ordenar municipios alfabéticamente
    df_palma_heat["Territorio"] = pd.Categorical(
        df_palma_heat["Territorio"],
        categories=sorted(df_palma_heat["Territorio"].unique()),
        ordered=True
    )

    n_municipios = df_palma_heat["Territorio"].nunique()

    plot = (
        ggplot(df_palma_heat, aes(x="factor(Año)", y="Territorio", fill="Valor (%)"))
        + geom_tile(color="white", size=0.8)
        + scale_fill_cmap(cmap_name="GnBu", labels=lambda x: [f"{v:.0f}%" for v in x])
        + labs(
            title="Peso de prestaciones por desempleo en la renta bruta media",
            subtitle="Municipios de La Palma (2015-2023)",
            x="Año",
            y="",
            fill="Peso en renta\nbruta media (%)\n"
        )
        + theme_bw()
        + theme(
            figure_size=(12, 7),
            axis_text_x=element_text(size=10),
            axis_text_y=element_text(size=10),
            axis_title_x=element_text(size=12),
            legend_position="right",
            legend_text=element_text(size=11),
            legend_title=element_text(size=12),
            plot_title=element_text(weight="bold", size=14),
            plot_subtitle=element_text(size=12, color="#666666"),
            panel_grid=element_blank(),
        )
    )

    path = "graph_03_heatmap_palma_desempleo.png"
    plot.save(path, width=12, height=7, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "isla": MetadataValue.text("La Palma"),
            "medida": MetadataValue.text("Prestaciones por desempleo"),
            "municipios": MetadataValue.int(n_municipios),
            "años": MetadataValue.text("2015-2023"),
        }
    )


@asset(key_prefix="visualizacion",
       description="Genera un scatter plot para analizar la relación entre el nivel de educación superior y el peso de los sueldos en la renta bruta media de los municipios de La Palma (2023).")
def graph_04(df_renta_edu: pd.DataFrame) -> MaterializeResult:
    # Isla: La Palma (municipios) | Medida: Sueldos y salarios | Año: 2023
    # Pregunta: ¿Los municipios con mayor educación superior tienen mayor peso de salarios?
    df_palma_analisis = df_renta_edu[
        (df_renta_edu["Isla"] == "La Palma") &
        (df_renta_edu["Tipo"] == "Municipio") &
        (df_renta_edu["Medida"] == "Sueldos y salarios") &
        (df_renta_edu["Año"] == 2023)
    ].dropna(subset=["Educación superior (%)"])

    n_municipios = len(df_palma_analisis)

    plot = (
        ggplot(df_palma_analisis, aes(x="Educación superior (%)", y="Valor (%)"))
        + geom_smooth(method="lm", color="#b3b3b3", fill="#eeeeee", size=0.8)
        + geom_point(size=3.5, color="#FD814E")
        + geom_text(aes(label="Territorio"), nudge_y=0.8, size=8, color="#444444")
        + scale_y_continuous(labels=lambda x: [f"{v:.0f}%" for v in x])
        + scale_x_continuous(labels=lambda x: [f"{v:.0f}%" for v in x])
        + labs(
            title="Relación entre educación superior y peso de salarios en la renta",
            subtitle="Municipios de La Palma (2023)",
            x="Proporción de población con educación superior (%)",
            y="Porcentaje de sueldos y salarios en la renta bruta media (%)"
        )
        + theme_bw()
        + theme(
            figure_size=(8, 6),
            axis_text_x=element_text(size=10),
            axis_text_y=element_text(size=9),
            axis_title=element_text(size=11),
            plot_title=element_text(weight="bold", size=14),
            plot_subtitle=element_text(size=11, color="#666666"),
            panel_grid_minor=element_blank(),
        )
    )

    path = "graph_04_scatter_palma_salario_educación_2023.png"
    plot.save(path, width=8, height=6, dpi=300)

    return MaterializeResult(
        metadata={
            "path": MetadataValue.path(path),
            "isla": MetadataValue.text("La Palma"),
            "medida": MetadataValue.text("Sueldos y salarios"),
            "año": MetadataValue.int(2023),
            "municipios_analizados": MetadataValue.int(n_municipios),
        }
    )


# ================================================================
# DEFINICIÓN DEL PIPELINE
# ================================================================

defs = Definitions(
    assets=[
        raw_renta, raw_nombres, raw_estudios,
        df_renta, df_nombres, df_estudios,
        df_renta_isla, df_renta_edu,
        graph_01, graph_02, graph_03, graph_04,
    ]
)