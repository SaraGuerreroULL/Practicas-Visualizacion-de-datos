import re
import requests
import pandas as pd
import subprocess
from pathlib import Path
from dagster import asset, asset_check, Output, AssetCheckResult, MetadataValue, Definitions
from plotnine import *


@asset
def islas_raw():
    df = pd.read_csv("Practica_04/data/pwbi-1.csv")
    return Output(
        value=df,
        metadata={
            "variables": MetadataValue.json(list(df.columns)),
            "mensaje": "Columnas del dataset"
        }
    )


@asset_check(asset=islas_raw)
def check_estandarizacion_islas(islas_raw):
    categorias_originales = sorted(islas_raw["isla"].dropna().astype(str).unique().tolist())
    categorias_normalizadas = sorted(
        islas_raw["isla"].dropna().astype(str).str.capitalize().unique().tolist()
    )

    originales = len(categorias_originales)
    normalizadas = len(categorias_normalizadas)

    passed = originales == normalizadas

    return AssetCheckResult(
        passed=passed,
        metadata={
            "No_categorias_detectadas": MetadataValue.int(originales),
            "categorias_detectadas": MetadataValue.json(categorias_originales),
            "categorias_normalizadas": MetadataValue.json(categorias_normalizadas),
            "No_categorias_esperadas": MetadataValue.int(normalizadas),
            "principio_gestalt": "Similitud (evitar fragmentación visual)",
            "mensaje": "Si hay nombres inconsistentes, la leyenda del gráfico puede duplicarse."
        }
    )


@asset
def template_ia(islas_raw):
    columnas = ", ".join(islas_raw.columns)
    islas = sorted(islas_raw["isla"].dropna().astype(str).unique().tolist())

    template_tecnico = """
        def generar_plot(df):
            # Debes devolver un objeto plotnine.
            # Estructura esperada:
            # plot = (ggplot(df, aes(...)) + ... + labs(...) + theme_bw() + theme(...) + ...)
            # return plot
        """

    system_content = (
        "Eres un experto en gramática de gráficos y Plotnine. "
        "Tu tarea es traducir descripciones en lenguaje natural a código Python ejecutable. "
        "Debes devolver exclusivamente código Python, sin explicaciones, sin markdown y sin bloques ```python. "
        "No hagas imports, asume que el entorno ya tiene importado plotnine y pandas. "
        "No leas archivos. "
        "No guardes imágenes. "
        "No imprimas nada. "
        f"Debes respetar este template:\n{template_tecnico}"
    )

    descripcion_grafico = f"""
        Genera código Plotnine para un DataFrame llamado df.

        Columnas disponibles: {columnas}
        Categorías detectadas en 'isla': {islas}

        Requisitos del gráfico:
        - Tipo de gráfico: línea
        - Eje X: 'año'
        - Eje Y: 'valor'
        - Color y group: 'isla'
        - Geometría principal: geom_line()
        - Título: Evolución del Gasto por Isla
        - Etiqueta del eje X: Año
        - Etiqueta del eje Y: Gasto en €
        - Títulos y etiquetas en negrita
        - Usa scale_color_manual(values=colores)
        - Debes definir un diccionario dinámico de colores
        - Resaltar la isla 'Tenerife' en azul, las otras islas en gris
        - Usa labs(...) correctamente
        - Usar únicamente nombres de columnas existentes
        - La función debe llamarse exactamente generar_plot
        - La función debe recibir df y devolver el gráfico
        """

    user_content = f"Basándote en esta descripción, completa el template:\n{descripcion_grafico}"

    return {
        "model": "ollama/llama3.1:8b",
        "messages": [
            {"role": "system", "content": system_content},
            {"role": "user", "content": user_content}
        ],
        "temperature": 0.1,
        "stream": False
    }


@asset
def codigo_generado_ia(context, template_ia):
    url = "http://gpu1.esit.ull.es:4000/v1/chat/completions"
    headers = {
        "Content-Type": "application/json",
        "Authorization": "Bearer sk-1234"
    }

    try:
        response = requests.post(url, json=template_ia, headers=headers, timeout=60)
        response.raise_for_status()

        res_json = response.json()
        codigo_raw = res_json["choices"][0]["message"]["content"]

        # Caso 1: si viene dentro de bloque ```python ... ```
        match = re.search(r"```python\s*(.*?)\s*```", codigo_raw, re.DOTALL)

        if match:
            codigo_final = match.group(1).strip()
        else:
            # Caso 2: limpieza manual de markdown o texto sobrante
            lineas = codigo_raw.split("\n")
            lineas_validas = []

            for l in lineas:
                texto = l.strip()
                if (
                    not texto.startswith("```")
                    and not texto.startswith("###")
                    and not texto.startswith("- ")
                ):
                    lineas_validas.append(l)

            codigo_final = "\n".join(lineas_validas).strip()

        # Validación mínima
        if "def generar_plot" not in codigo_final:
            raise ValueError("La IA no devolvió una función llamada 'generar_plot'.")

        return Output(
            value=codigo_final,
            metadata={
                "codigo_completo": MetadataValue.md(f"```python\n{codigo_final}\n```"),
                "mensaje": "Código generado por IA y limpiado correctamente"
            }
        )

    except Exception as e:
        context.log.error(f"Error en la petición o limpieza del código: {e}")
        raise


@asset_check(asset=codigo_generado_ia)
def check_codigo_generado_ia_valido(codigo_generado_ia):
    tiene_funcion = "def generar_plot" in codigo_generado_ia
    usa_ggplot = "ggplot" in codigo_generado_ia
    passed = tiene_funcion and usa_ggplot

    return AssetCheckResult(
        passed=passed,
        metadata={
            "contiene_generar_plot": MetadataValue.bool(tiene_funcion),
            "contiene_ggplot": MetadataValue.bool(usa_ggplot),
            "mensaje": "El código debe definir generar_plot(df) y usar Plotnine."
        }
    )


@asset
def visualizacion_png(context, codigo_generado_ia, islas_raw):
    import plotnine

    entorno_ejecucion = globals().copy()
    entorno_ejecucion["plotnine"] = plotnine
    entorno_ejecucion["pd"] = pd

    entorno_ejecucion.update({
        k: v for k, v in plotnine.__dict__.items() if not k.startswith("_")
    })

    try:
        # Ejecuta el código generado por la IA
        exec(codigo_generado_ia, entorno_ejecucion)

        if "generar_plot" not in entorno_ejecucion:
            raise ValueError("No se encontró la función 'generar_plot' en el entorno de ejecución.")

        grafico = entorno_ejecucion["generar_plot"](islas_raw)


        output_dir = Path(r"C:\Users\Usuario\Documents\Aaa ULL\Visualización\Practicas-Visualizacion-de-datos\Practica_04\visualizaciones\test")
        output_dir.mkdir(parents=True, exist_ok=True)
        ruta_archivo = output_dir / "visualizacion_test_prompt_1.png"
        grafico.save(ruta_archivo, width=10, height=6, dpi=100)

        # Automatizar el envío a GitHub desde local
        subprocess.run(["git", "add", ruta_archivo], check=False)
        subprocess.run(["git", "commit", "-m", "Test. Actualización automática del gráfico"], check=False)
        subprocess.run(["git", "push"], check=False)

        return Output(
            value=ruta_archivo,
            metadata={
                "ruta": ruta_archivo,
                "mensaje": "Gráfico generado, guardado y enviado a GitHub"
            }
        )

    except Exception as e:
        context.log.error(f"Error al renderizar el gráfico: {e}")
        raise


@asset_check(asset=visualizacion_png)
def check_visualizacion_generada(visualizacion_png):
    from pathlib import Path

    ruta = Path(visualizacion_png)
    existe = ruta.exists()
    tamano = ruta.stat().st_size if existe else 0
    passed = existe and tamano > 0

    return AssetCheckResult(
        passed=passed,
        metadata={
            "ruta": MetadataValue.path(str(ruta)),
            "tamano_bytes": MetadataValue.int(tamano),
            "mensaje": "Comprueba que la visualización se ha generado correctamente."
        }
    )

defs = Definitions(
    assets=[
        islas_raw,
        template_ia,
        codigo_generado_ia,
        visualizacion_png,
    ],
    asset_checks=[
        check_estandarizacion_islas,
        check_codigo_generado_ia_valido,
        check_visualizacion_generada,
    ]
)