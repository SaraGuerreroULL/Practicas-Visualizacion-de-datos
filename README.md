# Prácticas – Visualización de Datos

Repositorio correspondiente a las prácticas de la asignatura **Visualización de Datos** del Máster en Ciberseguridad e Inteligencia de Datos.

El objetivo general del repositorio es almacenar de forma estructurada el código, los datos, las visualizaciones generadas y los informes asociados a cada práctica, siguiendo principios de reproducibilidad y organización propios de DataOps.

---

## Estructura del repositorio

El repositorio contiene un directorio por práctica, debidamente nombrado. Actualmente el repositorio contiene la carpeta:

```
Practica_02/
Práctica_03/
```
---

## Práctica 02 – Gramática de Gráficos y DataOps

Esta práctica tuvo como objetivo diseñar e implementar un flujo de trabajo reproducible para el análisis y visualización de la distribución de la renta bruta media en Canarias.

Se trabajó bajo dos enfoques complementarios:

* **Gramática de gráficos (Wickham)** para la planificación conceptual de las visualizaciones.
* **DataOps** para la organización del pipeline mediante Dagster.

La estructura del directorio es la siguiente:

```
Practica_02
│
├── lab-renta.py                     # Definición del pipeline en Dagster
├── test-assets.py                   # Archivo de prueba de assets
├── Informe_Práctica02_VD_SaraGuerrero.pdf
│
├── graph_01_distribucion_renta.png
├── graph_02_linea_gomera_sueldos_salarios.png
├── graph_03_heatmap_palma_desempleo.png
├── graph_04_scatter_palma_salario_educación_2023.png
│
└── data/
    ├── distribucion-renta-canarias.csv
    ├── codislas.csv
    └── nivelestudios.xlsx
```
---

## Práctica 03 – Calidad de la visualización: Checks

Esta práctica tuvo como objetivo incorporar mecanismos de validación de calidad dentro de un pipeline de datos orientado a la visualización, utilizando Dagster como herramienta de orquestación. Para ello, se diseñó e implementó un conjunto de asset checks destinados a verificar la integridad, coherencia y consistencia de los datos a lo largo de todo el flujo de trabajo, desde la carga hasta la exportación de las visualizaciones.

El pipeline retoma el proyecto de análisis de la distribución de la renta en Canarias desarrollado en la práctica anterior y lo amplía con controles automáticos de calidad, alineados con el enfoque DataOps. En total, se trabajó con 12 assets y 19 checks, distribuidos entre las fases de carga, limpieza, transformación e inspección de salidas gráficas.

La estructura del directorio es la siguiente:

```
Practica_03
│
├── definitions.py                         # Definición central del proyecto en Dagster
├── Informe_Práctica03_VD_SaraGuerrero.pdf # Informe de la práctica
├── lab_renta_assets.py                    # Assets del pipeline principal
├── lab_renta_checks.py                    # Checks de calidad asociados a los assets
│
├── data/
│   ├── pwbi-1.csv                         # Dataset empleado en la práctica
│   └── test_checks.py                     # Script de prueba inicial para comprender los checks
│
├── visualizaciones/
│   ├── graph_01_distribucion_renta.png
│   ├── graph_02_linea_gomera_sueldos_salarios.png
│   ├── graph_03_heatmap_palma_desempleo.png
│   └── graph_04_scatter_palma_salario_educación_2023.png
│
└── __pycache__/                           # Archivos generados automáticamente por Python
```
---

## Autora

Sara V. Guerrero Espinosa
Máster en Ciberseguridad e Inteligencia de Datos
Universidad de La Laguna
