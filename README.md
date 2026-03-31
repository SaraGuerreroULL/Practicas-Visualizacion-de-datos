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

()
```
```
---

## Autora

Sara V. Guerrero Espinosa
Máster en Ciberseguridad e Inteligencia de Datos
Universidad de La Laguna
