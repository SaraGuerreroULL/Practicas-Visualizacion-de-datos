from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from Practica_03 import lab_renta_assets
from Practica_03 import lab_renta_checks

# Supongamos que tu archivo se llama proyecto_islas.py

defs = Definitions(
    assets=load_assets_from_modules([lab_renta_assets]),
    asset_checks=load_asset_checks_from_modules([lab_renta_checks])
)