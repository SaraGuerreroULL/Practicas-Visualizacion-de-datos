from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from Practica_04 import ia_lab_renta_assets
from Practica_04 import ia_lab_renta_checks

defs = Definitions(
    assets=load_assets_from_modules([ia_lab_renta_assets]),
    asset_checks=load_asset_checks_from_modules([ia_lab_renta_checks]),
    jobs=[ia_lab_renta_assets.pipeline_renta_job],
    sensors=[ia_lab_renta_assets.sensor_cambios_datos_renta],
)
