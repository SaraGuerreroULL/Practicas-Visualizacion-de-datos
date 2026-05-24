from dagster import Definitions, load_assets_from_modules, load_asset_checks_from_modules
from Proyecto_final import proyectofinal_assets
from Proyecto_final import proyectofinal_checks

defs = Definitions(
    assets=load_assets_from_modules([proyectofinal_assets]),
    asset_checks=load_asset_checks_from_modules([proyectofinal_checks]),
    jobs=[proyectofinal_assets.pipeline_proyecto_job],
    sensors=[proyectofinal_assets.sensor_cambios_datos_renta],
)
