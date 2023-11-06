from dagster import (
    AssetSelection,
    Definitions,
    ScheduleDefinition,
    asset,
    define_asset_job,
    ExperimentalWarning,
    load_assets_from_modules
)
from . import bbd

all_assets = load_assets_from_modules([bbd])


update_bbd = define_asset_job(name="update_bbd", selection=AssetSelection.all())
bbd = ScheduleDefinition(
    name="bbd", job=update_bbd, cron_schedule="0 9 * * 2-7", execution_timezone='Asia/Bangkok'
)

# Menambahkan jadwal baru untuk Senin pukul 13:30
bbd_monday = ScheduleDefinition(
    name="bbd_monday", job=update_bbd, cron_schedule="00 12 * * 1", execution_timezone='Asia/Bangkok'
)

defs = Definitions(
    assets=all_assets,
    schedules=[bbd, bbd_monday] 
)
