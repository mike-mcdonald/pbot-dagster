from dagster import Definitions

from repositories.assets.jobs import (
    sign_library_to_assets,
    synchronize_bridge_fields,
    update_bridge_evaluations,
)
from repositories.assets.schedules import (
    bridge_daily_schedule,
    bridge_eval_updates,
    sign_library_schedule,
)

DEFINITIONS = Definitions(
    jobs={sign_library_to_assets, synchronize_bridge_fields, update_bridge_evaluations},
    schedules={bridge_daily_schedule, bridge_eval_updates, sign_library_schedule},
)
