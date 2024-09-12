from dagster import Definitions

from repositories.assets.jobs import (
    sign_library_to_assets,
    synchronize_bridge_fields,
    update_bridge_evaluations,
)
from repositories.assets.schedules import (
    prod_bridge_eval_schedule,
    prod_bridge_field_schedule,
    test_bridge_eval_schedule,
    test_bridge_field_schedule,
    sign_library_schedule,
)

DEFINITIONS = Definitions(
    jobs={sign_library_to_assets, synchronize_bridge_fields, update_bridge_evaluations},
    resources={},
    schedules={
        prod_bridge_eval_schedule,
        prod_bridge_field_schedule,
        test_bridge_eval_schedule,
        test_bridge_field_schedule,
        sign_library_schedule,
    },
)
