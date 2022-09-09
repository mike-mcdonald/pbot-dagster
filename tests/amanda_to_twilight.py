import datetime

from repositories.amanda_to_twilight import amanda_schedule

from dagster import build_schedule_context

context = build_schedule_context(scheduled_execution_time=datetime.datetime.now())
run_request = amanda_schedule(context)
print(run_request.run_config)
