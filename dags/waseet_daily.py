"""The daily scan DAG.

Sensor -> branch -> (run_pipeline | skip_day) -> run_quality -> finish, with a
failure callback that names the task so an on-call engineer can route from it.

Iterate with:
    docker exec waseet-airflow airflow dags test waseet_daily 2026-05-04
"""

from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.sensors.filesystem import FileSensor
import pendulum

DATA = "/opt/airflow/data"
CODE = "/opt/airflow/pipeline"


def alert(context):
    """Fires after retries are exhausted. Which task failed matters more than
    that something did: the sensor timing out is the supplier's problem, the
    load failing is ours, and the two wake up different people."""
    task = context["task_instance"].task_id
    ds = context["ds"]
    line = ds + "  " + task + "  FAILED\n"
    with open(CODE + "/logs/alerts.txt", "a") as handle:
        handle.write(line)
    print("ALERT:", line)


def choose_branch(ds):
    """Return the task_id to run next. The 15th (Eid) is a header with no rows -
    that day the load is skipped, not failed. Failing that morning would be an
    alert on a day when nothing is wrong."""
    import pandas as pd
    day = pd.read_csv(DATA + "/scans_" + ds + ".csv", dtype=str)
    print(ds, "has", len(day), "rows")
    if len(day) == 0:
        return "skip_day"
    return "run_pipeline"


# A late supplier is not a transient failure, and a renamed column is not one
# either - so the pipeline itself does not retry. Only the sensor waits.
default_args = {
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": alert,
}


with DAG(
    dag_id="waseet_daily",
    start_date=pendulum.datetime(2026, 5, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["waseet"],
) as dag:

    # Wait for the file rather than fail on its absence. The 10th never arrives,
    # so the timeout is real: when it fires, the sensor fails and alert() names
    # wait_for_file - the supplier's problem. mode="reschedule" frees the worker
    # slot between pokes instead of holding it for the whole wait.
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=DATA + "/scans_{{ ds }}.csv",
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60,
        mode="reschedule",
    )

    choose = BranchPythonOperator(
        task_id="choose",
        python_callable=choose_branch,
    )

    run_pipeline = BashOperator(
        task_id="run_pipeline",
        bash_command="cd " + CODE + " && python pipeline.py --date {{ ds }}",
    )

    skip_day = BashOperator(
        task_id="skip_day",
        bash_command="echo no rows for {{ ds }}, skipping the load",
    )

    run_quality = BashOperator(
        task_id="run_quality",
        bash_command="cd " + CODE + " && python quality.py --date {{ ds }} || true",

    )

    # The join. Its trigger rule is the thing to get right. Left at the default
    # all_success, every skipped day would go green with run_quality quietly not
    # run. none_failed_min_one_success lets the skip path through while still
    # failing if a real task failed.
    finish = BashOperator(
        task_id="finish",
        bash_command="echo {{ ds }} done",
        trigger_rule="none_failed_min_one_success",
    )

    wait_for_file >> choose >> [run_pipeline, skip_day]
    run_pipeline >> run_quality >> finish
    skip_day >> finish