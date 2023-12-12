# This file is managed by the 'files-airflow' file bundle and updated automatically when `meltano upgrade` is run.
# To prevent any manual changes from being overwritten, remove the file bundle from `meltano.yml` or disable automatic updates:
#     meltano config --plugin-type=files files-airflow set _update orchestrate/dags/meltano.py false

# If you want to define a custom DAG, create
# a new file under orchestrate/dags/ and Airflow
# will pick it up automatically.

import json
from collections.abc import Iterable
from datetime import datetime, timedelta

import paramiko
from airflow import DAG
from airflow.models import Connection
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.utils.trigger_rule import TriggerRule
from mattermost import mm_failed_task
from logger import logging

logger = logging.getLogger(__name__)

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "catchup": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "concurrency": 1,
}

DEFAULT_TAGS = ["meltano"]


def _meltano_elt_generator(schedules):
    """Generate singular dag's for each legacy Meltano elt task.

    Args:
        schedules (list): List of Meltano schedules.
    """

    for schedule in schedules["schedules"].get("elt", []):
        logger.info(f"Considering schedule '{schedule['name']}': {schedule}")
        if not schedule["cron_interval"]:
            logger.info(
                f"No DAG created for schedule '{schedule['name']}' because its interval is set to `@once`."
            )
            continue

        args = DEFAULT_ARGS.copy()
        if schedule["start_date"]:
            args["start_date"] = schedule["start_date"]

        dag_id = f"meltano_{schedule['name']}"

        tags = DEFAULT_TAGS.copy()
        if schedule["extractor"]:
            tags.append(schedule["extractor"])
        if schedule["loader"]:
            tags.append(schedule["loader"])
        if schedule["transform"] == "run":
            tags.append("transform")
        elif schedule["transform"] == "only":
            tags.append("transform-only")

        # from https://airflow.apache.org/docs/stable/scheduler.html#backfill-and-catchup
        #
        # It is crucial to set `catchup` to False so that Airflow only create a single job
        # at the tail end of date window we want to extract data.
        #
        # Because our extractors do not support date-window extraction, it serves no
        # purpose to enqueue date-chunked jobs for complete extraction window.
        dag = DAG(
            dag_id,
            tags=tags,
            catchup=False,
            default_args=args,
            schedule_interval=schedule["interval"],
            max_active_runs=1,
            on_failure_callback=mm_failed_task,
        )

        elt = SSHOperator(
            task_id="extract_load",
            ssh_conn_id="meltano_ssh",
            command=f"cd /project; meltano schedule run {schedule['name']}",
            dag=dag,
        )

        # register the dag
        globals()[dag_id] = dag
        logger.info(f"DAG created for schedule '{schedule['name']}'")


def _meltano_job_generator(schedules):
    """Generate dag's for each task within a Meltano scheduled job.

    Args:
        schedules (list): List of Meltano scheduled jobs.
    """

    logger.info(f"Received schedules : {schedules}")
    for schedule in schedules["schedules"].get("job", []):
        if not schedule.get("job"):
            logger.info(
                f"No DAG's created for schedule '{schedule['name']}'. It was passed to job generator but has no job."
            )
            continue
        if not schedule["cron_interval"]:
            logger.info(
                f"No DAG created for schedule '{schedule['name']}' because its interval is set to `@once`."
            )
            continue

        base_id = f"meltano_{schedule['name']}_{schedule['job']['name']}"
        common_tags = DEFAULT_TAGS.copy()
        common_tags.append(f"schedule:{schedule['name']}")
        common_tags.append(f"job:{schedule['job']['name']}")
        interval = schedule["cron_interval"]
        args = DEFAULT_ARGS.copy()
        with DAG(
            base_id,
            tags=common_tags,
            catchup=False,
            default_args=args,
            schedule_interval=interval,
            start_date=datetime(2022, 9, 1),
            max_active_runs=1,
        ) as dag:
            previous_task = None
            for idx, task in enumerate(schedule["job"]["tasks"]):
                logger.info(
                    f"Considering task '{task}' of schedule '{schedule['name']}': {schedule}"
                )

                task_id = f"{base_id}_task{idx}"

                if isinstance(task, Iterable) and not isinstance(task, str):
                    run_args = " ".join(task)
                else:
                    run_args = task

                trigger_rule = TriggerRule.ALL_SUCCESS
                if ("docs-generate" in run_args) or ("monitor-report" in run_args):
                    trigger_rule = TriggerRule.ALL_DONE

                task = SSHOperator(
                    task_id=task_id,
                    ssh_conn_id="meltano_ssh",
                    command=f"cd /project; meltano run {run_args}",
                    dag=dag,
                    cmd_timeout=60 * 60 * 4,
                    trigger_rule=trigger_rule,
                    on_failure_callback=mm_failed_task,
                )
                if previous_task:
                    task.set_upstream(previous_task)
                previous_task = task
                logger.info(
                    f"Spun off task '{task}' of schedule '{schedule['name']}': {schedule}"
                )

        globals()[base_id] = dag
        logger.info(f"DAG created for schedule '{schedule['name']}', task='{run_args}'")


def create_dags():
    """Create DAGs for Meltano schedules."""

    con = Connection.get_connection_from_secrets("meltano_ssh")
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    client.connect(con.host, username=con.login, password=con.password)
    _, stdout, _ = client.exec_command(
        f"cd /project && meltano schedule list --format=json "
    )
    # logger.info(stdout.read())
    schedule_export = json.loads(stdout.read())

    _meltano_elt_generator(schedule_export)
    _meltano_job_generator(schedule_export)


create_dags()
