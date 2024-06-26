import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.utils.trigger_rule import TriggerRule
from logger import logging
from mattermost import mm_failed_task

from trackdechets_search_sirene.utils import (
    download_es_ca_pem,
    git_clone_trackdechets,
    npm_install_build,
    read_output,
)

es_connection = Connection.get_connection_from_secrets(
    "trackdechets_search_sirene_elasticsearch_url"
)

es_credentials = ""
if es_connection.login and es_connection.password:
    es_credentials = f"{es_connection.login}:{es_connection.password}@"

es_schema = "http"
if es_connection.schema:
    es_schema = f"{es_connection.schema}"

logger = logging.getLogger(__name__)

environ = {
    "FORCE_LOGGER_CONSOLE": Variable.get("FORCE_LOGGER_CONSOLE"),
    "ELASTICSEARCH_URL": f"{es_schema}://{es_credentials}{es_connection.host}:{es_connection.port}",
    "DD_LOGS_ENABLED": Variable.get("DD_LOGS_ENABLED"),
    "DD_TRACE_ENABLED": Variable.get("DD_TRACE_ENABLED"),
    "DD_API_KEY": Variable.get("DD_API_KEY"),
    "DD_APP_NAME": Variable.get("DD_APP_NAME"),
    "DD_ENV": Variable.get("DD_ENV"),
    "NODE_ENV": Variable.get("NODE_ENV"),
    "NODE_OPTIONS": Variable.get("NODE_OPTIONS"),
    "ELASTICSEARCH_CAPEM": Variable.get("ELASTICSEARCH_CAPEM"),
    "INDEX_CHUNK_SIZE": Variable.get("INDEX_CHUNK_SIZE"),
    "INDEX_SIRET_ONLY": Variable.get("INDEX_SIRET_ONLY"),
    "TD_SIRENE_INDEX_MAX_CONCURRENT_REQUESTS": Variable.get(
        "TD_SIRENE_INDEX_MAX_CONCURRENT_REQUESTS"
    ),
    "TD_SIRENE_INDEX_MAX_HIGHWATERMARK": Variable.get(
        "TD_SIRENE_INDEX_MAX_HIGHWATERMARK"
    ),
    "TD_SIRENE_INDEX_SLEEP_BETWEEN_CHUNKS": Variable.get(
        "TD_SIRENE_INDEX_SLEEP_BETWEEN_CHUNKS"
    ),
}


# Constant pointing to the node git indexation repo
TRACKDECHETS_SIRENE_SEARCH_GIT = Variable.get("TRACKDECHETS_SIRENE_SEARCH_GIT")
TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH = Variable.get("TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH", "main")


@dag(
    schedule_interval="0 22 1 * *",
    catchup=False,
    start_date=datetime(2022, 12, 1),
    on_failure_callback=mm_failed_task,
)
def full_update_search_sirene():
    """DAG permettant d'indexer la base SIRENE de l'INSEE dans ElasticSearch"""

    @task
    def task_git_clone_trackdechets() -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="trackdechets_search_sirene"))
        return git_clone_trackdechets(
            tmp_dir, TRACKDECHETS_SIRENE_SEARCH_GIT, TRACKDECHETS_SIRENE_SEARCH_GIT_BRANCH
        )

    @task
    def task_npm_install_build(tmp_dir) -> str:
        """
        npm install && npm run build
        """
        return npm_install_build(tmp_dir, TRACKDECHETS_SIRENE_SEARCH_GIT)

    @task
    def task_download_es_ca_pem(tmp_dir) -> str:
        return download_es_ca_pem(
            tmp_dir, environ["ELASTICSEARCH_CAPEM"], TRACKDECHETS_SIRENE_SEARCH_GIT
        )

    @task
    def task_npm_run_index(tmp_dir) -> str:
        """
        npm run index
        """
        if environ["INDEX_SIRET_ONLY"] == "true":
            command = "npm run index:siret"
        else:
            command = "npm run index"

        tmp_dir = Path(tmp_dir)
        index_command = command
        node_process = subprocess.Popen(
            index_command,
            shell=True,
            cwd=tmp_dir / TRACKDECHETS_SIRENE_SEARCH_GIT,
            env=environ,
            stdout=subprocess.PIPE,
        )
        # read the output
        while True:
            line = node_process.stdout.readline()
            if not line:
                break
            read_output(line)

        while node_process.wait():
            if node_process.returncode != 0:
                raise Exception(node_process)

        return str(tmp_dir)

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def task_cleanup_tmp_files(tmp_dir: str):
        """Clean DAG's artifacts"""
        shutil.rmtree(tmp_dir)

    """
    Dag workflow
    """
    tmp_dir = task_git_clone_trackdechets()
    (
        task_npm_install_build(tmp_dir)
        >> task_download_es_ca_pem(tmp_dir)
        >> task_npm_run_index(tmp_dir)
        >> task_cleanup_tmp_files(tmp_dir)
    )


trackdechets_search_sirene_dag = full_update_search_sirene()

if __name__ == "__main__":
    trackdechets_search_sirene_dag.test()
