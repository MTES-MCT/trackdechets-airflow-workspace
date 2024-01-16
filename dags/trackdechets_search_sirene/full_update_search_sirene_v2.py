import shutil
import tempfile
from datetime import datetime
from pathlib import Path

import logging

from mattermost import mm_failed_task

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.operators.bash import BashOperator

logging.basicConfig()

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
    "ELASTICSEARCH_CAPEM": Variable.get("ELASTICSEARCH_CAPEM"),
    "INDEX_CHUNK_SIZE": Variable.get("INDEX_CHUNK_SIZE"),
    "INDEX_SIRET_ONLY": Variable.get("INDEX_SIRET_ONLY"),
    "TD_SIRENE_INDEX_MAX_CONCURRENT_REQUESTS": Variable.get(
        "TD_SIRENE_INDEX_MAX_CONCURRENT_REQUESTS"
    ),
}


# Constant pointing to the node git indexation repo
TRACKDECHETS_SIRENE_SEARCH_GIT = Variable.get("TRACKDECHETS_SIRENE_SEARCH_GIT")


@dag(
    schedule_interval="0 22 1 * *",
    catchup=False,
    start_date=datetime(2022, 12, 1),
    on_failure_callback=mm_failed_task,
)
def full_update_search_sirene_v2():
    """DAG permettant d'indexer la base SIRENE de l'INSEE dans ElasticSearch"""

    @task
    def create_tmp_dir() -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="trackdechets_search_sirene"))
        return str(tmp_dir)

    tmp_dir = create_tmp_dir()
    git_clone_trackdechets = BashOperator(
        task_id="git_clone_trackdechets",
        bash_command=f"cd {{{{ ti.xcom_pull(task_ids='create_tmp_dir') }}}} && git clone https://github.com/MTES-MCT/{TRACKDECHETS_SIRENE_SEARCH_GIT}.git",
    )

    npm_install_build = BashOperator(
        task_id="npm_install_build",
        bash_command=f"cd {{{{ ti.xcom_pull(task_ids='create_tmp_dir') }}}}/{TRACKDECHETS_SIRENE_SEARCH_GIT} && npm install --quiet && npm run build",
    )

    download_es_ca_pem = BashOperator(
        task_id="download_es_ca_pem",
        bash_command=f"cd {{{{ ti.xcom_pull(task_ids='create_tmp_dir') }}}}/{TRACKDECHETS_SIRENE_SEARCH_GIT}/dist/common && curl -o es.cert {environ['ELASTICSEARCH_CAPEM']}",
    )

    npm_run_index = BashOperator(
        task_id="npm_run_index",
        bash_command=f"cd {{{{ ti.xcom_pull(task_ids='create_tmp_dir') }}}}/{TRACKDECHETS_SIRENE_SEARCH_GIT} && npm run index:siret"
        if environ["INDEX_SIRET_ONLY"] == "true"
        else f"cd {{{{ ti.xcom_pull(task_ids='create_tmp_dir') }}}}/{TRACKDECHETS_SIRENE_SEARCH_GIT} && npm run index",
        env=environ,
    )

    @task
    def task_cleanup_tmp_files(tmp_dir: str):
        """Clean DAG's artifacts"""
        shutil.rmtree(tmp_dir)

    """
    Dag workflow
    """
    (
        tmp_dir
        >> git_clone_trackdechets
        >> npm_install_build
        >> download_es_ca_pem
        >> npm_run_index
        >> task_cleanup_tmp_files(tmp_dir)
    )


trackdechets_search_sirene_v2_dag = full_update_search_sirene_v2()


if __name__ == "__main__":
    trackdechets_search_sirene_v2_dag.test()
