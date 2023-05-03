import logging
import shutil
import subprocess
import tempfile
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from mattermost import mm_failed_task

logging.basicConfig()
logger = logging.getLogger()

es_connection = Connection.get_connection_from_secrets(
    "trackdechets_search_sirene_elasticsearch_url"
)

es_credentials = ""
if es_connection.login and es_connection.password:
    es_credentials = f"{es_connection.login}:{es_connection.password}@"

es_schema = "http"
if es_connection.schema:
    es_schema = f"{es_connection.schema}"

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
}

trackdechets_sirene_search_git = "trackdechets-sirene-search"

@dag(
    schedule_interval="0 22 2 * *",
    catchup=False,
    start_date=datetime(2022, 12, 1),
    on_failure_callback=mm_failed_task,
)
def trackdechets_search_sirene():
    """DAG permettant d'indexer la base SIRENE de l'INSEE dans ElasticSearch"""

    @task
    def git_clone_trackdechets() -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="trackdechets_search_sirene"))
        clone_command = f"git clone https://github.com/MTES-MCT/{trackdechets_sirene_search_git}.git"
        completed_process = subprocess.run(
            clone_command, check=True, capture_output=True, shell=True, cwd=tmp_dir
        )
        logger.info(completed_process)
        return str(tmp_dir)

    @task
    def npm_install_build(tmp_dir) -> str:
        """
        npm install && npm run build
        """
        tmp_dir = Path(tmp_dir)
        install_command = "npm install --quiet"
        completed_process = subprocess.run(
            install_command,
            check=False,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / trackdechets_sirene_search_git,
        )
        logger.info(completed_process.stderr)
        logger.info(completed_process.stdout)
        if completed_process.returncode != 0:
            raise Exception(completed_process)

        build_command = "npm run build"
        completed_process = subprocess.run(
            build_command,
            check=False,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / trackdechets_sirene_search_git,
        )
        logger.info(completed_process.stderr)
        logger.info(completed_process.stdout)
        if completed_process.returncode != 0:
            raise Exception(completed_process)

        return str(tmp_dir)

    @task
    def download_es_ca_pem(tmp_dir) -> str:
        """Download certificate needed for ElasticSearch connection."""
        tmp_dir = Path(tmp_dir)
        curl = f"curl -o es.cert {environ['ELASTICSEARCH_CAPEM']}"
        completed_process = subprocess.run(
            curl,
            check=True,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / trackdechets_sirene_search_git / "dist" / "src" / "common",
        )
        logger.info(completed_process)
        return str(tmp_dir)

    @task
    def npm_run_index(tmp_dir) -> str:
        """
        npm run index
        """
        tmp_dir = Path(tmp_dir)
        index_command = "npm run index"
        process = subprocess.Popen(
            index_command,
            shell=True,
            cwd=tmp_dir / trackdechets_sirene_search_git,
            env=environ,
            stdout=subprocess.PIPE,
        )

        while True:
            line = process.stdout.readline()
            if not line:
                break
            logging.info(line.rstrip().decode("utf-8"))

        while process.wait():
            if process.returncode != 0:
                raise Exception(process)

        return str(tmp_dir)

    @task
    def cleanup_tmp_files(tmp_dir: str):
        """Clean DAG's artifacts"""
        shutil.rmtree(tmp_dir)

    tmp_dir = git_clone_trackdechets()
    npm_install_build(tmp_dir) >> download_es_ca_pem(tmp_dir) >> npm_run_index(
        tmp_dir
    ) >> cleanup_tmp_files(tmp_dir)


trackdechets_search_sirene_dag = trackdechets_search_sirene()
