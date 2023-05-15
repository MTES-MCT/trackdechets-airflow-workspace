import logging
import shutil
import subprocess
import tempfile
from datetime import datetime
from datetime import timedelta
from pathlib import Path

from pynsee.sirene import search_sirene
from pynsee.utils.init_conn import init_conn

from requests.exceptions import RequestException

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from mattermost import mm_failed_task

from utils import download_es_ca_pem, git_clone_trackdechets, npm_install_build


logging.basicConfig()
logger = logging.getLogger()


# Constant pointing to the node git indexation repo
TRACKDECHETS_SIRENE_SEARCH_GIT = Variable.get("TRACKDECHETS_SIRENE_SEARCH_GIT")

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
    "INSEE_KEY": Variable.get("INSEE_KEY"),
    "INSEE_SECRET": Variable.get("INSEE_SECRET"),
    "ELASTICSEARCH_CAPEM": Variable.get("ELASTICSEARCH_CAPEM"),
}


@dag(
    schedule_interval="0 4 * * *",
    catchup=False,
    start_date=datetime.now(),
    on_failure_callback=mm_failed_task,
)
def trackdechets_update_daily_sirene():
    """
    DAG permettant d'indexer les dernières modifications
    de SIRENE durant les 24h passées dans ElasticSearch
    https://api.insee.fr/catalogue/site/themes/wso2/subthemes/insee/pages/item-info.jag?name=Sirene&version=V3&provider=insee
    """

    @task
    def task_init_connection() -> str:
        """
        generate and cache an access token
        (only valid for a period of time)
        """
        init_conn(insee_key=environ["INSEE_KEY"], insee_secret=environ["INSEE_SECRET"])

        output_path = Path(tempfile.mkdtemp(prefix="trackdechets_update_daily_sirene"))
        return str(output_path)

    @task
    def task_git_clone_trackdechets(tmp_dir) -> str:
        return git_clone_trackdechets(tmp_dir, TRACKDECHETS_SIRENE_SEARCH_GIT)

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
    def task_query_and_index(tmp_dir) -> str:
        """
        query INSEE Sirene api the run index
        """
        tmp_dir = Path(tmp_dir)
        # get current date and time
        now = datetime.now()
        # calculate 24 hours ago
        twenty_four_hours_ago = now - timedelta(hours=24)
        pattern = f"{twenty_four_hours_ago.strftime('%Y-%m-%d')}%20TO%20{now.strftime('%Y-%m-%d')}"
        try:
            df = search_sirene(
                variable=["dateDernierTraitementEtablissement"],
                pattern=[f"[{pattern}]"],
                kind="siret",
            )
            # print the items
            path_or_buf = tmp_dir / f"{pattern}.csv"
            df.to_csv(path_or_buf=path_or_buf)

            index_command = f"npm run index:siret:csv -- {path_or_buf}"
            process = subprocess.Popen(
                index_command,
                shell=True,
                cwd=tmp_dir / TRACKDECHETS_SIRENE_SEARCH_GIT,
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
        except RequestException as error:
            if error.errno == 404:
                print("nothing")
                pass
            else:
                raise Exception(error)

        return str(tmp_dir)

    @task
    def task_cleanup_tmp_files(tmp_dir: str):
        """Clean DAG's artifacts"""
        shutil.rmtree(tmp_dir)

    """
    Dag workflow
    """
    tmp_dir = task_init_connection()
    (
        task_npm_install_build(tmp_dir)
        >> task_download_es_ca_pem(tmp_dir)
        >> task_query_and_index(tmp_dir)
        >> task_cleanup_tmp_files(tmp_dir)
    )


trackdechets_search_sirene_dag = trackdechets_update_daily_sirene()
