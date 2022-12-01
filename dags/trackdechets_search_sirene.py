import logging
from pathlib import Path
import shutil
import subprocess
import tempfile
from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable, Connection


logging.basicConfig()
logger = logging.getLogger()

environ = {
    "FORCE_LOGGER_CONSOLE": Variable.get("FORCE_LOGGER_CONSOLE"),
    "ELASTICSEARCH_URL": Connection.get_connection_from_secrets(
        "trackdechets_search_sirene_elasticsearch_url"
    ).get_uri(),
    "DD_LOGS_ENABLED": Variable.get("DD_LOGS_ENABLED"),
    "DD_TRACE_ENABLED": Variable.get("DD_TRACE_ENABLED"),
    "DD_API_KEY": Variable.get("DD_API_KEY"),
    "DD_APP_NAME": Variable.get("DD_APP_NAME"),
    "DD_ENV": Variable.get("DD_ENV"),
    "NODE_ENV": Variable.get("NODE_ENV"),
}


@dag(schedule_interval=None, catchup=False, start_date=datetime.now())
def trackdechets_search_sirene():
    """Dag qui tue"""

    @task
    def git_clone_trackdechets() -> str:
        tmp_dir = Path(tempfile.mkdtemp(prefix="trackdechets_search_sirene"))
        clone_command = "git clone https://github.com/MTES-MCT/trackdechets.git"
        completed_process = subprocess.run(
            clone_command, check=True, capture_output=True, shell=True,
            cwd=tmp_dir
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
            check=True,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / "trackdechets" / "search",
        )
        logger.info(completed_process)
        build_command = "npm run build"
        completed_process = subprocess.run(
            build_command,
            check=True,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / "trackdechets" / "search",
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
        completed_process = subprocess.run(
            index_command,
            check=True,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / "trackdechets" / "search",
            env=environ,
        )
        logger.info(completed_process)
        return str(tmp_dir)

    @task
    def cleanup_tmp_files(tmp_dir: str):
        shutil.rmtree(tmp_dir)

    tmp_dir = git_clone_trackdechets()
    npm_install_build(tmp_dir) >> npm_run_index(tmp_dir) >> cleanup_tmp_files(tmp_dir)


trackdechets_search_sirene_dag = trackdechets_search_sirene()
