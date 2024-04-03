import shutil
import subprocess
import tempfile
from typing import Any
import urllib
from asyncio.log import logger
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.models import Connection, Variable, Param
from airflow.utils.trigger_rule import TriggerRule

from etl_insee.schema import StockEtablissement, db
from mattermost import mm_failed_task


@dag(
    schedule_interval="@monthly",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    on_failure_callback=mm_failed_task,
    params={
        "force_recreate_table": Param(
            False,
            type="boolean",
        ),
    },
)
def base_sirene_etl():
    """DAG qui met à jour la base SIRENE dans le Data Warehouse Trackdéchets"""

    @task
    def extract_stock_etablissement() -> str:
        url = Variable.get("BASE_SIRENE_URL")

        tmp_dir = Path(tempfile.mkdtemp(prefix="base_sirene_ETL"))

        urllib.request.urlretrieve(url, tmp_dir / "stock_etablissement.zip")
        shutil.unpack_archive(tmp_dir / "stock_etablissement.zip", tmp_dir)

        return str(tmp_dir)

    @task
    def update_table(tmp_dir: str, params: dict[str, Any] = None):
        tmp_dir = Path(tmp_dir)
        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_uri = airflow_con.get_uri().replace("postgres", "postgresql")

        with db.connection_context():
            if params["force_recreate_table"]:
                logger.info("Force re-creation of table.")
                StockEtablissement.drop_table(safe=True, cascade=True)
                db.create_tables([StockEtablissement])

            if not StockEtablissement.table_exists():
                logger.info("Table does not exists, creation of the table.")
                db.create_tables([StockEtablissement])

            logger.info("Truncating the table.")
            StockEtablissement.truncate_table()

        copy_command = f"psql {sql_uri} -c \"\\copy raw_zone_insee.stock_etablissement from '{str(tmp_dir/'StockEtablissementS4F3_utf8.csv')}' WITH (FORMAT csv, HEADER true);\""
        logger.info("Beginning the insertion of the data.")
        completed_process = subprocess.run(
            copy_command, check=True, capture_output=True, shell=True
        )
        logger.info(completed_process)

    # @task(trigger_rule=TriggerRule.ALL_DONE)
    # def cleanup_tmp_files(tmp_dir: str):
    #     shutil.rmtree(tmp_dir)

    tmp_dir = extract_stock_etablissement()
    update_table(tmp_dir)


base_sirene_etl_dag = base_sirene_etl()

if __name__ == "__main__":
    base_sirene_etl_dag.test()
