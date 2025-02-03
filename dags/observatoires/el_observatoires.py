import logging
import shutil
import tempfile
import urllib
from datetime import datetime
from pathlib import Path

from airflow.models import Connection, Variable
import duckdb
from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

from mattermost import mm_failed_task

logging.basicConfig()
logger = logging.getLogger(__name__)


configs = {
    "bsdd": {
        "table_name": "refined_zone_observatoires.bsdd_observatoires",
        "date_expr": "date_part('year',coalesce(emetteur_date_signature_emission,date_creation))",
    },
    "bsda": {
        "table_name": "refined_zone_observatoires.bsda_observatoires",
        "date_expr": "date_part('year',coalesce(entreprise_travaux_date_signature,emetteur_date_signature_emission,date_creation))",
    },
    "bsff": {
        "table_name": "refined_zone_observatoires.bsff_observatoires",
        "date_expr": "date_part('year',coalesce(emetteur_date_signature_emission,date_creation))",
    },
    "bsdasri": {
        "table_name": "refined_zone_observatoires.bsdasri_observatoires",
        "date_expr": "date_part('year',coalesce(emetteur_date_signature_emission,date_creation))",
    },
    "bsvhu": {
        "table_name": "refined_zone_observatoires.bsvhu_observatoires",
        "date_expr": "date_part('year',coalesce(emetteur_date_signature_emission,date_creation))",
    },
}


@dag(
    schedule_interval="30 16 1 * *",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    on_failure_callback=mm_failed_task,
)
def el_observatoires():
    """DAG qui met à jour la base SIRENE dans le Data Warehouse Clickhouse Trackdéchets."""

    @task
    def extract_from_dwh_and_load_to_s3():
        dwh_con = Connection.get_connection_from_secrets("td_datawarehouse").to_dict()
        s3_key_id = Variable.get("GERICO_S3_KEY_ID")
        s3_secret = Variable.get("GERICO_S3_SECRET")

        s3_create_secret_stmt = f"""
        CREATE SECRET gerico_s3 (
            TYPE S3,
            KEY_ID '{s3_key_id}',
            SECRET '{s3_secret}',
            REGION 'fr-par',
            ENDPOINT 's3.fr-par.scw.cloud'
        );
        """

        dwh_attach_db_stmt = (
            f"ATTACH 'dbname={dwh_con['schema']} "
            f"user={dwh_con['login']} "
            f"password={dwh_con['password']} "
            f"host={dwh_con['host']} "
            f"port={dwh_con['port']}' "
            "AS dwh (TYPE POSTGRES, READ_ONLY);"
        )

        con = duckdb.connect()
        con.install_extension("postgres")
        con.load_extension("postgres")
        con.install_extension("httpfs")
        con.load_extension("httpfs")
        con.sql(s3_create_secret_stmt)
        con.sql(dwh_attach_db_stmt)

        for bs_type, config in configs.items():
            date_expr = config["date_expr"]
            table_name = config["table_name"]

            logger.info("Starting copying full table %s to S3.", bs_type)
            copy_stmt = f"""
            COPY (select * from dwh.{table_name}) TO 's3://donnees/{bs_type}/{bs_type}.parquet' (
                FORMAT PARQUET,
                COMPRESSION zstd,
                OVERWRITE_OR_IGNORE true
            );
            """
            con.sql(copy_stmt)
            logger.info("Finished copying full table %s to S3.", bs_type)

            years = list(range(2022, 2026))
            for i, year in enumerate(years):
                operator = "="
                if i == 0:
                    operator = "<="
                if i == (len(years) - 1):
                    operator = ">="

                logger.info(
                    "Starting copying table %s to S3 for year %s", bs_type, year
                )
                copy_stmt = f"""
                COPY (select * from dwh.{table_name} WHERE {date_expr}{operator}{year}) TO 's3://donnees/{bs_type}/{bs_type}_{year}.parquet' (
                    FORMAT PARQUET,
                    COMPRESSION zstd,
                    OVERWRITE_OR_IGNORE true
                );
                """
                con.sql(copy_stmt)
                logger.info(
                    "Finished copying full table %s to S3 for year %s", bs_type, year
                )

    extract_from_dwh_and_load_to_s3()


el_observatoires_dag = el_observatoires()

if __name__ == "__main__":
    el_observatoires_dag.test()
