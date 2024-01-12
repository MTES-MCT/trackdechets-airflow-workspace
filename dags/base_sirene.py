from asyncio.log import logger
import shutil
import tempfile
import urllib
from datetime import datetime
from pathlib import Path
import subprocess

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

from mattermost import mm_failed_task


@dag(
    schedule_interval="@monthly",
    catchup=False,
    start_date=datetime(2022, 8, 31),
    on_failure_callback=mm_failed_task,
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
    def update_table(tmp_dir: str):

        tmp_dir = Path(tmp_dir)

        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_uri = airflow_con.get_uri().replace("postgres", "postgresql")
        sql_engine = create_engine(sql_uri)
        con = sql_engine.connect()
        table_creation_query = """
CREATE TABLE IF NOT EXISTS raw_zone_insee.stock_etablissement (
	siren varchar NULL,
	nic varchar NULL,
	siret varchar NULL,
	"statutDiffusionEtablissement" varchar NULL,
	"dateCreationEtablissement" varchar NULL,
	"trancheEffectifsEtablissement" varchar NULL,
	"anneeEffectifsEtablissement" varchar NULL,
	"activitePrincipaleRegistreMetiersEtablissement" varchar NULL,
	"dateDernierTraitementEtablissement" varchar NULL,
	"etablissementSiege" varchar NULL,
	"nombrePeriodesEtablissement" varchar NULL,
	"complementAdresseEtablissement" varchar NULL,
	"numeroVoieEtablissement" varchar NULL,
	"indiceRepetitionEtablissement" varchar NULL,
	"typeVoieEtablissement" varchar NULL,
	"libelleVoieEtablissement" varchar NULL,
	"codePostalEtablissement" varchar NULL,
	"libelleCommuneEtablissement" varchar NULL,
	"libelleCommuneEtrangerEtablissement" varchar NULL,
	"distributionSpecialeEtablissement" varchar NULL,
	"codeCommuneEtablissement" varchar NULL,
	"codeCedexEtablissement" varchar NULL,
	"libelleCedexEtablissement" varchar NULL,
	"codePaysEtrangerEtablissement" varchar NULL,
	"libellePaysEtrangerEtablissement" varchar NULL,
	"complementAdresse2Etablissement" varchar NULL,
	"numeroVoie2Etablissement" varchar NULL,
	"indiceRepetition2Etablissement" varchar NULL,
	"typeVoie2Etablissement" varchar NULL,
	"libelleVoie2Etablissement" varchar NULL,
	"codePostal2Etablissement" varchar NULL,
	"libelleCommune2Etablissement" varchar NULL,
	"libelleCommuneEtranger2Etablissement" varchar NULL,
	"distributionSpeciale2Etablissement" varchar NULL,
	"codeCommune2Etablissement" varchar NULL,
	"codeCedex2Etablissement" varchar NULL,
	"libelleCedex2Etablissement" varchar NULL,
	"codePaysEtranger2Etablissement" varchar NULL,
	"libellePaysEtranger2Etablissement" varchar NULL,
	"dateDebut" varchar NULL,
	"etatAdministratifEtablissement" varchar NULL,
	"enseigne1Etablissement" varchar NULL,
	"enseigne2Etablissement" varchar NULL,
	"enseigne3Etablissement" varchar NULL,
	"denominationUsuelleEtablissement" varchar NULL,
	"activitePrincipaleEtablissement" varchar NULL,
	"nomenclatureActivitePrincipaleEtablissement" varchar NULL,
	"caractereEmployeurEtablissement" varchar NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS stock_etablissement_siret_idx ON raw_zone_insee.stock_etablissement USING btree (siret);
TRUNCATE TABLE raw_zone_insee.stock_etablissement;
"""

        con.execute(table_creation_query)

        copy_command = f"psql {sql_uri} -c \"\\copy raw_zone_insee.stock_etablissement from '{str(tmp_dir/'StockEtablissement_utf8.csv')}' WITH (FORMAT csv, HEADER true);\""
        completed_process = subprocess.run(
            copy_command, check=True, capture_output=True, shell=True
        )
        logger.info(completed_process)

    @task
    def cleanup_tmp_files(tmp_dir: str):

        shutil.rmtree(tmp_dir)

    tmp_dir = extract_stock_etablissement()
    update_table(tmp_dir) >> cleanup_tmp_files(tmp_dir)


base_sirene_etl_dag = base_sirene_etl()
