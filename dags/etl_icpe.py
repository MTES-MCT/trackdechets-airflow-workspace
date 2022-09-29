import logging
import shutil
import tarfile
import tempfile
import urllib
from datetime import datetime
from pathlib import Path

import pandas as pd
import pendulum
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from sqlalchemy import create_engine

logger = logging.getLogger()


DF_READ_CONFIG = {
    "IC_etablissement.csv": {
        "names": [
            "codeS3ic",
            "s3icNumeroSiret",
            "x",
            "y",
            "region",
            "nomEts",
            "codeCommuneEtablissement",
            "codePostal",
            # 1 = en construction, 2 = en fonctionnement, 3 = à l'arrêt, 4 = cessation déclarée, 5 = Récolement fait
            "etatActivite",
            "codeApe",
            "nomCommune",
            "seveso",
            "regime",
            "prioriteNationale",
            # cf. biblio https://aida.ineris.fr/node/193
            "ippc",
            # Etablissement soumis à la déclaration annuelle d'émissions polluantes et de déchets
            "declarationAnnuelle",
            # IN = industrie, BO = bovins, PO = porcs, VO = volailles, CA = carrières
            "familleIc",
            # 1 + 1 = DREAL, etc.
            "baseIdService",
            "natureIdService",
            "adresse1",
            "adresse2",
            "dateInspection",
            # Sites et sols pollués:
            "indicationSsp",
            "rayon",
            "precisionPositionnement",
        ],
        "dtype": str,
    },
    "IC_installation_classee.csv": {
        "names": [
            "codeS3ic",
            "id",
            "volume",
            "unite",
            "date_debut_exploitation",
            "date_fin_validite",
            "statut_ic",
            "id_ref_nomencla_ic",
        ],
        "dtype": str,
    },
    "IC_ref_nomenclature_ic.csv": {
        "names": [
            "id",
            "rubrique_ic",
            "famille_ic",
            "sfamille_ic",
            "ssfamille_ic",
            "alinea",
            "libellecourt_activite",
            "id_regime",
            "envigueur",
            "ippc",
        ],
        "dtype": str,
    },
}


@dag(
    start_date=pendulum.datetime(2022, 7, 1, 3, tz="Europe/Paris"),
    schedule_interval="0 0 * * 1",
    catchup=False,
)
def etl_icpe():
    @task
    def extract_data() -> str:
        url = Variable.get("ICPE_URL")

        files_to_extract = [
            "IC_etablissement.csv",
            "IC_installation_classee.csv",
            "IC_ref_nomenclature_ic.csv",
        ]

        res = {}
        with urllib.request.urlopen(url) as response:

            with tempfile.NamedTemporaryFile() as tmp_file:
                shutil.copyfileobj(response, tmp_file)

                with tarfile.open(tmp_file.name, "r:gz") as tar:
                    for member in tar.getmembers():
                        if member.name == "version":
                            version_info = tar.extractfile(member).readlines()
                            logger.info(f"ICPE file version info: {version_info}")
                        if member.name in files_to_extract:
                            config = DF_READ_CONFIG[member.name]
                            res[member.name] = pd.read_csv(
                                tar.extractfile(member), sep=";", **config
                            )

                            logger.info(
                                f"{member.name} dataframe shape: {res[member.name].shape}"
                            )

        tmp_dir = Path(tempfile.mkdtemp(prefix="etl_icpe"))

        for filename, df in res.items():
            df.to_pickle(tmp_dir / f"{filename[:-4]}.pkl")

        return str(tmp_dir)

    @task()
    def load(tmp_dir: str):

        configs = [
            {"filename": "IC_etablissement.pkl", "table_name": "ic_etablissement"},
            {
                "filename": "IC_installation_classee.pkl",
                "table_name": "ic_installation_classee",
            },
            {
                "filename": "IC_ref_nomenclature_ic.pkl",
                "table_name": "ic_ref_nomenclature_ic",
            },
        ]

        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )

        inserted_at = datetime.utcnow()

        logger.info("New insertion date : %s", inserted_at)

        tmp_dir = Path(tmp_dir)
        for config in configs:
            df_path = tmp_dir / config["filename"]
            df = pd.read_pickle(df_path)
            df["inserted_at"] = inserted_at
            logger.info(
                f"Loading {config['filename']} dataframe shape: {df.shape} into table {config['table_name']}"
            )
            df.to_sql(
                name=config["table_name"],
                con=sql_engine,
                if_exists="append",
                index=False,
                schema="raw_zone_icpe",
            )

    @task
    def cleanup_tmp_files(tmp_dir: str):

        shutil.rmtree(tmp_dir)

    dataframes_tmp_dir = extract_data()
    load(dataframes_tmp_dir) >> cleanup_tmp_files(dataframes_tmp_dir)


etl_icpe_dag = etl_icpe()
