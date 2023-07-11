import logging
from datetime import datetime

from sqlalchemy import create_engine
import pandas as pd
import requests
from airflow.decorators import dag, task
from airflow.models import Connection, Variable

from mattermost import mm_failed_task

logger = logging.getLogger()


@dag(
    start_date=datetime(2023, 2, 7, 10),
    schedule_interval="@daily",
    catchup=False,
    on_failure_callback=mm_failed_task,
)
def extract_transform_and_load_georisques():
    """
    DAG dedicated to the loading of a subset of company data to data.gouv.fr
    """

    @task()
    def get_last_update_date() -> str:
        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )

        res = sql_engine.execute(
            "SELECT max(date_modification) FROM raw_zone_icpe.installations"
        )
        date_max: str = res.first()[0][:10]
        logger.info("Least recent date in the datawarehouse is %s", date_max)

        return date_max

    @task()
    def extract_data_from_georisques_api(date_modification: str) -> list[dict]:
        # Première requête pour récupérer le nombre de pages
        base_url = "https://www.georisques.gouv.fr/api/v1/installations_classees"
        date_maj = date_modification
        res = requests.get(f"{base_url}?&page_size=100&dateMaj={date_maj}&page=1")

        res_json = res.json()

        total_pages_to_get = res_json["total_pages"]
        logger.info("Number of pages to fetch is %s", total_pages_to_get)

        data = res_json["data"]

        for page in range(2, total_pages_to_get + 1):
            url = f"{base_url}?&page_size=100&dateMaj={date_maj}&page={page}"
            res = requests.get(url)
            data.extend(res.json()["data"])

        logger.info("Retrieved data, len of data is %s", len(data))

        return data

    @task()
    def transform_and_load_installations_data(data: list[dict]):
        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )

        df: pd.DataFrame = pd.DataFrame.from_dict(data)

        col_mapping = {
            "codeAIOT": "code_aiot",
            "siret": "num_siret",
            "coordonneeXAIOT": "x",
            "coordonneeYAIOT": "y",
            "adresse1": "adresse1",
            "adresse2": "adresse2",
            "adresse3": "adresse3",
            "codePostal": "code_postal",
            "codeInsee": "code_insee",
            "commune": "commune",
            "raisonSociale": "raison_sociale",
            "etatActivite": "etat_activite",
            "codeNaf": "code_naf",
            "prioriteNationale": "priorite_nationale",
            "ied": "ied",
            "serviceAIOT": "type_service_aiot",
            "bovins": "bovins",
            "porcs": "porcs",
            "volailles": "volailles",
            "carriere": "carriere",
            "eolienne": "eolienne",
            "industrie": "industrie",
            "longitude": "longitude",
            "latitude": "latitude",
            "date_maj": "date_modification",
        }

        df = df.rename(columns=col_mapping)

        logger.info("Beginning upsert of installations data")

        df[
            [
                "raison_sociale",
                "adresse1",
                "code_postal",
                "code_insee",
                "commune",
                "longitude",
                "latitude",
                "bovins",
                "porcs",
                "volailles",
                "code_aiot",
                "carriere",
                "eolienne",
                "industrie",
                "priorite_nationale",
                "ied",
                "etat_activite",
                "code_naf",
                "num_siret",
                "x",
                "y",
                "type_service_aiot",
                "regime",
                "date_modification",
                "adresse2",
                "adresse3",
            ]
        ].to_sql(
            "installations_temp",
            con=sql_engine,
            schema="raw_zone_icpe",
            index=False,
            if_exists="replace",
        )

        q_res = sql_engine.execute(
            """
        insert
            into
            raw_zone_icpe.installations (
            code_aiot,
            num_siret,
            x,
            y,
            adresse1,
            adresse2,
            adresse3,
            code_postal,
            code_insee,
            commune,
            raison_sociale,
            etat_activite,
            code_naf,
            priorite_nationale,
            ied,
            type_service_aiot,
            bovins,
            porcs,
            volailles,
            carriere,
            eolienne,
            industrie,
            longitude,
            latitude,
            regime,
            date_modification)
        select
            code_aiot,
            num_siret,
            x,
            y,
            adresse1,
            adresse2,
            adresse3,
            code_postal,
            code_insee,
            commune,
            raison_sociale,
            etat_activite,
            code_naf,
            priorite_nationale,
            ied,
            type_service_aiot,
            bovins,
            porcs,
            volailles,
            carriere,
            eolienne,
            industrie,
            longitude,
            latitude,
            regime,
            date_modification
        from
            raw_zone_icpe.installations_temp
        on
            conflict (code_aiot) 
        do
        update
        set
            "num_siret" = EXCLUDED."num_siret",
            "x" = EXCLUDED."x",
            "y" = EXCLUDED."y",
            "adresse1" = EXCLUDED."adresse1",
            "adresse2" = EXCLUDED."adresse2",
            "adresse3" = EXCLUDED."adresse3",
            "code_postal" = EXCLUDED."code_postal",
            "code_insee" = EXCLUDED."code_insee",
            "commune" = EXCLUDED."commune",
            "raison_sociale" = EXCLUDED."raison_sociale",
            "etat_activite" = EXCLUDED."etat_activite",
            "priorite_nationale" = EXCLUDED."priorite_nationale",
            "ied" = EXCLUDED."ied",
            "type_service_aiot" = EXCLUDED."type_service_aiot",
            "bovins" = EXCLUDED."bovins",
            "porcs" = EXCLUDED."porcs",
            "volailles" = EXCLUDED."volailles",
            "carriere" = EXCLUDED."carriere",
            "eolienne" = EXCLUDED."eolienne",
            "industrie" = EXCLUDED."industrie",
            "longitude" = EXCLUDED."longitude",
            "latitude" = EXCLUDED."latitude",
            "regime" = EXCLUDED."regime",
            "date_modification" = EXCLUDED."date_modification"
            """
        )

        logger.info("Finished upsert of data")

    @task()
    def transform_and_load_rubriques_data(data: list[dict]):
        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )

        rubriques_data = [
            {"code_aiot": e["codeAIOT"], **rubrique}
            for e in data
            if e
            for rubrique in e["rubriques"]
        ]

        df: pd.DataFrame = pd.DataFrame.from_dict(rubriques_data)

        col_mapping = {
            "numeroRubrique": "rubrique",
            "regimeAutoriseAlinea": "regime",
            "quantiteTotale": "quantite_totale",
        }

        df = df.rename(columns=col_mapping)

        code_aiot_str = ",".join(("'" + df["code_aiot"] + "'").unique())

        logger.info("Beginning upsert of rubriques data")

        sql_engine.execute(
            f"DELETE FROM raw_zone_icpe.rubriques WHERE code_aiot in ({code_aiot_str})"
        )

        df.to_sql(
            "rubriques",
            con=sql_engine,
            schema="raw_zone_icpe",
            index=False,
            if_exists="append",
        )
        logger.info("Finished upsert of rubriques data")

    @task()
    def clean_artifacts():
        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )

        sql_engine.execute("DROP TABLE raw_zone_icpe.installations_temp")

    date = get_last_update_date()
    georisque_data = extract_data_from_georisques_api(date)
    transform_and_load_installations_data(georisque_data)
    transform_and_load_rubriques_data(georisque_data) >> clean_artifacts()


extract_transform_and_load_georisques_dag = extract_transform_and_load_georisques()
