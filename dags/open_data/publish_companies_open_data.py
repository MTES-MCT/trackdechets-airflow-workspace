from datetime import datetime

import pandas as pd
import requests
from logger import logging
from mattermost import mm_failed_task

from airflow.decorators import dag, task
from airflow.models import Connection, Variable

logger = logging.getLogger(__name__)


@dag(
    start_date=datetime(2022, 2, 7),
    schedule_interval="@daily",
    user_defined_macros={},
    catchup=False,
    on_failure_callback=mm_failed_task,
)
def publish_companies_open_data():
    """
    DAG dedicated to the loading of a subset of company data to data.gouv.fr
    """

    @task()
    def extract_transform_and_load_company_data():
        from sqlalchemy import create_engine

        airflow_con = Connection.get_connection_from_secrets("td_prod_postgres")

        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )

        df_company = pd.read_sql_query(
        """
        SELECT 
            "Company"."siret", 
            cast("Company"."createdAt" as date) as date_inscription,
            cast("Company"."updatedAt" as date) as date_derniere_mise_a_jour,
            "Company"."companyTypes", 
            "Company"."name" as nom, 
            "Company"."verificationStatus"
        FROM "default$default"."Company"
        """,
            con=sql_engine,
            dtype={"siret": str},
            index_col="siret",
        )

        logger.info(f"Number of établissements unfiltered: {df_company.index.size}")

        # Filter and drop columns
        df_company = df_company.loc[
            :,
            ~df_company.columns.isin(["verificationStatus", "companyTypes"]),
        ]

        # Print stats
        logger.info(f"Number of établissements filtered: {df_company.index.size}")

        df_anonymous = pd.read_sql_query(
            """
        SELECT "AnonymousCompany"."siret"
        FROM "default$default"."AnonymousCompany"
        """,
            con=sql_engine,
            dtype={"siret": str},
        )

        df_anonymous["non_diffusible"] = "oui"

        # Add the non_diffusible column
        company_filtered_anonymous = df_company.join(
            df_anonymous.set_index("siret"), how="left"
        )

        api_key = Variable.get("DATAGOUVFR_API_KEY")
        dataset_id = Variable.get("ETABLISSEMENTS_DATASET_ID")
        resource_id = Variable.get("ETABLISSEMENTS_RESOURCE_ID")

        response = requests.post(
            url=f"https://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/upload",
            headers={"X-API-KEY": api_key},
            files={
                "file": (
                    "etablissements_inscrits.csv",
                    company_filtered_anonymous.to_csv(),
                )
            },
        )

        logger.info(f"Data.gouv response : {response.text}")

    extract_transform_and_load_company_data()


publish_companies_open_data_dag = publish_companies_open_data()

if __name__ == "__main__":
    publish_companies_open_data_dag.test()
