import asyncio
import base64
from datetime import datetime

import aiohttp
import pandas as pd
from mattermost import mm_failed_task
from sqlalchemy import create_engine

from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from logger import logging

logger = logging.getLogger(__name__)


def bordereaux_list_to_base64_csv(bordereaux: list[dict]) -> str:
    bordereaux_df = pd.DataFrame.from_dict(bordereaux)
    bordereaux_df = bordereaux_df.sort_values("received_at").rename(
        columns={
            "quantity": "Quantité en tonne",
            "readable_id": "Numéro du bordereau",
            "received_at": "Date de réception",
            "bordereau_type": "Type de bordereau",
        }
    )
    bordereaux_csv_string = (
        bordereaux_df[
            [
                "Numéro du bordereau",
                "Type de bordereau",
                "Quantité en tonne",
                "Date de réception",
            ]
        ]
        .to_csv(index=False)
        .encode("utf-8")
    )
    bordereaux_csv_base64 = base64.b64encode(bordereaux_csv_string).decode()

    return bordereaux_csv_base64


async def send_email_async(client: aiohttp.ClientSession, url: str, body: dict) -> dict:
    async with session.post(url, json=body) as response:
        logger.info(
            "Sending e-mail(s) to %s, email adresses : % s",
            body["params"]["nom_societe"],
            body["to"],
        )
        return await response.json()


async def send_emails(df: pd.DataFrame):
    base_url = "https://api.brevo.com/v3/smtp/email"
    api_key = Variable.get("SIB_API_KEY")

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "api-key": api_key,
    }

    async with aiohttp.ClientSession(
        headers=headers, connector=aiohttp.TCPConnector(limit_per_host=10)
    ) as session:
        for row in df.itertuples(index=False):
            emails = row.user_emails

            nom_etablissement = row.company_name
            siret = row.siret
            num_bordereaux = len(row.bordereaux)

            bordereaux_csv = bordereaux_list_to_base64_csv(row.bordereaux)

            body = {
                "to": [{"email": email} for email in emails],
                "templateId": 315,
                "replyTo": {
                    "email": "contact@mail.trackdechets.beta.gouv.fr",
                    "name": "Trackdéchets",
                },
                "params": {
                    "nom_societe": nom_etablissement,
                    "siret": siret,
                    "num_bordereaux": num_bordereaux,
                },
                "attachment": [
                    {
                        "content": bordereaux_csv,
                        "name": "bordereaux.csv",
                    }
                ],
            }

            try:
                response = await send_email_async(session, base_url, body)
            except Exception as e:
                logger.error(
                    "Error sending the message to e-mail : %s with exception : %s",
                    emails,
                    e,
                )

            logger.info(
                "Successfully sent request to Brevo with response : %s", response
            )

            await asyncio.sleep(0.1)  # Apply rate-limiting between API requests


@dag(
    start_date=datetime(2023, 6, 1),
    schedule_interval=None,
    catchup=False,
    on_failure_callback=mm_failed_task,
)
def send_emails_to_companies_with_outliers():
    """
    DAG dedicated to send an email to companies that have been mentioned on a "bordereaux" that have an outlier quantity.
    """

    @task()
    def get_data_from_dwh() -> str:
        airflow_con = Connection.get_connection_from_secrets("td_datawarehouse")
        sql_engine = create_engine(
            airflow_con.get_uri().replace("postgres", "postgresql")
        )
        with sql_engine.connect() as connection:
            df = pd.read_sql_table(
                con=connection,
                table_name="etablisements_avec_bordereaux_valeurs_aberrantes",
                schema="refined_zone_analytics",
            )

        logger.info(
            "Retrieved data from the DWH, numbers of message to be sent : %s", len(df)
        )

        return df.to_json()

    @task()
    def send_emails_with_brevo(df: str):
        df: pd.DataFrame = pd.read_json(df, dtype={"siret": str})

        loop = asyncio.get_event_loop()
        loop.run_until_complete(send_emails(df))

    df = get_data_from_dwh()
    send_emails_with_brevo(df)


send_emails_to_companies_with_outliers_dag = send_emails_to_companies_with_outliers()

if __name__ == "__main__":
    send_emails_to_companies_with_outliers_dag.test()
