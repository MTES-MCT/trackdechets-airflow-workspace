import logging
from datetime import timedelta
from time import sleep

import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models import Variable

logger = logging.getLogger()


@dag(
    schedule_interval=timedelta(days=7),
    start_date=pendulum.datetime(2022, 7, 4, 7, 0, 0, tz="Europe/Paris"),
    catchup=False,
)
def reload_public_stats_app():
    """
    ### DAGS to refresh data of the public stats app
    This is a simple DAG that use Scalingo API to restart the containers hosting
    the Public Stats application. This allows to refresh data of the application as data is
    loaded only on application start.
    """

    @task
    def get_scalingo_bearer_token():
        """
        ### Get a Bearer Token using API Token
        Scalingo API needs Bearer token as authentification token to accept requests.
        The API token (linked to a Scalingo account) is exchanged for a temporary 1-hour
        Bearer token.
        """
        scalingo_api_token = Variable.get("SCALINGO_API_TOKEN")

        response = requests.post(
            "https://auth.scalingo.com/v1/tokens/exchange",
            auth=(scalingo_api_token, scalingo_api_token),
            headers={"Content-Type": "application/json", "Accept": "application/json"},
        )

        json_response = response.json()

        return json_response["token"]

    @task
    def restart_scalingo_app(bearer_token: str):
        """
        ### Request an application restart
        Use the Bearer token to request the restart of all application containers.
        Wait until the operation succeed or fail.
        """

        scaligo_app_name = Variable.get("PUBLIC_STATS_SCALINGO_APP_NAME")
        response = requests.post(
            f"https://api.osc-secnum-fr1.scalingo.com/v1/apps/{scaligo_app_name}/restart",
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {bearer_token}",
            },
            data={"scope": None},
        )

        logger.info(
            "Response code for app reload request: %s, headers: %s",
            response.content,
            response.headers,
        )

        operation_url = response.headers["Location"]

        response = requests.get(
            operation_url,
            headers={
                "Content-Type": "application/json",
                "Accept": "application/json",
                "Authorization": f"Bearer {bearer_token}",
            },
            data={"scope": None},
        )

        if response.status_code != 200:
            raise Exception(
                f"Problem getting operation status, reponse content: {response.content}"
            )

        json_response = response.json()["operation"]
        while json_response["status"] != "done":
            response = requests.get(
                operation_url,
                headers={
                    "Content-Type": "application/json",
                    "Accept": "application/json",
                    "Authorization": f"Bearer {bearer_token}",
                },
                data={"scope": None},
            )
            json_response = response.json()["operation"]

            if response.status_code != 200:
                raise Exception(
                    f"Problem getting operation status, reponse content: {response.content}"
                )

            logger.info(
                "Operation %s status: %s",
                operation_url.split("/")[-1],
                json_response["status"],
            )
            sleep(2)

    bearer_token = get_scalingo_bearer_token()
    restart_scalingo_app(bearer_token)


reload_public_stats_app_dag = reload_public_stats_app()
