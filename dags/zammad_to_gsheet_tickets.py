import json
import logging
import shutil
import tempfile
from datetime import timedelta
from pathlib import Path
from typing import Dict, List, Union

import numpy as np
import pandas as pd
import pendulum
import pygsheets
import requests
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from pendulum.datetime import DateTime

from mattermost import mm_failed_task

logger = logging.getLogger()

ZAMMAD_TO_GSHEET_COLUMN_MAPPING = {
    "number": "Ticket number",
    "title": "Title",
    "state": "State",
    "priority": "Priority",
    "group": "Group",
    "owner_name": "Owner",
    "customer_name": "Customer",
    "organization_id": "Organization",
    "create_article_type": "Create Channel",
    "create_article_sender": "Sender",
    "time_unit": "Time Units Total",
    "pending_time": "Pending till",
    "created_at": "Created At",
    "updated_at": "Updated At",
    "close_at": "Closed At",
    "close_escalation_at": "Close Escalation At",
    "close_in_min": "Close In Min",
    "close_diff_in_min": "Close Diff In Min",
    "first_response_at": "First Response At",
    "first_response_escalation_at": "First Response Escalation At",
    "first_response_in_min": "First Response In Min",
    "first_response_diff_in_min": "First Response Diff In Min",
    "update_escalation_at": "Update Escalation At",
    "update_in_min": "Update In Min",
    "update_diff_in_min": "Update Diff In Min",
    "last_contact_at": "Last Contact At",
    "last_contact_agent_at": "Last Contact Agent At",
    "last_contact_customer_at": "Last Contact Customer At",
    "article_count": "Article Count",
    "escalation_at": "Escalation At",
}

ZAMMAD_FIELDS_TO_INCLUDE = [
    "Created year & week",
    "Created Month",
    "Created week",
    "Created Day",
    "Ticket number",
    "Title",
    "State",
    "Priority",
    "Group",
    "Owner",
    "Customer",
    "Organization",
    "Create Channel",
    "Sender",
    "Tags",
    "Time Units Total",
    "Pending till",
    "Created At",
    "Updated At",
    "Closed At",
    "Close Escalation At",
    "Close In Min",
    "Close Diff In Min",
    "First Response At",
    "First Response Escalation At",
    "First Response In Min",
    "First Response Diff In Min",
    "Update Escalation At",
    "Update In Min",
    "Update Diff In Min",
    "Last Contact At",
    "Last Contact Agent At",
    "Last Contact Customer At",
    "Article Count",
    "Escalation At",
    "Délai avant 1ère réponse (h)",
    "Délai avant 1ère réponse (d)",
]


@dag(
    schedule_interval=timedelta(days=7),
    start_date=pendulum.datetime(2022, 7, 25, 18, tz="Europe/Paris"),
    catchup=True,
    on_failure_callback=mm_failed_task,
)
def zammad_to_gsheet_tickets():
    """
## ETL DAG to export Zammad tickets to Google Sheet for reporting

Needed connection:

- **airflow_service_account** : Google cloud platform service account key file path
with right to edit the Zammad reporting spreadsheet.

Needed variables:

- **ZAMMAD_REPORTING_SPREADSHEET_URL**: URL of the Google Spreadsheet where to load transformed data;
- **ZAMMAD_API_TOKEN**: API Token with the permission to query /tickets and /tags endpoints.
    """

    @task
    def get_gsheet_start_infos(data_interval_start: DateTime = None) -> Dict[str, int]:
        """
        Call Google Spreadsheet API to get the line index where to start to load data
        and extract the last ticket number of the period to get the first ticket number to query and avoid data duplication.
        """
        start_date = data_interval_start.subtract(days=7)

        service_account_filepath = json.loads(
            Connection.get_connection_from_secrets("airflow_service_account").extra
        )["extra__google_cloud_platform__key_path"]

        spreadsheet_url = Variable.get("ZAMMAD_REPORTING_SPREADSHEET_URL")

        gc = pygsheets.authorize(service_file=service_account_filepath)

        sh = gc.open_by_url(spreadsheet_url)
        sheet = sh.worksheet_by_title("Export")
        days = sheet.get_col(col=4)

        start_date_str = f"{start_date:%Y-%m-%d}"

        k = None
        for i, e in enumerate(reversed(days)):
            if e < start_date_str:
                k = i
                break

        first_index_at_date = sheet.rows - k + 1
        first_ticket_number = int(sheet.get_value(f"E{first_index_at_date-1}")) + 1
        logger.info(
            "Data will be appended starting at row %s with ticket number>=%s",
            first_index_at_date,
            first_ticket_number,
        )

        return {
            "first_index_gsheet": first_index_at_date,
            "first_ticket_number": first_ticket_number,
        }

    @task
    def get_zammad_tickets(
        first_ticket_number: int,
        data_interval_start: DateTime = None,
        data_interval_end: DateTime = None,
    ):
        """
        Call Zammad API to get all tickets and associated tags within the time period [Yesterday - 15J, Yesterday].
        Save the result in a DataFrame stored inside a temporary directory.
        """

        zammad_api_token = Variable.get("ZAMMAD_API_TOKEN")

        start_date = data_interval_start.subtract(days=7)
        end_date = data_interval_end.subtract(days=1)
        logger.info(
            "About to query Zammad tickets between dates %s and %s",
            start_date,
            end_date,
        )

        headers = {"Authorization": f"Token token={zammad_api_token}"}
        url_tickets_base = f"https://trackdechets.zammad.com/api/v1/tickets/search?query=(created_at:[{start_date:%Y-%m-%d} TO {end_date:%Y-%m-%d}] AND number:>={first_ticket_number})&sort_by=created_at&order_by=asc&per_page=50&expand=true"

        logger.info("Getting tickets")
        res = requests.get(url_tickets_base, headers=headers)
        res_json = res.json()
        df = pd.DataFrame(res_json)

        # Pagination
        i = 2
        while len(res_json) == 50:
            url_tickets = url_tickets_base + f"&page={i}"
            logger.info("Getting new page tickets : %s", i)
            res = requests.get(url_tickets, headers=headers)
            res_json = res.json()

            if len(res_json) > 0:
                df = pd.concat([df, pd.DataFrame(res_json)])

            i += 1

        def get_tags_for_ticket(ticket_id: Union[str, int]) -> List[str]:
            url_tags_base = f"https://trackdechets.zammad.com/api/v1/tags?object=Ticket&o_id={ticket_id}"
            try:
                res = requests.get(url_tags_base, headers=headers)
                res_json = res.json()
                return res_json["tags"]
            except Exception as e:
                logger.error(
                    "Error querying Zammad API - Tags endpoint for ticket_id: %s. Error : %s",
                    ticket_id,
                    e,
                )
                return []

        logger.info("Getting tags")
        df["Tags"] = df.apply(lambda x: get_tags_for_ticket(x["id"]), axis=1)

        def get_users_infos(
            user_ids: List[int], name_column: str, email_as_name: bool = False
        ) -> pd.DataFrame:
            url_user_base = "https://trackdechets.zammad.com/api/v1/users/"
            user_info_list = []

            for user_id in user_ids:
                res = requests.get(f"{url_user_base}{user_id}", headers=headers)
                res_json = res.json()
                user_info_list.append(res_json)

            users_df = pd.DataFrame(user_info_list)
            users_df[name_column] = users_df["firstname"] + " " + users_df["lastname"]

            if email_as_name:
                users_df.loc[
                    (users_df.firstname == "") & (users_df.lastname == ""), name_column
                ] = users_df["email"]

            return users_df[["id", name_column]]

        logger.info("Getting owners infos")
        owners_df = get_users_infos(df["owner_id"].unique(), "owner_name")
        logger.info("Getting customers infos")
        customers_df = get_users_infos(
            df["customer_id"].unique(), "customer_name", email_as_name=True
        )

        df = df.merge(
            owners_df,
            left_on="owner_id",
            right_on="id",
            validate="many_to_one",
            how="left",
        )
        df = df.merge(
            customers_df,
            left_on="customer_id",
            right_on="id",
            validate="many_to_one",
            how="left",
        )

        tmp_dir = Path(tempfile.mkdtemp(prefix="zammad_to_gsheet_tickets_"))
        df.to_pickle(tmp_dir / "zammad_tickets_pkl")

        return str(tmp_dir)

    @task
    def transform_zammad_tickets(tmp_dir: str, first_index_gsheet: int):
        """
        Apply transformations to tickets DatFrame before loading it.
        """

        tmp_dir = Path(tmp_dir)
        df_filepath = tmp_dir / "zammad_tickets_pkl"
        df = pd.read_pickle(df_filepath)

        for column in [
            "created_at",
            "updated_at",
            "close_at",
            "close_escalation_at",
            "first_response_at",
            "first_response_escalation_at",
            "update_escalation_at",
            "last_contact_at",
            "last_contact_agent_at",
            "last_contact_customer_at",
            "escalation_at",
        ]:
            df[column] = pd.to_datetime(df[column], utc=True).dt.strftime(
                "%Y-%m-%d %H:%M:%S"
            )

        df = df.rename(columns=ZAMMAD_TO_GSHEET_COLUMN_MAPPING)
        df.index = np.arange(first_index_gsheet, first_index_gsheet + df.shape[0])

        index = df.index.astype(str)

        # Gsheet formulas
        df["Created Day"] = "=GAUCHE(R" + index + ",10)"
        df["Created week"] = "=NO.SEMAINE(D" + index + ",21)"
        df["Created Month"] = "=GAUCHE(R" + index + ",7)"
        df["Created year & week"] = (
            "=CONCAT(CONCAT(GAUCHE(D" + index + ',4),"-"),C' + index + ")"
        )
        df["Délai avant 1ère réponse (h)"] = (
            "=SI(X" + index + '="","",X' + index + "-R" + index + ")"
        )
        df["Délai avant 1ère réponse (d)"] = "=AJ" + index
        df["Tags"] = df["Tags"].str.join(",")

        df = df[ZAMMAD_FIELDS_TO_INCLUDE]
        df = df.fillna("")

        logger.info(
            "Transformed df shape: %s,min created_at: %s, max created_at: %s, first line created_at: %s, last line created_at: %s",
            df.shape,
            df["Created At"].min(),
            df["Created At"].max(),
            df.iloc[0]["Created At"],
            df.iloc[-1]["Created At"],
        )

        df.to_pickle(df_filepath)

    @task
    def load_zammad_tickets_to_gsheet(tmp_dir: str, first_index_gsheet: int):
        """
        Load the dataframe to the Google Spreadsheet, inserting rows beginning at index first_index_gsheet. 
        """

        tmp_dir = Path(tmp_dir)

        df = pd.read_pickle(tmp_dir / "zammad_tickets_pkl")

        service_account_filepath = json.loads(
            Connection.get_connection_from_secrets("airflow_service_account").extra
        )["extra__google_cloud_platform__key_path"]

        spreadsheet_url = Variable.get("ZAMMAD_REPORTING_SPREADSHEET_URL")

        gc = pygsheets.authorize(service_file=service_account_filepath)

        sh = gc.open_by_url(spreadsheet_url)
        sheet = sh.worksheet_by_title("Export")

        sheet.update_values(
            crange=f"A{first_index_gsheet}:AK{first_index_gsheet+df.shape[0]}",
            values=df.values.tolist(),
            extend=True,
        )

    @task
    def cleanup_tmp_files(tmp_dir: str):
        """
        Delete all artifacts generated by the DAG run.
        """
        shutil.rmtree(tmp_dir)

    gsheet_infos = get_gsheet_start_infos()
    tmp_dir = get_zammad_tickets(gsheet_infos["first_ticket_number"])
    transform_zammad_tickets(
        tmp_dir, gsheet_infos["first_index_gsheet"]
    ) >> load_zammad_tickets_to_gsheet(
        tmp_dir, gsheet_infos["first_index_gsheet"]
    ) >> cleanup_tmp_files(
        tmp_dir
    )


zammad_to_gsheet_tickets_dag = zammad_to_gsheet_tickets()
