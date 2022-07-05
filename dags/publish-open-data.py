from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from os.path import join
import pandas as pd


# DAG dedicated to the loading of a subset of company data to data.gouv.fr

@task()
def init_dir() -> str:
    import os

    tmp_data_dir: str = join(Variable.get("TMP_DATA_DIR_BASE"), 'tmp' + str(datetime.now()), '')
    Variable.set('TMP_DATA_DIR', tmp_data_dir)

    os.mkdir(tmp_data_dir)
    return tmp_data_dir


@task()
def query_database(tmp_data_dir) -> str:
    from sqlalchemy import create_engine

    connection = create_engine(Variable.get('DATABASE_URL'))
    df = pd.read_sql_query("""
    SELECT "Company"."siret", cast("Company"."createdAt" as date) as date_inscription,
    "Company"."companyTypes", "Company"."name" as nom, "Company"."verificationStatus"
    FROM "default$default"."Company"
    """, con=connection, dtype={'siret': str}, index_col='siret')

    company_pickle_path = join(tmp_data_dir, 'company.pkl')
    df.to_pickle(company_pickle_path)

    return company_pickle_path


@task()
def filter_company_data(company_pickle_path) -> str:
    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    df = pd.read_pickle(company_pickle_path)
    print("Number of établissements unfiltered: " + str(df.index.size))

    for label, row in df.iterrows():
        if row['companyTypes'] == '{PRODUCER}':
            df.at[label, 'verificationStatus'] = 'VERIFIED'

    # Filter and drop columns
    df = df.loc[df['verificationStatus'] == 'VERIFIED']\
        .drop(columns=['verificationStatus', 'companyTypes'])

    # Print stats
    print("Number of établissements filtered: " + str(df.index.size))

    company_filtered_pickle_path = join(tmp_data_dir, 'company_filtered.pkl')
    df.to_pickle(company_filtered_pickle_path)
    df.to_csv(company_filtered_pickle_path + '.csv')

    return company_filtered_pickle_path


@task()
def join_non_diffusible(company_filtered_pickle_path) -> str:
    from sqlalchemy import create_engine

    tmp_data_dir = Variable.get('TMP_DATA_DIR')

    connection = create_engine(Variable.get('DATABASE_URL'))
    df: pd.DataFrame = pd.read_sql_query("""
    SELECT "AnonymousCompany"."siret"
    FROM "default$default"."AnonymousCompany"
    """, con=connection, dtype={'siret': str})

    df['non_diffusible'] = 'oui'
    company_filtered = pd.read_pickle(company_filtered_pickle_path)

    # Add the non_diffusible column
    company_filtered_anonymous = company_filtered.join(df.set_index('siret'), how='left')

    # Save to pickle
    company_filtered_anonymous_pickle_path = join(tmp_data_dir, 'etablissements_inscrits.pkl')
    company_filtered_anonymous.to_pickle(company_filtered_anonymous_pickle_path)

    return company_filtered_anonymous_pickle_path


@task()
def send_to_datagouvfr(company_filtered_anonymous_pickle_path) -> str:
    import requests

    df = pd.read_pickle(company_filtered_anonymous_pickle_path)
    api_key = Variable.get('DATAGOUVFR_API_KEY')
    dataset_id = Variable.get('ETABLISSEMENTS_DATASET_ID')
    resource_id = Variable.get('ETABLISSEMENTS_RESOURCE_ID')

    response = requests.post(url=f'https://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}/upload',
                             headers={'X-API-KEY': api_key},
                             files={'file': ('etablissements_inscrits.csv', df.to_csv())})
    requests.put(url=f'https://www.data.gouv.fr/api/1/datasets/{dataset_id}/resources/{resource_id}',
                 headers={'X-API-KEY': api_key},
                 json={'title': 'Établissements inscrits sur Trackdéchets'})

    return response.text


# @task()
# def load_backup(tar_file) -> str:
#     # The bash script can probably get the env var too
#     # database_url = os.getenv("SCALINGO_DATABASE_URL")
#     subprocess.call(['bash/load_tar_to_db.sh', tar_file])


@dag(start_date=datetime(2022, 2, 7),
     schedule_interval="@daily",
     user_defined_macros={},
     catchup=False)
def publish_open_data_etl():
    tmp_data_dir = init_dir()
    company_pickle_path = query_database(tmp_data_dir)
    company_filtered_pickle_path = filter_company_data(company_pickle_path)
    company_filtered_anonymous_csv_path = join_non_diffusible(company_filtered_pickle_path)
    send_to_datagouvfr(company_filtered_anonymous_csv_path)


publish_open_data = publish_open_data_etl()
