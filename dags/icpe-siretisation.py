from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime
from os.path import join
import pandas as pd


@task()
def init_dir() -> str:
    import os

    tmp_data_dir: str = join(
        Variable.get("TMP_DATA_DIR_BASE"), "tmp" + str(datetime.now()), ""
    )
    Variable.set("TMP_DATA_DIR", tmp_data_dir)

    os.mkdir(tmp_data_dir)
    return tmp_data_dir


@task()
def download_icpe_data(tmp_data_dir) -> str:
    icpe_tar_path = join(tmp_data_dir, "icpe.tar.gz")

    icpe_url: str = Variable.get("ICPE_URL")

    # If the file is on the local filesystem (testing env), copy instead of downloading
    if icpe_url.startswith("/"):
        from shutil import copyfile

        copyfile(icpe_url, icpe_tar_path)
    else:
        from requests import get

        icpe_data = get(icpe_url, allow_redirects=True)
        open(icpe_tar_path, "wb").write(icpe_data.content)

    return icpe_tar_path


@task()
def extract_icpe_files(icpe_tar_path) -> list:
    import tarfile

    tmp_data_dir = Variable.get("TMP_DATA_DIR")

    # https://stackoverflow.com/a/37474942
    tar = tarfile.open(icpe_tar_path, "r:gz")
    files_to_extract = [
        "IC_etablissement.csv",
        "IC_installation_classee.csv",
        "IC_ref_nomenclature_ic.csv",
    ]
    for member in tar.getmembers():
        if member.name in files_to_extract:
            tar.extract(member, path=tmp_data_dir)

    return files_to_extract


@task()
def add_icpe_headers(icpe_files: list) -> dict:
    tmp_data_dir = Variable.get("TMP_DATA_DIR")
    now = str(datetime.time(datetime.now()))

    def makeNewFilename(ori) -> str:
        return join(tmp_data_dir, "{}_{}.pkl".format(ori, now))

    options = {
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
                # 1 = en construction, 2 = en fonctionnement, 3 = ?? l'arr??t, 4 = cessation d??clar??e, 5 = R??colement fait
                "etatActivite",
                "codeApe",
                "nomCommune",
                "seveso",
                "regime",
                "prioriteNationale",
                # cf. biblio https://aida.ineris.fr/node/193
                "ippc",
                # Etablissement soumis ?? la d??claration annuelle d'??missions polluantes et de d??chets
                "declarationAnnuelle",
                # IN = industrie, BO = bovins, PO = porcs, VO = volailles, CA = carri??res
                "familleIc",
                # 1 + 1 = DREAL, etc.
                "baseIdService",
                "natureIdService",
                "adresse1",
                "adresse2",
                "dateInspection",
                # Sites et sols pollu??s:
                "indicationSsp",
                "rayon",
                "precisionPositionnement",
            ],
            "dtype": {
                "codeS3ic": str,
                "s3icNumeroSiret": str,
                "codePostal": str,
                "codeCommuneEtablissement": str,
            },
            "parse_dates": ["dateInspection"],
            "usecols": [
                "codeS3ic",
                "s3icNumeroSiret",
                "nomEts",
                "familleIc",
                "regime",
                "seveso",
                "codePostal",
                "nomCommune",
                "adresse1",
                "adresse2",
            ],
            "index_col": False,
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
            "dtype": {"codeS3ic": str, "id": str, "volume": float, "statut_ic": str},
            "parse_dates": ["date_debut_exploitation", "date_fin_validite"],
            "index_col": False,
            "usecols": False,
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
            "dtype": {
                "rubrique_ic": str,
                "alinea": str,
                "id_regime": str,
                "envigueur": int,
                "ippc": int,
            },
            "parse_dates": [],
            "index_col": False,
            "usecols": False,
        },
    }

    icpe_with_headers = {}

    for file in icpe_files:
        new_filename = join(tmp_data_dir, makeNewFilename(file))
        icpe_with_headers[file] = new_filename
        usecols = options[file]["usecols"] or options[file]["names"]
        print(options[file]["index_col"])
        df = pd.read_csv(
            join(tmp_data_dir, file),
            sep=";",
            header=None,
            dtype=options[file]["dtype"],
            parse_dates=options[file]["parse_dates"],
            names=options[file]["names"],
            index_col=options[file]["index_col"],
            dayfirst=True,
        )

        df = df[usecols]
        print(str(file))
        print(df.shape)
        print("-" * 35)
        df.to_pickle(new_filename)

    return icpe_with_headers


@task()
def enrich_rubriques(icpe_files: dict) -> str:
    tmp_data_dir = Variable.get("TMP_DATA_DIR")

    rubriques = pd.read_pickle(icpe_files["IC_ref_nomenclature_ic.csv"])
    rubriques["rubrique_ic_alinea"] = (
        rubriques["rubrique_ic"] + "_" + rubriques["alinea"]
    )
    rubriques["rubrique_ic_alinea"] = rubriques["rubrique_ic_alinea"].fillna("")
    # rubriques = rubriques[rubriques['rubrique_ic_alinea'].str.startswith('27')]

    rubriques_pickle_path = join(tmp_data_dir, "rubriques.pkl")
    print("enrich_rubrique")
    print(rubriques.shape)
    print("-" * 35)

    rubriques.to_pickle(rubriques_pickle_path)

    return rubriques_pickle_path


@task()
def enrich_installations(icpe_files: dict) -> str:
    tmp_data_dir = Variable.get("TMP_DATA_DIR")

    installations_pickle_path = join(tmp_data_dir, "installations.pkl")

    etablissements = pd.read_pickle(icpe_files["IC_etablissement.csv"])
    installations = pd.read_pickle(icpe_files["IC_installation_classee.csv"])

    print(installations)
    print(etablissements)

    print("installations,etablissements")
    print(installations.shape, etablissements.shape)
    print("-" * 35)

    installations = installations.merge(
        etablissements, left_on="codeS3ic", right_on="codeS3ic", how="left"
    )

    def setValue(value, reference_dict):
        if isinstance(value, str):
            result = ""
            try:
                result = reference_dict[value]
            except KeyError:
                print(
                    "Value "
                    + value
                    + " not understood. Expecting: "
                    + ", ".join(reference_dict.keys())
                )
            return result

    # Seveso label
    lib_seveso = {
        "S": "Seveso",
        "NS": "Non Seveso",
        "SB": "Seveso Seuil Bas",
        "SH": "Seveso Seuil Haut",
        "H": "Seveso Seuil Haut",
        "B": "Seveso Seuil Bas",
    }

    installations["lib_seveso"] = [
        setValue(x, lib_seveso) for x in installations["seveso"]
    ]

    # famille IC label
    famille_ic = {
        "IN": "Industries",
        "BO": "Bovins",
        "PO": "Porcs",
        "VO": "Volailles",
        "CA": "Carri??res",
    }
    installations["famille_ic_libelle"] = [
        setValue(x, famille_ic) for x in installations["familleIc"]
    ]

    # R??gime label
    regime = {
        "A": "Soumis ?? Autorisation",
        "E": "Enregistrement",
        "D": "Soumis ?? D??claration",
        "DC": "Soumis ?? D??claration avec Contr??le p??riodique",
        "NC": "Inconnu",
    }
    installations["libRegime"] = [setValue(x, regime) for x in installations["regime"]]

    print("Installations after enrichment:")
    print(installations.columns)
    print("installations dans enrich_installation")
    print(installations.shape)
    print("-" * 35)
    installations.to_pickle(installations_pickle_path)

    return installations_pickle_path


@task()
def get_siret_from_trackdechets_company(installations_pickle_path) -> str:
    from sqlalchemy import create_engine

    connection = create_engine(Variable.get("DATABASE_URL"))
    df_company: pd.DataFrame = pd.read_sql_query(
        """
        SELECT "Company"."siret" as siret, "Company"."name" as nom, "Company"."address" as address
        FROM "default$default"."Company"
        """,
        con=connection,
        dtype={"siret": str},
    )

    # Add a postal_code column from the Company.address column
    df_company["postal_code"] = df_company["address"].str.extract(r"(\d{5}) ")

    df_installations: pd.DataFrame = pd.read_pickle(installations_pickle_path)

    print("Before:")
    print(
        df_installations[["s3icNumeroSiret"]]
        .query("s3icNumeroSiret.str.len() == 14")
        .nunique()
    )

    df_installations = df_installations.merge(
        df_company, left_on=["nomEts"], right_on=["nom"], how="left"
    )

    for row in df_installations[["s3icNumeroSiret", "siret"]].itertuples():
        if len(str(row[1])) < 14 and len(str(row[2])) == 14:
            df_installations.at[row[0], "s3icNumeroSiret"] = row[2]

    # df_installations['s3icNumeroSiret'] = df_installations.where(df_installations['s3icNumeroSiret'].str.len() == 14,
    #                                                             df_installations['siret'], axis=0)

    print("After:")
    print(
        df_installations[["s3icNumeroSiret"]]
        .query("s3icNumeroSiret.str.len() == 14")
        .nunique()
    )

    df_installations.drop(columns=["siret", "postal_code", "address"], inplace=True)

    print("df_installations dans get_siret_from_trackdechets_company")
    print(df_installations.shape)
    print("-" * 35)

    installations_enriched_path = installations_pickle_path + "_td.pkl"
    df_installations.to_pickle(installations_enriched_path)

    return installations_enriched_path


@task()
def get_siret_from_gerep(installations_pickle_path) -> str:
    df_gerep = pd.read_csv(
        "https://docs.google.com/spreadsheets/d/1uzcWPJhpcQCbbVbW7f6UA2XpD0zJ7Iqb/export?format=csv",
        usecols=["Code ??tablissement", "Numero Siret", "Annee"],
        dtype={"Code ??tablissement": str, "Numero Siret": str, "Annee": str},
        index_col="Code ??tablissement",
    )
    df_gerep.index = df_gerep.index.astype(str)

    # GEREP data has several SIRET for each s3ic ID
    # Sort by ascending year, then group identical s3ic ids and keep the last SIRET (hopefully the current one)
    df_gerep.sort_values(by="Annee", inplace=True, ascending=True)
    df_gerep.drop(columns=["Annee"], inplace=True)
    df_gerep = df_gerep.groupby(level=0).last()

    # s3ic codes from GEREP have one less padding 0 at the beginning, let's fix it
    df_gerep.index = "0" + df_gerep.index

    df_installations = pd.read_pickle(installations_pickle_path)
    print(
        df_installations[["s3icNumeroSiret"]]
        .query("s3icNumeroSiret.str.len() == 14")
        .nunique()
    )

    df_installations = df_installations.merge(
        df_gerep, left_on="codeS3ic", right_index=True, how="left"
    )

    for row in df_installations[["s3icNumeroSiret", "Numero Siret"]].itertuples(
        index=True
    ):
        if len(str(row[1])) < 14 and len(str(row[2])) == 14:
            df_installations.at[row[0], "s3icNumeroSiret"] = row[2]

    # df.where(if false, use this value, on the row axis)
    # df_installations = df_installations.where(df_installations['s3icNumeroSiret'].str.len() == 14,
    #                                           df_installations['Numero Siret'], axis=0)

    df_installations.drop(columns=["Numero Siret"], inplace=True)

    print(
        df_installations[["s3icNumeroSiret"]]
        .query("s3icNumeroSiret.str.len() == 14")
        .nunique()
    )

    installations_enriched_path = installations_pickle_path + "_gerep.pkl"
    print("df_installations dans get_siret_from_gerep")
    print(df_installations.shape)
    print("-" * 35)
    df_installations.to_pickle(installations_enriched_path)

    return installations_enriched_path


@task()
def make_stats(installations_pickle_path, rubriques_pickle_path):
    installations = pd.read_pickle(installations_pickle_path)
    rubriques = pd.read_pickle(rubriques_pickle_path)

    rubriques = rubriques[rubriques["rubrique_ic_alinea"].str.startswith("27")]
    installations = installations.merge(
        rubriques, left_on="id_ref_nomencla_ic", right_on="id", how="inner"
    )
    # installations.to_csv(join(Variable.get("TMP_DATA_DIR"), 'installations_rubriques.csv'))
    installations.to_pickle(
        join(Variable.get("TMP_DATA_DIR"), "installations_rubriques.pkl")
    )

    print("df_installations dans make_stats")
    print(installations.shape)
    print("-" * 35)
    # installations with rubriques that are relevant for trackdechets
    rubriques_trackdechets = [
        "2710",
        "2712",
        "2718",
        "2770",
        "2790",
        "2792",
        "2793",
        "2795",
        "2797",
        "2798",
    ]
    rubriques_trackdechets_alinea = ["2720_1" "2760_1", "2760_4"]
    installations_td = installations.loc[
        (installations["rubrique_ic"].isin(rubriques_trackdechets))
        | (installations["rubrique_ic_alinea"].isin(rubriques_trackdechets_alinea))
    ]

    installations_td = installations_td[
        ["codeS3ic", "s3icNumeroSiret"]
    ].drop_duplicates(subset=["codeS3ic"])
    nb_installations_trackdechets = installations_td.index.size

    # Extract installations without SIRET to disk for other uses
    installations_td_no_siret = installations_td.query(
        "s3icNumeroSiret.str.len() < 14 or s3icNumeroSiret.isnull()"
    )
    nb_installations_td_no_siret = installations_td_no_siret.index.size
    # installations_td_no_siret.to_pickle(join(Variable.get("TMP_DATA_DIR"), 'installations_td_no_siret.pkl'))
    # installations_td_no_siret.to_csv(join(Variable.get("TMP_DATA_DIR"), 'installations_td_no_siret.csv'))

    sirets_trackdechets = (
        installations_td.query("s3icNumeroSiret.str.len() == 14")
        .drop_duplicates(subset=["s3icNumeroSiret"])
        .index.size
    )

    stats = f"""
    Installations d??chets dangereuxn concern??es par Trackd??chets
        nombre d'installations TD (n?? s3ic) = {nb_installations_trackdechets}
        installations TD avec siret (soustraction) = {nb_installations_trackdechets - nb_installations_td_no_siret}
        ({(nb_installations_trackdechets - nb_installations_td_no_siret)/nb_installations_trackdechets * 100} %)
        installations TD sans siret (index.size) = {nb_installations_td_no_siret}
        ({nb_installations_td_no_siret / nb_installations_trackdechets * 100} %)
        nombre de sirets uniques dans installations TD (d??duplication sirets) = {sirets_trackdechets}
    """
    return stats


@task()
def load_to_database(installations_pickle_path, rubriques_pickle_path) -> dict:
    from sqlalchemy import create_engine

    pg_user = Variable.get("PGSQL_USER")
    pg_password = Variable.get("PGSQL_PASSWORD")
    pg_host = Variable.get("PGSQL_HOST")
    pg_port = Variable.get("PGSQL_PORT")
    pg_database = Variable.get("PGSQL_DATABASE")
    pg_connection_string = Variable.get("PGSQL_CONNECTION_STRING", default_var=False)
    pg_schema = Variable.get("PGSQL_SCHEMA")
    table_installations = Variable.get("TABLE_INSTALLATIONS")
    table_rubriques = Variable.get("TABLE_RUBRIQUES")

    engine_string = pg_connection_string or "{}:{}@{}:{}/{}".format(
        pg_user, pg_password, pg_host, pg_port, pg_database
    )

    engine = create_engine("postgresql+psycopg2://" + engine_string)

    installations = pd.read_pickle(installations_pickle_path)
    print(installations)
    installations.to_sql(
        table_installations,
        con=engine,
        schema=pg_schema,
        if_exists="replace",
        chunksize=3,
    )

    rubriques = pd.read_pickle(rubriques_pickle_path)

    print(rubriques)
    rubriques.to_sql(table_rubriques, con=engine, schema=pg_schema, if_exists="replace")

    return {
        "Installation": installations_pickle_path,
        "Rubrique": rubriques_pickle_path,
    }


@dag(
    start_date=datetime(2021, 1, 1),
    schedule_interval=None,
    user_defined_macros={},
    catchup=False,
)
def icpe_etl_dag():
    tmp_data_dir = init_dir()
    icpe_tar_path = download_icpe_data(tmp_data_dir)
    get_icpe_data = extract_icpe_files(icpe_tar_path)
    icpe_with_headers = add_icpe_headers(get_icpe_data)
    rubriques_pickle_path = enrich_rubriques(icpe_with_headers)
    installations_pickle_path = enrich_installations(icpe_with_headers)
    enriched_installations_pickle_path = get_siret_from_trackdechets_company(
        get_siret_from_gerep(installations_pickle_path)
    )
    make_stats(enriched_installations_pickle_path, rubriques_pickle_path)
    make_stats(installations_pickle_path, rubriques_pickle_path)
    # load_to_database(installations_pickle_path, rubriques_pickle_path)


icpe_etl = icpe_etl_dag()
