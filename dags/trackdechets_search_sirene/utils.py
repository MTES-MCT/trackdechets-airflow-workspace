import logging
import re
import subprocess
from datetime import datetime, timezone
from pathlib import Path

from authlib.integrations.httpx_client import OAuth2Client
from authlib.oauth2.rfc7523 import ClientSecretJWT
from tqdm.auto import tqdm

logger = logging.getLogger(__name__)


def refresh_token(
    client: OAuth2Client, token_endpoint: str, username: str, password: str
) -> int:
    """
    Refreshes an OAuth2 access token using a username and password.

    Parameters
    ----------
    client : OAuth2Client
        OAuth2 client instance used to refresh the token.
    token_endpoint : str
        URL of the token endpoint to use for refreshing the token.
    username : str
        Username to use for authentication.
    password : str
        Password to use for authentication.

    Returns
    -------
    token_expiration_timestamp : int
        Timestamp (in seconds since epoch) at which the refreshed token expires.

    Notes
    -----
    This function uses the `client.fetch_token` method to refresh the access token
    and returns the timestamp of when the new token expires.
    """

    token = client.fetch_token(
        token_endpoint,
        grant_type="password",
        username=username,
        password=password,
    )
    token_expiration_timestamp = token["expires_at"]
    logger.debug("Acquired new token that expires at %s", token_expiration_timestamp)

    return token_expiration_timestamp


def format_extracted_companies(company_list: list[dict]) -> list[dict]:
    """
    Formats a list of extracted companies by flattening nested dictionaries and lists.

    Parameters
    ----------
    company_list : list[dict]
        List of company dictionaries to format.

    Returns
    -------
    companies_formatted : list[dict]
        Formatted list of company dictionaries with no nested structures.

    Notes
    -----
    This function uses a pattern matching approach to recursively flatten any
    nested dictionaries or lists in the input company dictionaries.
    """
    companies_formatted = []

    for company in company_list:
        company_formatted = {}
        for key, val in company.items():
            match val:
                case dict():
                    company_formatted = {**company_formatted, **val}
                case list():
                    company_formatted = {**company_formatted, **val[0]}
                case _:
                    company_formatted[key] = val
        companies_formatted.append(company_formatted)
    return companies_formatted


def extract_companies(
    client_id: str,
    client_secret: str,
    username: str,
    password: str,
    date_start: datetime,
    date_end: datetime,
) -> list[dict]:
    """
    Extract companies from Sirene API based on a date range.

    Parameters
    ----------
    client_id : str
        Client ID for OAuth2 authentication.
    client_secret : str
        Client secret for OAuth2 authentication.
    username : str
        Username for OAuth2 authentication.
    password : str
        Password for OAuth2 authentication.
    date_start : datetime
        Start of the date range (inclusive).
    date_end : datetime
        End of the date range (inclusive).

    Returns
    -------
    companies : list[dict]
        List of company dictionaries extracted from Sirene API.

    Notes
    -----
    This function uses OAuth2 authentication to access the Sirene API.
    It extracts companies based on a date range and returns a list of company dictionaries.
    The function also logs progress and errors using the `logger` module.
    """

    token_endpoint = (
        "https://auth.insee.net/auth/realms/apim-gravitee/protocol/openid-connect/token"
    )

    client = OAuth2Client(
        client_id=client_id,
        client_secret=client_secret,
    )

    client.register_client_auth_method(ClientSecretJWT(token_endpoint))
    token_expiration_timestamp = refresh_token(
        client, token_endpoint, username, password
    )

    companies_endpoint = f"https://api.insee.fr/api-sirene/prive/3.11/siret?q=dateDernierTraitementEtablissement:[{date_start:%Y-%m-%dT00:00:00} TO {date_end:%Y-%m-%dT00:00:00}]"

    res = client.get(f"{companies_endpoint}&nombre=0")
    res_json = res.json()
    number_of_companies_to_extract = res_json["header"]["total"]
    logger.info("Number of companies to extract: %s", number_of_companies_to_extract)

    old_cursor = None
    cursor = "*"
    companies = []

    with tqdm(total=number_of_companies_to_extract) as t:
        while cursor != old_cursor:
            ts_now = datetime.now(timezone.utc)
            if ts_now.timestamp() - 30 > token_expiration_timestamp:
                token_expiration_timestamp = refresh_token(
                    client, token_endpoint, username, password
                )

            res = client.get(f"{companies_endpoint}&curseur={cursor}&nombre=1000")
            res_json = res.json()

            number_of_companies_extracted = res_json["header"]["nombre"]
            t.write(f"Number of companies extracted:{number_of_companies_extracted}")
            t.update(number_of_companies_extracted)
            companies.extend(res_json["etablissements"])

            old_cursor = cursor
            cursor = res_json["header"]["curseurSuivant"]

    logger.info("Finished to extract companies")
    return companies


def log_message(extracted_level, message):
    # Map the extracted level to the logging function
    log_level_mapper = {
        "debug": logger.debug,
        "info": logger.info,
        "warning": logger.warning,
        "error": logger.error,
        "critical": logger.critical,
    }

    # Get the logging function based on the extracted level
    log_func = log_level_mapper.get(
        extracted_level, logging.info
    )  # Default to 'info' if level is not recognized

    # Call the logging function with the message
    log_func(message)


def ensure_str(input_data):
    """
    Avoid type errors
    """
    if isinstance(input_data, bytes):
        # Decode bytes to a string using utf-8 encoding
        return input_data.decode("utf-8")
    elif isinstance(input_data, str):
        # Input is already a string, return as is
        return input_data
    else:
        # Handle other types if necessary, or return empty
        return ""


def extract_log_level(log_bytes):
    # Decode the bytes-like object to a string
    log_string = ensure_str(log_bytes)
    # Define the pattern to search for. This pattern looks for anything between '[' and ']'
    # following the '@level@' portion of your string.
    pattern = r"@level@\[(.*?)\]"

    # Search for the pattern in the string
    match = re.search(pattern, log_string, re.MULTILINE | re.I)

    # Extract and return the match if it exists, otherwise return None
    return match.group(1).lower() if match else None


def read_output(line):
    if not line:
        return

    log_line = line.rstrip()
    if len(log_line) == 0:
        return

    # match "@level@***" to get the level of log
    level = extract_log_level(log_line)
    if level is None:
        level = "info"

    log_message(level, log_line)


def download_es_ca_pem(
    tmp_dir, elasticsearch_capem, trackdechets_sirene_search_git
) -> str:
    """Download certificate needed for ElasticSearch connection."""
    tmp_dir = Path(tmp_dir)

    if "https" in elasticsearch_capem:
        curl = f"curl -o es.cert {elasticsearch_capem}"
        completed_process = subprocess.run(
            curl,
            check=True,
            capture_output=True,
            shell=True,
            cwd=tmp_dir / trackdechets_sirene_search_git / "dist" / "common",
        )
        logger.info(completed_process)
    else:
        # Incase the certificate is already stored in the elasticsearch_capem variable
        (tmp_dir / "ca.pem").write_text(elasticsearch_capem)
    return str(tmp_dir)


def git_clone_trackdechets(tmp_dir, trackdechets_sirene_search_git, branch_name) -> str:
    clone_command = f"git clone https://github.com/MTES-MCT/{trackdechets_sirene_search_git}.git --branch {branch_name}"

    completed_process = subprocess.run(
        clone_command, check=True, capture_output=True, shell=True, cwd=tmp_dir
    )
    logger.info(completed_process)
    return str(tmp_dir)


def npm_install_build(tmp_dir, trackdechets_sirene_search_git) -> str:
    """
    npm install && npm run build
    """
    tmp_dir = Path(tmp_dir)
    install_command = "npm install --quiet"
    completed_process = subprocess.run(
        install_command,
        check=False,
        capture_output=True,
        shell=True,
        cwd=tmp_dir / trackdechets_sirene_search_git,
    )

    logger.info(completed_process.stdout)
    if completed_process.returncode != 0:
        raise Exception(completed_process)

    build_command = "npm run build"
    completed_process = subprocess.run(
        build_command,
        check=False,
        capture_output=True,
        shell=True,
        cwd=tmp_dir / trackdechets_sirene_search_git,
    )

    logger.info(completed_process.stdout)
    if completed_process.returncode != 0:
        raise Exception(completed_process)

    return str(tmp_dir)
