import logging
import re
import subprocess
from pathlib import Path

logger = logging.getLogger(__name__)


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


def extract_log_level(log_bytes):
    # Decode the bytes-like object to a string
    log_string = log_bytes.decode('utf-8')
    # Define the pattern to search for. This pattern looks for anything between '[' and ']'
    # following the '@level@' portion of your string.
    pattern = r"@level@\[(.*?)\]"

    # Search for the pattern in the string
    match = re.search(pattern, log_string, re.MULTILINE | re.I)

    # Extract and return the match if it exists, otherwise return None
    return match.group(1).lower() if match else None


def read_output(process):
    for line in iter(process.stdout.readline, ""):
        log_line = line.rstrip()
        # match "@level@***" to get the level of log
        level = extract_log_level(log_line)
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


def git_clone_trackdechets(tmp_dir, trackdechets_sirene_search_git) -> str:
    clone_command = (
        f"git clone https://github.com/MTES-MCT/{trackdechets_sirene_search_git}.git"
    )
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
