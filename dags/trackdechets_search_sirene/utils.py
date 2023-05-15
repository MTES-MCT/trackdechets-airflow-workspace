import subprocess
from pathlib import Path
import logging

logging.basicConfig()
logger = logging.getLogger()


def download_es_ca_pem(
    tmp_dir, elasticsearch_capem, trackdechets_sirene_search_git
) -> str:
    """Download certificate needed for ElasticSearch connection."""
    tmp_dir = Path(tmp_dir)
    curl = f"curl -o es.cert {elasticsearch_capem}"
    completed_process = subprocess.run(
        curl,
        check=True,
        capture_output=True,
        shell=True,
        cwd=tmp_dir / trackdechets_sirene_search_git / "dist" / "src" / "common",
    )
    logger.info(completed_process)
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
