"""
Utils to download and maintain the ASN database.
"""
import datetime as dt
import json
import logging
import re
from ftplib import FTP
from pathlib import Path
from time import time
from typing import Dict, Final, Literal, NewType, Optional, Tuple

import pyasn
import requests
from pyasn import mrtx

from aleph_scoring.config import settings

Server = NewType("Server", str)

logger = logging.getLogger(__name__)

EXTRACT_ASNAME_C: Final = re.compile(
    r"<a .+>AS(?P<code>.+?)\s*</a>\s*(?P<name>.*)", re.U
)
_asn_db: Optional[pyasn.pyasn] = None


# Imported from pyasn_util_download.py
def ftp_download(server: Server, remote_dir: str, remote_file: str, local_file: Path):
    """Downloads a file from an FTP server and stores it locally"""
    with FTP(server) as ftp:
        ftp.login()
        ftp.cwd(remote_dir)
        logger.debug("Downloading ftp://%s/%s/%s", server, remote_dir, remote_file)
        filesize = ftp.size(remote_file)
        # perhaps warn before overwriting file?
        with local_file.open("wb") as fp:

            def recv(s):
                fp.write(s)
                recv.chunk += 1
                recv.bytes += len(s)
                if recv.chunk % 100 == 0:
                    logger.debug(
                        "\r %.f%%, %.fKB/s",
                        recv.bytes * 100 / filesize,
                        recv.bytes / (1000 * (time() - recv.start)),
                    )

            recv.chunk, recv.bytes, recv.start = 0, 0, time()
            ftp.retrbinary("RETR %s" % remote_file, recv)
    logger.debug("\nDownload complete.")


# Imported from pyasn_util_download.py
def find_latest_in_ftp(
    server: Server, archive_root: str, sub_dir: str
) -> Tuple[Server, str, str]:
    """Returns (server, filepath, filename) for the most recent file in an FTP archive"""
    logger.debug("Connecting to ftp://%s", server)
    ftp = FTP(server)
    ftp.login()
    months = sorted(
        ftp.nlst(archive_root), reverse=True
    )  # e.g. 'route-views6/bgpdata/2016.12'
    filepath = "/%s/%s" % (months[0], sub_dir)
    logger.debug("Finding most recent archive in %s ...", filepath)
    ftp.cwd(filepath)
    fls = ftp.nlst()
    if not fls:
        filepath = "/%s/%s" % (months[1], sub_dir)
        logger.debug("Finding most recent archive in %s ...", filepath)
        ftp.cwd(filepath)
        fls = ftp.nlst()
        if not fls:
            raise LookupError(
                "Cannot find file to download. Please report a bug on github?"
            )
    filename = max(fls)
    ftp.close()
    return server, filepath, filename


# Imported from pyasn_util_download.py
def find_latest_routeviews(
    archive_ipv: Literal["4", "6", "46", "64"]
) -> Tuple[Server, str, str]:
    # RouteViews archives are as follows:
    # ftp://archive.routeviews.org/datapath/YYYYMM/ribs/XXXX
    archive_ipv = str(archive_ipv)
    assert archive_ipv in ("4", "6", "46", "64")
    return find_latest_in_ftp(
        server=Server("archive.routeviews.org"),
        archive_root="bgpdata"
        if archive_ipv == "4"
        else "route-views6/bgpdata"
        if archive_ipv == "6"
        else "route-views4/bgpdata",  # 4+6
        sub_dir="RIBS",
    )


def update_asn_database(local_archive_file: Path):
    server, remote_dir, remote_filename = find_latest_routeviews(archive_ipv="4")
    ftp_download(
        server=server,
        remote_dir=remote_dir,
        remote_file=remote_filename,
        local_file=local_archive_file,
    )


def convert_asn_database(archive_file: Path, db_file: Path):
    prefixes = mrtx.parse_mrt_file(
        str(archive_file), print_progress=False, skip_record_on_error=False
    )
    mrtx.dump_prefixes_to_file(prefixes, str(db_file), str(archive_file))


# Imported from pyasn_util_asnames.py
def download_asnames() -> str:
    """
    Downloads and parses to utf-8 asnames html file
    """
    asnames_url = "http://www.cidr-report.org/as2.0/autnums.html"
    response = requests.get(asnames_url)

    response.raise_for_status()
    return response.text


def _parse_asname_line(line: str) -> Tuple[str, str]:
    match = EXTRACT_ASNAME_C.match(line)
    return match.groups()


# Imported from pyasn_util_asnames.py
def _html_to_dict(data: str) -> Dict:
    """
    Translates an HTML string available at `ASNAMES_URL` into a dict
    """
    lines = data.split("\n")
    lines = (line for line in lines if line.startswith("<a"))
    asn_name_tuples = (_parse_asname_line(line) for line in lines)
    return dict(asn_name_tuples)


def update_names_file(names_file: Path):
    asnames = download_asnames()
    asnames_dict = _html_to_dict(asnames)

    with names_file.open("w") as f:
        f.write(json.dumps(asnames_dict))


def should_update_asn_db(asn_db_file: Path) -> bool:
    if not asn_db_file.exists():
        logger.debug("ASN DB file does not exist, downloading it.")
        return True

    last_update_time = dt.datetime.fromtimestamp(asn_db_file.stat().st_mtime)
    if dt.datetime.now() > last_update_time + dt.timedelta(
        days=settings.ASN_DB_REFRESH_PERIOD_DAYS
    ):
        logger.debug("ASN DB file is outdated, updating it.")
        return True

    return False


def get_asn_database() -> pyasn.pyasn:
    global _asn_db

    asn_db_file = settings.ASN_DB_DIRECTORY / "asn_db"
    as_names_files = settings.ASN_DB_DIRECTORY / "asnames.json"

    should_update_db = should_update_asn_db(asn_db_file)

    if should_update_db:
        asn_archive_file = settings.ASN_DB_DIRECTORY / "asn_db.bz2"

        logger.info("Downloading ASN database...")
        update_asn_database(asn_archive_file)
        logger.info("Converting ASN database...")
        convert_asn_database(asn_archive_file, asn_db_file)
        logger.info("Updating names file...")
        update_names_file(as_names_files)

        logger.info("Loading ASN database...")
        _asn_db = pyasn.pyasn(str(asn_db_file), as_names_file=str(as_names_files))

    if should_update_db or _asn_db is None:
        _asn_db = pyasn.pyasn(str(asn_db_file), as_names_file=str(as_names_files))

    return _asn_db
