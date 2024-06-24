import logging
import os
import shutil
import tempfile
from enum import Enum
from typing import IO
from zipfile import ZipFile

import requests
from requests.auth import HTTPBasicAuth

BASE_URL = "https://download.maxmind.com/geoip/databases/{edition}/download?suffix=zip"

logger = logging.getLogger(__name__)


class Edition(str, Enum):
    ASN = "GeoLite2-ASN-CSV"
    CITY = "GeoLite2-City-CSV"
    COUNTRY = "GeoLite2-Country-CSV"


# Map of edition to specific IP V4 file
IP_V4_FILENAMES = {
    Edition.ASN: ["GeoLite2-ASN-Blocks-IPv4.csv"],
    Edition.CITY: ["GeoLite2-City-Blocks-IPv4.csv", "GeoLite2-City-Locations-en.csv"],
    Edition.COUNTRY: ["GeoLite2-Country-Blocks-IPv4.csv", "GeoLite2-Country-Locations-en.csv"],
}

# Map of filename to target table.
FILENAME_TO_TABLE = {
    "GeoLite2-ASN-Blocks-IPv4.csv": "ASN_IPV4",
    "GeoLite2-City-Blocks-IPv4.csv": "CITY_IPV4",
    "GeoLite2-City-Locations-en.csv": "CITY_LOCATIONS",
    "GeoLite2-Country-Blocks-IPv4.csv": "COUNTRY_IPV4",
    "GeoLite2-Country-Locations-en.csv": "COUNTRY_LOCATIONS",
}


class MaxMindClient:
    def __init__(self, account_id: str, license_id: str):
        self._auth = HTTPBasicAuth(account_id, license_id)

    def _download_file(self, url: str, target: IO, chunk_size: int = 8192):
        # Streaming download file to target in order to minimize memory footprint
        with requests.get(url, allow_redirects=True, stream=True, auth=self._auth) as r:
            # Raise exception on HTTP errors
            r.raise_for_status()
            shutil.copyfileobj(r.raw, target)

    def download(self, edition: Edition, target: str):
        """
        Downloads the given GeoLite package from MaxMind and extracts the needed file to target directory.
        """
        with tempfile.NamedTemporaryFile(mode='wb') as fp:
            # Download zip file containing data + metadata + licenses
            self._download_file(BASE_URL.format(edition=edition), fp)
            fp.flush()
            # Copy IPv4 file to target
            with ZipFile(fp.name, 'r') as archive:
                files = [f for f in archive.namelist() if f.split('/')[-1] in IP_V4_FILENAMES[edition]]
                if not files:
                    raise ValueError(f"No files found in downloaded {edition} file")
                if len(files) != len(IP_V4_FILENAMES[edition]):
                    raise ValueError("Not all expected files found in zip file")

                # Map read files to outputs
                targets = [os.path.join(target, file.split('/')[-1]) for file in files]
                for inp, out in zip(files, targets):
                    logger.info(f'Extracting {inp} to {out}')
                    with archive.open(inp) as src, open(os.path.join(target, inp.split('/')[-1]), 'wb') as out:
                        shutil.copyfileobj(src, out)

                return targets
