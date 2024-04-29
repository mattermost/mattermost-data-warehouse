import tempfile

import pytest
from requests import HTTPError
from responses import Response

from utils.geolite.service import Edition, MaxMindClient

# Customize defaults for given_request_to
__MOCK_REQUEST_DEFAULTS = {
    'dir': 'geolite',
}


def test_download_country_partial(responses, given_request_to):
    # GIVEN: request to download expected edition
    EXPECTED_URL = "https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip"
    given_request_to(EXPECTED_URL, "country_partial.zip", method="GET")

    # WHEN: request to download given file but only partial results in file
    client = MaxMindClient('account', 'license')
    with tempfile.TemporaryDirectory() as target_dir, pytest.raises(ValueError) as excinfo:
        client.download(Edition.COUNTRY, target_dir)

    # THEN: expect proper error message
    assert "Not all expected files found in zip file" in str(excinfo.value)

    # THEN: expect request to have been sent
    responses.assert_call_count(EXPECTED_URL, 1)


def test_download_error_code(responses):
    # GIVEN: request to download expected edition throws an error
    EXPECTED_URL = "https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip"
    responses.add(Response(method="GET", url=EXPECTED_URL, status=401))

    # WHEN: request to download given file but only partial results in file
    client = MaxMindClient('account', 'license')
    with tempfile.TemporaryDirectory() as target_dir, pytest.raises(HTTPError) as excinfo:
        client.download(Edition.COUNTRY, target_dir)

    # THEN: expect proper error message
    assert "401 Client Error" in str(excinfo.value)

    # THEN: expect request to have been sent
    responses.assert_call_count(EXPECTED_URL, 1)


@pytest.mark.parametrize(
    "expected_url,response_file,edition,expected_contents",
    [
        pytest.param(
            "https://download.maxmind.com/geoip/databases/GeoLite2-ASN-CSV/download?suffix=zip",
            "asn.zip",
            Edition.ASN,
            [
                # First file
                "network,autonomous_system_number,autonomous_system_organization\n"
                "1.1.1.0/24,13335,CLOUDFLARENET"
            ],
            id="ASN",
        ),
        pytest.param(
            "https://download.maxmind.com/geoip/databases/GeoLite2-City-CSV/download?suffix=zip",
            "city.zip",
            Edition.CITY,
            [
                # First file
                "network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider,postal_code,latitude,longitude,accuracy_radius,is_anycast\n"  # noqa: E501
                "1.1.1.0/32,6252001,2077456,,0,0,,37.7510,-97.8220,1000,",
                # Second file
                "geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,subdivision_1_iso_code,subdivision_1_name,subdivision_2_iso_code,subdivision_2_name,city_name,metro_code,time_zone,is_in_european_union\n"  # noqa: E501
                "666978,en,EU,Europe,RO,Romania,CJ,\"Cluj County\",,,Sânnicoară,,Europe/Bucharest,1",
            ],
            id="city",
        ),
        pytest.param(
            "https://download.maxmind.com/geoip/databases/GeoLite2-Country-CSV/download?suffix=zip",
            "country.zip",
            Edition.COUNTRY,
            [
                # First file
                "network,geoname_id,registered_country_geoname_id,represented_country_geoname_id,is_anonymous_proxy,is_satellite_provider,is_anycast\n"  # noqa: E501
                "1.1.1.1/32,,2077456,,0,0,\n"
                "1.1.1.2/31,6252001,2077456,,0,0,",
                # Second file
                "geoname_id,locale_code,continent_code,continent_name,country_iso_code,country_name,is_in_european_union\n"  # noqa: E501
                "2077456,en,OC,Oceania,AU,Australia,0",
            ],
            id="country",
        ),
    ],
)
def test_download(responses, given_request_to, expected_url, response_file, edition, expected_contents):
    # GIVEN: request to download expected edition
    given_request_to(expected_url, response_file, method="GET")

    # WHEN: request to download given file
    client = MaxMindClient('account', 'license')
    with tempfile.TemporaryDirectory() as target_dir:
        files = sorted(client.download(edition, target_dir))

        # THEN: expect two files to be downloaded
        assert len(files) == len(expected_contents)

        # THEN: expect the proper files to have been extracted
        for file, expected_content in zip(files, expected_contents):
            with open(file, 'r') as fp:
                assert fp.read() == expected_content

    # THEN: expect request to have been sent
    responses.assert_call_count(expected_url, 1)
