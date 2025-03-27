from os.path import join

import pytest

from hdx.api.configuration import Configuration
from hdx.api.locations import Locations
from hdx.data.vocabulary import Vocabulary
from hdx.scraper.wfp.foodprices.__main__ import main
from hdx.utilities.easy_logging import setup_logging
from hdx.utilities.path import script_dir_plus_file
from hdx.utilities.useragent import UserAgent

setup_logging()


@pytest.fixture(scope="session")
def fixtures_dir():
    return join("tests", "fixtures")


@pytest.fixture(scope="session")
def input_dir(fixtures_dir):
    return join(fixtures_dir, "input")


@pytest.fixture(scope="session")
def configuration():
    UserAgent.set_global("test")
    Configuration._create(
        hdx_read_only=True,
        hdx_site="prod",
        project_config_yaml=script_dir_plus_file(
            join("config", "project_configuration.yaml"), main
        ),
    )
    Locations.set_validlocations(
        [
            {"name": "cog", "title": "Congo"},
            {"name": "blr", "title": "Belarus"},
            {"name": "pse", "title": "State of Palestine"},
            {"name": "syr", "title": "Syrian Arab Republic"},
            {"name": "world", "title": "World"},
        ]
    )
    Vocabulary._approved_vocabulary = {
        "tags": [
            {"name": tag}
            for tag in (
                "hxl",
                "economics",
                "food security",
                "indicators",
                "markets",
            )
        ],
        "id": "b891512e-9516-4bf5-962a-7a289772a2a1",
        "name": "approved",
    }
    return Configuration.read()
