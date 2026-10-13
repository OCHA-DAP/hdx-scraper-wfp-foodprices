"""Admin matching regression set built from the markets whose admin codes
changed between the prod (hdx-python-country 4.1.3) and stage (4.2.2) world
runs of 2026-10-12/13. See tests/fixtures/admin_matching/markets.csv: expected
codes are "" for no match, "?" for unknown (not scored) and alternatives are
separated by "|"."""

import csv
from os.path import join

import pytest
from hdx.api.utilities.hdx_error_handler import HDXErrorHandler
from hdx.utilities.downloader import Download
from hdx.utilities.loader import load_yaml
from hdx.utilities.path import script_dir_plus_file, temp_dir
from hdx.utilities.retriever import Retrieve

from hdx.scraper.wfp.foodprices.world.__main__ import main
from hdx.scraper.wfp.foodprices.world.hapi_output import HAPIOutput


def score(expected: str, actual: str) -> str:
    if expected == "?":
        return "skipped"
    if actual in expected.split("|"):
        return "correct"
    if not actual:
        return "miss"
    return "wrong"


def match_admins(configuration, admin_matching_dir: str) -> list[dict]:
    cases_path = join(admin_matching_dir, "markets.csv")
    with open(cases_path, encoding="utf-8") as f:
        cases = list(csv.DictReader(f))
    results = []
    with HDXErrorHandler(write_to_hdx=False) as error_handler:
        with temp_dir("TestWFPAdminMatching") as tempdir:
            with Download(user_agent="test") as downloader:
                retriever = Retrieve(
                    downloader,
                    tempdir,
                    admin_matching_dir,
                    tempdir,
                    save=False,
                    use_saved=True,
                )
                hapi_output = HAPIOutput(
                    configuration, downloader, tempdir, error_handler
                )
                hapi_output.setup_admins(retriever)
                for case in cases:
                    row = {
                        "market": case["market_code"],
                        "countryiso3": case["countryiso3"],
                        "admin1": case["provider_admin1_name"],
                        "admin2": case["provider_admin2_name"],
                    }
                    base_row = {"warning": set()}
                    hapi_output.complete_admin(row, base_row)
                    result = dict(case)
                    for level in (1, 2):
                        actual = base_row[f"admin{level}_code"]
                        result[f"actual_admin{level}_code"] = actual
                        result[f"admin{level}_score"] = score(
                            case[f"expected_admin{level}_code"], actual
                        )
                    results.append(result)
    return results


class TestAdminMatching:
    @pytest.fixture(scope="class")
    def admin_matching_dir(self, fixtures_dir):
        return join(fixtures_dir, "admin_matching")

    def test_admin_matching(self, configuration, admin_matching_dir):
        configuration.update(
            load_yaml(
                script_dir_plus_file(join("config", "project_configuration.yaml"), main)
            )
        )
        results = match_admins(configuration, admin_matching_dir)
        wrong = [
            f"{r['countryiso3']} {r['provider_admin1_name']}/"
            f"{r['provider_admin2_name']}: admin{level} "
            f"{r[f'actual_admin{level}_code']} not in "
            f"{r[f'expected_admin{level}_code']!r}"
            for r in results
            for level in (1, 2)
            if r[f"admin{level}_score"] == "wrong"
        ]
        assert wrong == []
