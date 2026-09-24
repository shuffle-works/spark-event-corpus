import json
from pathlib import Path

import pytest

from corpus.catalog import sha256_of
from corpus.detectors import (
    CHECKED_WITH,
    EXIT_OK,
    EXIT_TARGET_MISSED,
    EXIT_UNCHECKED,
    CheckResult,
    InvalidReport,
    exit_code,
    fired_tags,
    missing_targets,
    verify_entries,
)

FIXTURES = Path(__file__).parent / "fixtures" / "sparkforensics"


def load_fixture(name: str) -> dict:
    return json.loads((FIXTURES / name).read_text())


def test_fired_tags_are_sorted_unique_finding_tags():
    assert fired_tags(load_fixture("fires_skew_and_strag.json")) == ["SKEW", "STRAG"]


def test_fired_tags_ignore_clean_checks():
    # A clean check means the detector ran and found nothing, so SKEW must not count.
    assert fired_tags(load_fixture("fires_nothing.json")) == []


def test_fired_tags_rejects_report_without_findings():
    with pytest.raises(InvalidReport):
        fired_tags(load_fixture("missing_findings.json"))


def test_fired_tags_rejects_finding_without_tag():
    with pytest.raises(InvalidReport):
        fired_tags({"findings": [{"id": "f1", "type": "skew"}]})


def test_missing_targets_keeps_target_order():
    assert missing_targets(["SPEC", "SKEW", "HOST"], ["SKEW"]) == ["SPEC", "HOST"]


def test_missing_targets_empty_when_all_fire():
    assert missing_targets(["SKEW"], ["SKEW", "STRAG"]) == []


def test_exit_code_ok_when_every_target_fires():
    results = [CheckResult(id="a", missing=[]), CheckResult(id="b", missing=[])]
    assert exit_code(results) == EXIT_OK


def test_exit_code_nonzero_when_a_target_does_not_fire():
    results = [CheckResult(id="a", missing=[]), CheckResult(id="b", missing=["SPEC"])]
    assert exit_code(results) == EXIT_TARGET_MISSED


def test_exit_code_unchecked_takes_precedence_over_missed_target():
    results = [
        CheckResult(id="a", missing=["SPEC"]),
        CheckResult(id="b", error="log not found"),
    ]
    assert exit_code(results) == EXIT_UNCHECKED


def make_log(data_repo: Path, name: str, body: bytes = b"{}\n") -> dict:
    path = data_repo / "logs" / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(body)
    return {"path": f"logs/{name}", "checksum": sha256_of(path)}


def test_verify_entries_records_fires_detectors_on_generated_entries(tmp_path):
    entry = {
        "id": "pairwise-01",
        "source": "self-generated",
        "targets_detectors": ["SKEW", "SPEC"],
        **make_log(tmp_path, "pairwise-01.ndjson"),
    }
    external = {"id": "external-x", "source": "external", "path": "logs/external/x.ndjson"}

    results = verify_entries(
        [entry, external], tmp_path, lambda _: load_fixture("fires_skew_and_strag.json")
    )

    assert entry["fires_detectors"] == ["SKEW", "STRAG"]
    assert entry["fires_detectors_checked_with"] == CHECKED_WITH
    assert "fires_detectors" not in external
    assert results == [CheckResult(id="pairwise-01", missing=["SPEC"])]
    assert exit_code(results) == EXIT_TARGET_MISSED


def test_verify_entries_reports_missing_log_without_touching_entry(tmp_path):
    entry = {
        "id": "pairwise-02",
        "source": "self-generated",
        "targets_detectors": ["SKEW"],
        "path": "logs/pairwise-02.ndjson",
        "checksum": "sha256:00",
    }

    results = verify_entries([entry], tmp_path, lambda _: pytest.fail("analyze must not run"))

    assert "fires_detectors" not in entry
    assert results[0].error is not None
    assert exit_code(results) == EXIT_UNCHECKED


def test_verify_entries_rejects_log_whose_checksum_differs_from_catalog(tmp_path):
    entry = {
        "id": "pairwise-03",
        "source": "self-generated",
        "targets_detectors": [],
        **make_log(tmp_path, "pairwise-03.ndjson"),
    }
    (tmp_path / entry["path"]).write_bytes(b"changed\n")

    results = verify_entries([entry], tmp_path, lambda _: pytest.fail("analyze must not run"))

    assert "checksum" in results[0].error
    assert "fires_detectors" not in entry


def test_verify_entries_reports_analyzer_failure(tmp_path):
    entry = {
        "id": "pairwise-04",
        "source": "self-generated",
        "targets_detectors": ["SKEW"],
        **make_log(tmp_path, "pairwise-04.ndjson"),
    }

    def broken_analyze(_):
        raise InvalidReport("not JSON")

    results = verify_entries([entry], tmp_path, broken_analyze)

    assert results[0].error == "not JSON"
    assert "fires_detectors" not in entry
