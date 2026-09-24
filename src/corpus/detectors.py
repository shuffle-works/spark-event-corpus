"""Check each generated log's intended detector tags against what
sparkforensics actually reports.

targets_detectors in the catalog is what a scenario was designed to trigger;
fires_detectors is what `sparkforensics-analyze --format json` observed on the
committed log. Recording both side by side keeps the labels honest: a
contributor picking a log to exercise a detector can see whether it really
fires there.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from .catalog import sha256_of

# Detector tags and thresholds change between sparkforensics releases, so the
# observed tags are only meaningful against one pinned CLI version.
SPARKFORENSICS_CLI_VERSION = "0.2.4"
CHECKED_WITH = f"sparkforensics-cli@{SPARKFORENSICS_CLI_VERSION}"

EXIT_OK = 0
EXIT_TARGET_MISSED = 1
EXIT_UNCHECKED = 2

Report = dict[str, Any]
Analyzer = Callable[[Path], Report]


class AnalyzeError(Exception):
    """The analyzer could not produce a usable report for a log."""


class InvalidReport(AnalyzeError):
    """The analyzer's JSON does not have the shape this check relies on."""


@dataclass
class CheckResult:
    id: str
    missing: list[str] | None = None
    error: str | None = None


def fired_tags(report: Report) -> list[str]:
    """Sorted, de-duplicated tags of every finding in a sparkforensics report.

    Only findings count. cleanChecks lists detectors that ran and found
    nothing, so a tag appearing there has not fired.
    """
    findings = report.get("findings")
    if not isinstance(findings, list):
        raise InvalidReport("report has no findings list")
    tags = set()
    for finding in findings:
        tag = finding.get("tag") if isinstance(finding, dict) else None
        if not isinstance(tag, str) or not tag:
            raise InvalidReport(f"finding without a tag: {finding!r}")
        tags.add(tag)
    return sorted(tags)


def missing_targets(targets: list[str], fires: list[str]) -> list[str]:
    fired = set(fires)
    return [tag for tag in targets if tag not in fired]


def verify_entries(
    entries: list[dict[str, Any]], data_repo: Path, analyze: Analyzer
) -> list[CheckResult]:
    """Analyze every self-generated entry's log and record fires_detectors on it
    in place. External logs have no targets, so they are left alone.

    An entry whose log is missing, differs from its catalog checksum, or cannot
    be analyzed keeps its existing fields and is reported with an error.
    """
    results = []
    for entry in entries:
        if entry.get("source") != "self-generated":
            continue
        log_path = data_repo / entry["path"]
        if not log_path.is_file():
            results.append(CheckResult(id=entry["id"], error=f"log not found: {log_path}"))
            continue
        if sha256_of(log_path) != entry["checksum"]:
            results.append(
                CheckResult(id=entry["id"], error=f"checksum differs from catalog: {log_path}")
            )
            continue
        try:
            fires = fired_tags(analyze(log_path))
        except AnalyzeError as exc:
            results.append(CheckResult(id=entry["id"], error=str(exc)))
            continue
        entry["fires_detectors"] = fires
        entry["fires_detectors_checked_with"] = CHECKED_WITH
        results.append(
            CheckResult(id=entry["id"], missing=missing_targets(entry["targets_detectors"], fires))
        )
    return results


def exit_code(results: list[CheckResult]) -> int:
    # An unchecked log outranks a missed target: the check is incomplete, so
    # "only some targets missed" would understate what is unknown.
    if any(r.error for r in results):
        return EXIT_UNCHECKED
    if any(r.missing for r in results):
        return EXIT_TARGET_MISSED
    return EXIT_OK
