"""The Spark versions the corpus is generated against, pinned in code, plus a
check-only lookup of what the Apache dist listing offers.

The generation matrix reads SPARK_VERSIONS and never the network: log ids are a
consumer contract (sparkforensics names corpus files in its CI), so they must
not change with the upstream listing, whether a patch release appears or
Apache drops one. Bumping a patch is a one-line edit here; the id stays the same and
the new patch is recorded in the catalog entry's spark_version.
"""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.request import urlopen

# Minor line -> the patch release generated for it. Baseline ids are keyed by
# the minor line (spark-4.1-parquet-baseline), so a bump here regenerates a
# log in place under the same id.
SPARK_VERSIONS: dict[str, str] = {
    "3.5": "3.5.9",
    "4.0": "4.0.4",
    "4.1": "4.1.3",
    "4.2": "4.2.0",
}
# The minor line every pairwise, failure and cache scenario runs on.
SCENARIO_SPARK_LINE = "4.2"

SPARK_DIST_URL = "https://downloads.apache.org/spark/"
VERSION_DIR_RE = re.compile(r'href="spark-(\d+)\.(\d+)\.(\d+)/"')


class VersionResolutionError(RuntimeError):
    """Raised when the Apache dist listing can't be fetched or parsed."""


@dataclass(frozen=True)
class SparkVersion:
    major: int
    minor: int
    patch: int

    @property
    def minor_line(self) -> tuple[int, int]:
        return (self.major, self.minor)

    def __str__(self) -> str:
        return f"{self.major}.{self.minor}.{self.patch}"


def fetch_dist_listing(url: str = SPARK_DIST_URL) -> str:
    try:
        with urlopen(url, timeout=30) as resp:
            return resp.read().decode("utf-8")
    except OSError as exc:
        raise VersionResolutionError(
            f"could not fetch Spark release listing from {url}: {exc}"
        ) from exc


def parse_versions(listing_html: str) -> list[SparkVersion]:
    matches = VERSION_DIR_RE.findall(listing_html)
    if not matches:
        raise VersionResolutionError(
            "no spark-X.Y.Z/ directories found in dist listing; page format may have changed"
        )
    return [SparkVersion(int(a), int(b), int(c)) for a, b, c in matches]


def latest_patch_per_minor(versions: list[SparkVersion]) -> list[SparkVersion]:
    latest: dict[tuple[int, int], SparkVersion] = {}
    for v in versions:
        current = latest.get(v.minor_line)
        if current is None or v.patch > current.patch:
            latest[v.minor_line] = v
    return sorted(latest.values(), key=lambda v: v.minor_line)


def resolve_versions(url: str = SPARK_DIST_URL) -> list[SparkVersion]:
    return latest_patch_per_minor(parse_versions(fetch_dist_listing(url)))


def scenario_spark_version() -> str:
    return SPARK_VERSIONS[SCENARIO_SPARK_LINE]


def upstream_report(
    pinned: dict[str, str], upstream: list[SparkVersion]
) -> list[str]:
    """One line per difference between the pinned set and the upstream
    listing: a newer patch, a pinned patch missing from the listing, or a minor
    line that is not pinned. Empty when they agree. Informational only, since
    nothing here feeds the matrix."""
    listed = {str(v) for v in upstream}
    latest = {f"{v.major}.{v.minor}": v for v in latest_patch_per_minor(upstream)}
    lines = []
    for line, patch in pinned.items():
        newest = latest.get(line)
        if newest is not None and str(newest) != patch:
            lines.append(f"{line}: pinned {patch}, upstream has {newest}")
        if patch not in listed:
            lines.append(f"{line}: pinned {patch} is not on the dist listing")
    for line, newest in latest.items():
        if line not in pinned:
            lines.append(f"{line}: not pinned, upstream has {newest}")
    return lines
