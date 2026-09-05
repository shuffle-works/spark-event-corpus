"""Resolve the latest patch release for each currently-released Spark minor line."""
from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.request import urlopen

SPARK_DIST_URL = "https://archive.apache.org/dist/spark/"
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
