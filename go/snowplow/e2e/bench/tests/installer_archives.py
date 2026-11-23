"""The published installer archives the suite renders with the real helm (tests/fixtures/installer_charts),
and which one a run can install.

A run writes krateo-057's features and component values, recorded at ONE installer version
(bench/installer_features.py, bench/installer_component_values.py; #1572), and refuses, before any
values are written or any cluster exists, an installer that cannot express them (check_chart). So the
only archive a test renders with a run's values is RECORDED: the published archive of the version the
record was read on. The older archives (0.3.545, 0.3.616, 0.3.641) stay for what they are about, their
history and their shape, and every one of them is refused by the chart check
(tests/test_installer_features.py holds that). Nothing here rewrites a run's values for an old chart.

When 057's record is re-taken on a new installer, test_the_suite_holds_the_archive_of_the_recorded_installer
fails until the new version's published archive is added here, and the tests that read RECORDED's own
facts (its private-image list, its routes, its page charts) are re-read from it.
"""

from __future__ import annotations

import hashlib
import tarfile
from pathlib import Path

CHARTS = Path(__file__).resolve().parent / "fixtures" / "installer_charts"
#: each archive's own sha256, as `helm pull oci://ghcr.io/krateo-platformops/charts/installer --version V` gave it
SHA256 = {
    "0.3.545": "ff89684302ec33852f390279f2cbd9d069fc65b39ce1ad566b5bf6907653efc1",
    "0.3.616": "614280d447301d7df1a7779e8618eeb079ccb99e7d28ef4bc3e8d466d0c9f285",
    "0.3.641": "d6692a9f3910d29b7b2ea7bf4fafa1d7499bab5686281259ce31fa25ecbc309b",
    # pulled 2026-11-23 (#1575), the registry's manifest digest sha256:d086c876b27ca2e8e10295bdf2efb8eb3a07bf8ee2d2eb77b6725847b1dcd20b
    "0.3.700": "652d839eff5cef2d9dc801a9260769e1ff1e7996e89bef5e7ccd71fe1bab6d60",
}
#: the archive of the installer krateo-057's record was read on: the one a run's values render against
RECORDED = "0.3.700"
#: the archives a run's chart check refuses (they cannot express 057's recorded features)
BEFORE_THE_RECORD = tuple(v for v in SHA256 if v != RECORDED)


def archive(version: str) -> Path:
    src = CHARTS / f"installer-{version}.tgz"
    assert hashlib.sha256(src.read_bytes()).hexdigest() == SHA256[version], src
    return src


def pull(into: Path, version: str) -> Path:
    """The pull directory, as `helm pull --untar -d <dir>` leaves it (<dir>/installer)."""
    into.mkdir(parents=True, exist_ok=True)
    with tarfile.open(archive(version)) as tf:
        tf.extractall(into, filter="data")
    return into
