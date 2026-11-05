"""The answer a fake gcloud gives to `compute regions describe` (#1171): every
quota of europe-west4 as gcloud returned them on 2026-10-25
(fixtures/region_europe_west4_quotas_20261025.json), so a pre-check that
reads one more quota reads it from the real listing, never from a fake that
happens to omit it.

Usage is steered per quota: FAKE_USAGE_<METRIC> (FAKE_USAGE_IN_USE_ADDRESSES=13),
and the older names FAKE_SSD_USAGE / FAKE_STATIC_USAGE. One default differs
from the reading: IN_USE_ADDRESSES starts at 0 used. The reading's 13 of 16
leaves 3, and one bench run needs 5 node external IPs + its LoadBalancer IPs,
so with it no arm could start a run (run 21 attempt 2's failure, #1171); the
arms that test that quota steer it.

A fake gcloud is its own process (the run606 harness runs it with `python -I`,
no PYTHONPATH), so it loads this file by path: LOADER is the source to put in
front of its body; it defines `region_quotas(name)`.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

FIXTURE = Path(__file__).resolve().parent / "fixtures" / "region_europe_west4_quotas_20261025.json"
DEFAULT_USAGE = {"IN_USE_ADDRESSES": 0.0}
OLDER_NAMES = {"SSD_TOTAL_GB": "FAKE_SSD_USAGE", "STATIC_ADDRESSES": "FAKE_STATIC_USAGE"}


def quotas(env=None, name: str = "europe-west4") -> dict:
    env = os.environ if env is None else env
    region = json.loads(FIXTURE.read_text())
    region["name"] = name
    for q in region["quotas"]:
        m = q["metric"]
        v = env.get(f"FAKE_USAGE_{m}") or (env.get(OLDER_NAMES[m]) if m in OLDER_NAMES else None)
        if v is not None:
            q["usage"] = float(v)
        elif m in DEFAULT_USAGE:
            q["usage"] = DEFAULT_USAGE[m]
    return region


LOADER = (
    "import importlib.util as _ilu\n"
    f"_frs = _ilu.spec_from_file_location('fake_region', {str(Path(__file__).resolve())!r})\n"
    "_frm = _ilu.module_from_spec(_frs)\n"
    "_frs.loader.exec_module(_frm)\n"
    "def region_quotas(name):\n"
    "    return _frm.quotas(None, name)\n"
)
