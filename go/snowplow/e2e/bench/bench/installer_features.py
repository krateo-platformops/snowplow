"""#1572: the installer features a bench install turns on: krateo-057's, from ONE declared record.

WHY (the owner's ruling, 2026-11-22: "the bench's install must match 057's installer features"). Up
to run 30 the bench's values listed three features by hand (agentGateway on, coreAgents and
specialistAgents off) and took the installer chart's defaults for the rest. 057 runs both agent
features, so the bench served none of kagent.dev/v1alpha2 (agents, remotemcpservers; the installer
gates kagent-crds on coreAgents) nor review.krateo.io/v1alpha1 (proposals; nightly-review-crd on
specialistAgents). The portal's dashboard-inbox RESTAction lists them: on the bench its steps
answered 404 and snowplow declined to cache four dashboard widgets, a sign-in landing tail that 057
does not have. A hand-picked feature list measures a platform nobody runs.

THE RECORD (bench/installer_features_057.json), written by `record` and never by hand: every feature
057 runs, as booleans, with where they were read. 057's Installer CR (installers.composition.krateo.io
krateo-system/installer) carries spec.features; the installer renders the platform from that CR over
its own chart's values, so a feature the CR leaves out runs at the chart's default. The record holds
both, named apart: the CR's features as read, and the defaults of the chart at the CR's own version
for the features the CR leaves out. The bench VM never holds 057 credentials: the record is read on
the operator's laptop, read-only, feature keys and booleans only (nothing else of the CR is kept):

    kubectl --context <057> get installers.composition.krateo.io -A -o json \\
      | python3 -m bench.installer_features record --installer-pulled <installer chart at 057's version> \\
          --date YYYY-MM-DD --out bench/installer_features_057.json
    kubectl --context <057> get installers.composition.krateo.io -A -o json \\
      | python3 -m bench.installer_features drift          # exit 1 + what moved: 057 changed since the record

tests/test_installer_features.py holds the committed record to the reading of 2026-11-22: changing
the record is a new reading, made on purpose, never an edit.

THE BENCH'S FEATURES (bench_features): the record's, except the DEVIATIONS below. A deviation is a
feature the bench cannot run as 057 does, each with why and the owner's ruling; it is named in every
check's output, so no launch differs from 057 silently. Every feature the record holds is written
into the run's values explicitly (bench/installer_values.py), so no chart default decides one.

THE CHART CHECK (check_chart; `check`, run606's source stage and bench.vm606's laptop chart check,
before any cluster exists): the installer chart a run installs must declare exactly the record's
features (values.schema.json features.properties). A feature 057 runs that the chart cannot express
(installer 0.3.641 has no internalTls, which 057 runs on 0.3.697), or a feature of the chart the
record does not name (057's value for it is unknown), refuses the run, naming them: the bench would
not run what 057 runs.

WHAT A FEATURE TURNS ON (component_enabled): the installer's own rule, inst.compEnabled
(templates/_helpers.tpl): a component is on when its `feature` is on or any of its `alsoFeatures` is;
a component with no feature is on; a feature in inst.coreFeatures is always on. ONE definition for
every bench module that derives the platform from the pins (ghcr_pull, lb_need, diag606).

Imports nothing from bench; stdlib + PyYAML.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys

RECORD_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "installer_features_057.json")
RECORD_KIND = "installer_features_057"
#: the Installer CR's API group: its version names the installer version (v0-3-697 = 0.3.697)
INSTALLER_GROUP = "composition.krateo.io"
_CR_VERSION = re.compile(r"^v(\d+)-(\d+)-(\d+)$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")

#: The features the bench runs differently from 057, by name: {feature: (the bench's value, why, ruling)}.
#: Each needs a credential or an endpoint outside the bench's reach; the owner rules on each, and
#: every check says each one, with the fidelity gap it leaves.
DEVIATIONS: dict[str, tuple[bool, str, str]] = {
    "configConnector": (
        False,
        "GCP Config Connector needs a GCP service-account key Secret (the installer's gcp-configconnector "
        "chart, cluster mode, configConnector.credentialSecretName) that the bench must not hold. "
        "Fidelity gap: ~274 cnrm.cloud.google.com CRDs 057 serves (of its 590) are not served on the bench",
        "the owner (Diego), 2026-11-22: OFF"),
    "serviceDesk": (
        False,
        "the service desk needs Krateo's Zendesk token and endpoint (zendesk-kog); without them the page "
        "would add a bench-only failure tail. Fidelity gap: the service-desk page and its Zendesk "
        "components are not installed on the bench",
        "the owner (Diego), 2026-11-22: OFF"),
}


class Refused(ValueError):
    """Why a record, a reading or a chart cannot be used, naming what."""


# ─── the record ────────────────────────────────────────────────────────────

def _bools(d, what: str) -> dict[str, bool]:
    if not isinstance(d, dict):
        raise Refused(f"{what}: not a map of features")
    for k, v in d.items():
        if not (isinstance(k, str) and k) or not isinstance(v, bool):
            raise Refused(f"{what}: {k!r} is not a feature with a boolean value ({type(v).__name__})")
    return dict(d)


def validate(rec) -> dict:
    """The record, or Refused: its kind, its reading's provenance, the CR's features and the chart
    defaults as booleans, the two apart, and `features` their union."""
    if not isinstance(rec, dict) or rec.get("kind") != RECORD_KIND:
        raise Refused(f"not an {RECORD_KIND} record")
    read = rec.get("read")
    if not isinstance(read, dict) or not _DATE.fullmatch(str(read.get("date") or "")) \
            or not read.get("installer_version"):
        raise Refused("the record names no reading (read.date, read.installer_version)")
    cr = _bools(rec.get("cr_features"), "cr_features")
    dflt = _bools(rec.get("chart_defaults"), "chart_defaults")
    if set(cr) & set(dflt):
        raise Refused(f"a feature is both read and defaulted: {sorted(set(cr) & set(dflt))}")
    if rec.get("features") != {**cr, **dflt}:
        raise Refused("features is not the CR's features plus the chart's defaults")
    return rec


def load(path: str = RECORD_PATH) -> dict:
    with open(path, encoding="utf-8") as fh:
        return validate(json.load(fh))


def bench_features(rec: dict | None = None, deviations: dict | None = None) -> dict[str, bool]:
    """The features a bench install writes: the record's, each deviation's value in its place."""
    rec = load() if rec is None else validate(rec)
    deviations = DEVIATIONS if deviations is None else deviations
    out = dict(rec["features"])
    for f, (value, why, ruling) in deviations.items():
        if f not in out:
            raise Refused(f"deviation {f}: 057's record has no such feature")
        if not isinstance(value, bool) or value == out[f]:
            raise Refused(f"deviation {f}: the bench's value must be a boolean other than 057's ({out[f]})")
        if not (why and ruling):
            raise Refused(f"deviation {f}: it names no reason or no ruling")
        out[f] = value
    return out


def deviation_lines(rec: dict | None = None, deviations: dict | None = None) -> list[str]:
    rec = load() if rec is None else rec
    deviations = DEVIATIONS if deviations is None else deviations
    return [f"the bench differs from 057 on {f}: {rec['features'][f]} on 057, {v} on the bench ({ruling}): {why}"
            for f, (v, why, ruling) in sorted(deviations.items())]


# ─── a reading of 057 ──────────────────────────────────────────────────────

def cr_reading(listed) -> tuple[str, dict[str, bool]]:
    """(the installer version, the CR's features) of `kubectl get installers.<group> -A -o json`:
    exactly one Installer, its apiVersion composition.krateo.io/v<x>-<y>-<z>, its spec.features
    booleans. Nothing else of the CR is returned."""
    items = (listed or {}).get("items") if isinstance(listed, dict) else None
    if not isinstance(items, list) or len(items) != 1:
        raise Refused(f"the reading lists {len(items) if isinstance(items, list) else 'no'} Installer(s), want exactly 1")
    it = items[0] if isinstance(items[0], dict) else {}
    group, _, ver = str(it.get("apiVersion") or "").partition("/")
    m = _CR_VERSION.fullmatch(ver)
    if group != INSTALLER_GROUP or not m:
        raise Refused(f"the Installer's apiVersion {it.get('apiVersion')!r} names no installer version")
    return ".".join(m.groups()), _bools((it.get("spec") or {}).get("features"), "the Installer's spec.features")


def drift(rec: dict, listed) -> list[str]:
    """What moved between the record and a fresh reading of 057 (empty: nothing)."""
    version, cr = cr_reading(listed)
    out = []
    if version != rec["read"]["installer_version"]:
        out.append(f"057 runs installer {version}, the record was read on {rec['read']['installer_version']}")
    for f in sorted(set(cr) | set(rec["cr_features"])):
        a, b = rec["cr_features"].get(f), cr.get(f)
        if a != b:
            out.append(f"{f}: {'absent' if a is None else a} in the record, {'absent' if b is None else b} on 057 now")
    return out


# ─── the installer version 057 runs (the owner's ruling, 2026-11-22) ───────
#
# A kit's INSTALLER_VERSION is the installer tag live on 057 when the kit is built, read from the
# cluster and recorded, never typed. The helm release record is a Secret, which the bench never
# reads; the version is read from what the release renders: the installer's own
# CompositionDefinition (krateo-system/installer), spec.chart.url .../installer at spec.chart.version
# (the chart 057's platform is rendered from; its spec, not the Installer's status):
#
#     kubectl --context <057> get compositiondefinitions.core.krateo.io -n krateo-system installer -o json \
#       | python3 -m bench.installer_features installer-version --out <kit>/installer-version.json
#
# prints the version for the kit's env file. A launch installs exactly the version the feature
# record was read on (check_kit_version): a kit built after 057 rolled is refused until 057's
# features are recorded again at that version.

_SEMVER = re.compile(r"^\d+\.\d+\.\d+$")


def live_installer_version(cd) -> str:
    """The installer chart version of 057's installer CompositionDefinition (its spec.chart)."""
    if not isinstance(cd, dict) or cd.get("kind") != "CompositionDefinition":
        raise Refused("the reading is not one CompositionDefinition (kubectl get compositiondefinitions"
                      ".core.krateo.io -n krateo-system installer -o json)")
    chart = (cd.get("spec") or {}).get("chart") or {}
    url, version = str(chart.get("url") or ""), str(chart.get("version") or "")
    if not url.rstrip("/").endswith("/installer"):
        raise Refused(f"the CompositionDefinition's chart {url!r} is not the installer's")
    if not _SEMVER.fullmatch(version):
        raise Refused(f"the installer CompositionDefinition's chart version {version!r} is not a release tag")
    return version


def check_kit_version(version: str, rec: dict | None = None) -> list[str]:
    """The lines to say, or Refused: the kit's INSTALLER_VERSION must be the version 057's features
    were recorded on."""
    rec = load() if rec is None else validate(rec)
    want = rec["read"]["installer_version"]
    if version != want:
        raise Refused(f"the kit installs installer {version}, but 057's features were recorded on {want} "
                      f"(read {rec['read']['date']}): read 057's live installer version and record its features "
                      f"again at it (python3 -m bench.installer_features installer-version / record)")
    return [f"installer {version}: the version 057's features were recorded on ({rec['read']['date']})"]


# ─── the installer chart ───────────────────────────────────────────────────

def _chart_root(installer_pulled: str) -> str:
    """The chart directory: `installer_pulled`/installer (as installer_src pulls it), or the chart
    directory itself."""
    sub = os.path.join(installer_pulled, "installer")
    return sub if os.path.isfile(os.path.join(sub, "Chart.yaml")) else installer_pulled


def chart_features(installer_pulled: str) -> tuple[str, dict[str, bool]]:
    """(the chart's version, {feature: its default}) of a pulled installer chart: the features its
    values.schema.json declares, each with its values.yaml default (absent there: off, as
    inst.featureEnabled reads an absent key)."""
    import yaml
    root = _chart_root(installer_pulled)
    try:
        with open(os.path.join(root, "Chart.yaml"), encoding="utf-8") as fh:
            version = str((yaml.safe_load(fh) or {}).get("version") or "")
        with open(os.path.join(root, "values.schema.json"), encoding="utf-8") as fh:
            schema = json.load(fh)
        with open(os.path.join(root, "values.yaml"), encoding="utf-8") as fh:
            values = yaml.safe_load(fh) or {}
    except (OSError, ValueError) as e:
        raise Refused(f"the installer chart at {root} cannot be read: {e}") from None
    props = (((schema.get("properties") or {}).get("features") or {}).get("properties"))
    if not isinstance(props, dict) or not props:
        raise Refused(f"installer {version}: its values schema declares no features")
    defaults = values.get("features") or {}
    return version, {f: bool(defaults.get(f, False)) for f in props}


def check_chart(installer_pulled: str, rec: dict | None = None, deviations: dict | None = None) -> list[str]:
    """The lines to say, or Refused: the chart must declare exactly the record's features."""
    rec = load() if rec is None else validate(rec)
    version, declared = chart_features(installer_pulled)
    missing = sorted(set(rec["features"]) - set(declared))
    unknown = sorted(set(declared) - set(rec["features"]))
    src = f"057 (installer {rec['read']['installer_version']}, read {rec['read']['date']})"
    if missing:
        named = ", ".join("%s=%s" % (f, rec["features"][f]) for f in missing)
        raise Refused(f"installer {version} cannot express {len(missing)} feature(s) {src} runs: {named}; "
                      f"install an installer that declares them (057 runs {rec['read']['installer_version']})")
    if unknown:
        raise Refused(f"installer {version} declares {len(unknown)} feature(s) the record of {src} does not name: "
                      f"{', '.join(unknown)}; what 057 runs for them is unknown: record 057 again "
                      f"(python3 -m bench.installer_features record)")
    feats = bench_features(rec, deviations)
    on = sorted(f for f, v in feats.items() if v)
    return [f"installer features: {len(feats)} written, as {src} runs them, {len(on)} on: {', '.join(on)}"] \
        + deviation_lines(rec, deviations)


def record(listed, installer_pulled: str, date: str) -> dict:
    """The record of a reading: the CR's features, and the defaults of the chart at the CR's version
    for the features the chart declares and the CR leaves out."""
    if not _DATE.fullmatch(date or ""):
        raise Refused(f"--date {date!r} is not YYYY-MM-DD")
    version, cr = cr_reading(listed)
    chart_version, declared = chart_features(installer_pulled)
    if chart_version != version:
        raise Refused(f"the chart is installer {chart_version}, 057's Installer CR is {version}: pull {version}")
    unknown = sorted(set(cr) - set(declared))
    if unknown:
        raise Refused(f"057's CR carries feature(s) installer {version}'s schema does not declare: {', '.join(unknown)}")
    dflt = {f: v for f, v in sorted(declared.items()) if f not in cr}
    return validate({
        "kind": RECORD_KIND,
        "read": {"date": date, "installer_version": version,
                 "object": f"installers.{INSTALLER_GROUP} (the one Installer CR), spec.features",
                 "chart": f"installer {version}: values.schema.json features, values.yaml defaults"},
        "cr_features": dict(sorted(cr.items())),
        "chart_defaults": dflt,
        "features": dict(sorted({**cr, **dflt}.items())),
    })


def values_block(features: dict[str, bool]) -> str:
    """The `features:` map of a values document, one line per feature, sorted."""
    return "features:\n" + "".join(f"  {f}: {'true' if v else 'false'}\n" for f, v in sorted(features.items()))


# ─── what a feature turns on ───────────────────────────────────────────────

def feature_enabled(feature, features: dict, core=()) -> bool:
    """inst.featureEnabled: no feature = on; a core feature = on; else features[feature] is true."""
    return not feature or feature in core or bool(features.get(feature, False))


def component_enabled(component: dict, features: dict, core=()) -> bool:
    """inst.compEnabled: the component's feature, or any of its alsoFeatures, is on."""
    return feature_enabled(component.get("feature") or "", features, core) or any(
        feature_enabled(f, features, core) for f in (component.get("alsoFeatures") or []) if f)


# ─── CLI ───────────────────────────────────────────────────────────────────

def _read_stdin():
    try:
        return json.load(sys.stdin)
    except ValueError as e:
        raise Refused(f"stdin is not the JSON of kubectl get installers -o json: {e}") from None


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(prog="bench.installer_features")
    sub = ap.add_subparsers(dest="cmd", required=True)
    c = sub.add_parser("check", help="the installer chart must declare exactly 057's recorded features")
    c.add_argument("--installer-pulled", required=True)
    c.add_argument("--out")
    r = sub.add_parser("record", help="057's Installer CR list on stdin -> the record")
    r.add_argument("--installer-pulled", required=True, help="the installer chart at 057's CR version")
    r.add_argument("--date", required=True)
    r.add_argument("--out", required=True)
    sub.add_parser("drift", help="057's Installer CR list on stdin: what moved since the record")
    v = sub.add_parser("installer-version",
                       help="057's installer CompositionDefinition on stdin -> the version a kit installs")
    v.add_argument("--out", help="the kit's record of the reading (JSON)")
    v.add_argument("--date", default="", help="YYYY-MM-DD of the reading, for the record")
    sub.add_parser("show", help="the features a bench install writes")
    a = ap.parse_args(argv)
    try:
        if a.cmd == "check":
            lines = check_chart(a.installer_pulled)
            if a.out:
                with open(a.out, "w", encoding="utf-8") as fh:
                    json.dump({"kind": "installer_features_check", "ok": True, "features": bench_features(),
                               "deviations": sorted(DEVIATIONS), "lines": lines}, fh, indent=1, sort_keys=True)
            print("\n".join(lines))
        elif a.cmd == "record":
            rec = record(_read_stdin(), a.installer_pulled, a.date)
            tmp = a.out + ".part"
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(rec, fh, indent=1)
                fh.write("\n")
            os.replace(tmp, a.out)
            print(f"recorded {len(rec['features'])} features of installer {rec['read']['installer_version']} "
                  f"({len(rec['cr_features'])} from the CR, {len(rec['chart_defaults'])} chart defaults)")
        elif a.cmd == "installer-version":
            version = live_installer_version(_read_stdin())
            if a.out:
                tmp = a.out + ".part"
                with open(tmp, "w", encoding="utf-8") as fh:
                    json.dump({"kind": "installer_version_057", "installer_version": version, "date": a.date,
                               "source": "compositiondefinitions.core.krateo.io krateo-system/installer "
                                         "spec.chart.version"}, fh, indent=1)
                    fh.write("\n")
                os.replace(tmp, a.out)
            print(version)
        elif a.cmd == "drift":
            moved = drift(load(), _read_stdin())
            print("\n".join(moved) if moved else "057's features are the record's")
            return 1 if moved else 0
        else:
            print(values_block(bench_features()), end="")
    except Refused as e:
        if a.cmd == "check" and a.out:
            with open(a.out, "w", encoding="utf-8") as fh:
                json.dump({"kind": "installer_features_check", "ok": False, "refused": str(e)}, fh, indent=1)
        print(f"refusing: {e}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
