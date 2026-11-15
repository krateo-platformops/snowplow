"""The installer's component pins of a pulled installer chart: ONE reader, for every bench module
that needs them (installer_src's preflight, crd_schema, ghcr_pull and through it login_contract,
lb_need).

Why one: each module opened `files/component-pins.yaml` by name. Installer 0.3.640 stopped shipping
that file (it is .helmignore'd there: 97.6 % comment prose, and it pushed the helm release Secret
past its 1 MiB limit on krateo-057); the packaged chart carries `files/component-pins.min.yaml`, a
generated data-only copy, and its own helper loads that one. The first real dry pass on installer
0.3.641 was refused at the source step for the file "missing", and no run could start.

The rule: read what the installer chart ITSELF reads. Its templates load the pins with
`.Files.Get "files/component-pins<...>.yaml"` (the inst.componentsYaml define, templates/
_helpers.tpl): that name is the file. Read from the published charts on 2026-11-14/15:
    0.3.545 .. 0.3.639   ship and load  files/component-pins.yaml
    0.3.640, 0.3.641     ship and load  files/component-pins.min.yaml
(0.3.616, 620, 625, 630, 633, 636, 639, 640, 641 pulled; the entries of 0.3.639's annotated file
and 0.3.641's min file carry the same 31 fields, every one the bench reads among them: name, kind,
chart, version, repo, feature, alsoFeatures, deps, tier, expose, exposePort, svcMatch.)

Nothing is chosen by a file's name or by an order of preference:
  - the templates name exactly one pins file, and the chart ships it: that file;
  - the templates name none (a chart that does not load its pins through .Files.Get): the one pins
    file the chart ships, when it ships exactly one;
  - anything else is refused, naming the files: the templates name a file the chart does not ship,
    or more than one; the chart ships no pins file; it ships more than one and no template says
    which; or it ships a second pins file whose components are not those of the one it loads (a
    chart that would render one set of pins while a reader could take the other).

Imports nothing from bench.
"""

from __future__ import annotations

import os
import re

FILES_DIR = "files"
#: a pins file's name: component-pins.yaml, component-pins.min.yaml, any later variant
_PINS_NAME = re.compile(r"^component-pins(?:\.[A-Za-z0-9_-]+)*\.ya?ml$")
_FILES_GET = re.compile(r"\.Files\.Get\s+\"files/([^\"/]+)\"")


class Refused(ValueError):
    """Why a chart's pins cannot be read, naming the files."""


def _yaml(data: bytes, what: str):
    import yaml
    return yaml.safe_load(data)


def loaded_by_templates(chart_dir: str) -> list[str]:
    """The pins files the chart's own templates load (`.Files.Get "files/component-pins...yaml"`),
    sorted, each once."""
    tdir = os.path.join(chart_dir, "templates")
    names: set[str] = set()
    for name in sorted(os.listdir(tdir)) if os.path.isdir(tdir) else []:
        if not name.endswith((".tpl", ".yaml", ".yml")):
            continue
        with open(os.path.join(tdir, name), encoding="utf-8", errors="replace") as fh:
            names |= {n for n in _FILES_GET.findall(fh.read()) if _PINS_NAME.match(n)}
    return sorted(names)


def shipped(chart_dir: str) -> list[str]:
    """The pins files the chart ships under files/, sorted."""
    fdir = os.path.join(chart_dir, FILES_DIR)
    return sorted(n for n in (os.listdir(fdir) if os.path.isdir(fdir) else []) if _PINS_NAME.match(n))


def pins_file(chart_dir: str) -> str:
    """The name (under files/) of the pins file the chart reads. Raises Refused (module doc)."""
    if not os.path.isfile(os.path.join(chart_dir, "Chart.yaml")):
        raise Refused(f"{chart_dir} is not a chart (no Chart.yaml): the installer's pins cannot be read")
    asked, have = loaded_by_templates(chart_dir), shipped(chart_dir)
    where = os.path.join(chart_dir, FILES_DIR)
    ships = f"it ships {', '.join(have) or 'no pins file'} in {where}"
    if len(asked) > 1:
        raise Refused(f"the installer chart's templates load more than one pins file ({', '.join(asked)}); "
                      f"{ships}: which one holds its pins is not decided here")
    if asked:
        if asked[0] not in have:
            raise Refused(f"the installer chart's templates load files/{asked[0]}, which the chart does not ship; "
                          f"{ships}")
        return asked[0]
    if len(have) == 1:
        return have[0]
    raise Refused(f"the installer chart's templates name no pins file (.Files.Get \"files/component-pins...\"), and "
                  f"{ships}: " + ("there is none to read" if not have else "which one holds its pins is not said"))


def load(chart_dir: str, parse=_yaml) -> tuple[dict, str]:
    """(the pins document, the file it was read from, as files/<name>) of the pulled installer chart
    at `chart_dir`. `parse(bytes, what)`: the caller's YAML reader (ghcr_pull's bounded one). Raises
    Refused when the file is not decided (pins_file), is not a map with a `components` list, or the
    chart ships another pins file whose components differ."""
    name = pins_file(chart_dir)
    docs = {}
    for n in shipped(chart_dir):
        with open(os.path.join(chart_dir, FILES_DIR, n), "rb") as fh:
            doc = parse(fh.read(), f"the installer's {FILES_DIR}/{n}: ")
        if not isinstance(doc, dict) or not isinstance(doc.get("components"), list):
            raise Refused(f"the installer's {FILES_DIR}/{n} is not a map with a `components` list")
        docs[n] = doc
    other = [n for n in docs if n != name and docs[n]["components"] != docs[name]["components"]]
    if other:
        raise Refused(f"the installer chart loads {FILES_DIR}/{name} and also ships {', '.join(other)}, whose "
                      f"components are not the same: the chart renders one set of pins and the files show another")
    return docs[name], f"{FILES_DIR}/{name}"


def components(chart_dir: str, parse=_yaml) -> list[dict]:
    """The chart's pinned components (load()'s document's `components`)."""
    return load(chart_dir, parse)[0]["components"]
