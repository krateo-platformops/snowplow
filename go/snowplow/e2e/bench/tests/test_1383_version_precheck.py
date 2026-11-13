"""#1383 gap 8: DEFAULT_CLUSTER_VERSION ("1.36") was never checked against what GKE's REGULAR channel offers in
the region, so a retired minor would be found out by a refused create. gke-up's preflight now reads
`gcloud container get-server-config` and refuses, naming the requirement, before anything is created."""

from __future__ import annotations

import pytest

from bench import gke
from tests import fake_region
from tests.test_gke_pvc_disks import NAME, _calls, fakes  # noqa: F401  (fixture)


@pytest.mark.parametrize("version, valid, ok", [
    ("1.36", ["1.36.4-gke.1391002", "1.35.9-gke.1"], True),
    ("1.36", ["1.37.1-gke.1", "1.35.9-gke.1"], False),
    ("1.3", ["1.36.4-gke.1391002"], False),                   # a prefix of the string is not the minor
    ("1.36.4-gke.1391002", ["1.36.4-gke.1391002"], True),
    ("1.36.4-gke.1391002", ["1.36.5-gke.1"], False),
])
def test_the_regular_channel_must_offer_the_version(version, valid, ok):
    cfg = {"channels": [{"channel": "RAPID", "validVersions": [version]},       # another channel's offer: no
                        {"channel": "REGULAR", "validVersions": valid}]}
    got, msg = gke.version_offered(cfg, version)
    assert got is ok
    if not ok:
        assert f"--cluster-version {version} --release-channel regular" in msg and "DEFAULT_CLUSTER_VERSION" in msg


def test_no_regular_channel_is_not_an_offer():
    got, msg = gke.version_offered({"channels": [{"channel": "RAPID", "validVersions": ["1.36.4-gke.1"]}]}, "1.36")
    assert not got and "lists no REGULAR channel" in msg


def test_gke_up_refuses_a_minor_the_channel_does_not_offer_and_creates_nothing(fakes, monkeypatch, capsys):  # noqa: F811
    monkeypatch.setenv("FAKE_REGULAR_VERSIONS", "1.37.1-gke.1000000,1.35.9-gke.1180000")
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    err = capsys.readouterr().err
    assert ("refusing: GKE 1.36 is not offered on the REGULAR channel in europe-west4 (its minors: 1.35, 1.37): "
            "gke-up creates --cluster-version 1.36 --release-channel regular, so the REGULAR channel in europe-west4 "
            "must offer a 1.36.x patch (DEFAULT_CLUSTER_VERSION, bench/gke.py); nothing was created") in err, err
    c = _calls(fakes)
    assert any(x[1:3] == ["container", "get-server-config"] and "--region" in x for x in c)
    assert not any(x[1:4] == ["container", "clusters", "create"] for x in c)


def test_gke_up_goes_on_when_the_channel_offers_it(fakes, capsys):  # noqa: F811
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 0
    assert f"GKE 1.36: offered on the REGULAR channel in europe-west4 ({fake_region.GKE_VERSION})" in capsys.readouterr().out


def test_an_unread_server_config_is_unknown_never_offered(fakes, monkeypatch, capsys):  # noqa: F811
    bin_ = fakes / "bin" / "gcloud"
    real = bin_.read_text()
    bin_.write_text(real.replace('if a[:2] == ["container", "get-server-config"]:',
                                 'if a[:2] == ["container", "get-server-config"]:\n        sys.exit(1)\n    if False:'))
    assert gke.main(["gke-up", "--name", NAME, "--address-need", "1", "--yes"]) == 2
    assert "whether the REGULAR channel offers 1.36 is unknown" in capsys.readouterr().err
