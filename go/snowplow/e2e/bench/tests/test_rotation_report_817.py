"""#817 (b) instrumentation: mem606 captures the identity-class inputs, so a
run can date every key rotation and match it to a viewer that stopped being
pushed. Run 9 could not: none of these keys was captured."""

from __future__ import annotations

from bench import load606, load606_job


def _mem(t, **extra):
    return {"kind": "mem", "t": t, "extra": extra}


def test_mem606_captures_the_rotation_inputs_by_default():
    for k in load606.ROTATION_KEYS:
        assert k.startswith(tuple(load606.RUN8_VARS_PREFIXES)), k
    m = load606_job.render_mem(snowplow_url="http://s", authn_url="http://a", user="u",
                               password="pw", duration_s=60,
                               vars_prefixes=load606.RUN8_VARS_PREFIXES)
    for p in ("snowplow_rbac_", "snowplow_l1_identity_class_drift",
              "snowplow_refresh_broadcaster"):
        assert f'--vars-prefix "{p}"' in m


def test_rotation_report_dates_each_change():
    seq, bumps, epoch, drift = load606.ROTATION_KEYS
    mem = [_mem(10, **{seq: 16, bumps: 40, epoch: 0, drift: 0}),
           _mem(25, **{seq: 16, bumps: 40, epoch: 0, drift: 0}),
           _mem(40, **{seq: 17, bumps: 52, epoch: 0, drift: 0}),
           _mem(55, **{seq: 17, bumps: 52, epoch: 0, drift: 9}),
           {"kind": "heap_profile", "t": 56}]
    rep = load606.rotation_report(mem)
    assert rep["first"] == {seq: 16, bumps: 40, epoch: 0, drift: 0}
    assert rep["changes"] == [{"t": 40, seq: 17, bumps: 52}, {"t": 55, drift: 9}]
    v = load606.evaluate([], [], [], mem=mem)
    assert v["report"]["key_rotation_report_only"] == rep


def test_rotation_report_is_absent_when_not_captured():
    assert load606.rotation_report([_mem(1, other=1)]) is None
