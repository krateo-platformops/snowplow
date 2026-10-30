"""How a snowplow build JUDGES a forced verification pass's divergence
candidates, read from the counters THAT build publishes (#995), the same way
bench/store_rebuild.py reads how it rebuilds a store.

Two mechanisms exist:

  recheck           (#872/#875, to 1.12.58) a candidate is re-checked with a
                    quorum GET against the object's current state
                    (store_divergence_recheck_*_total);
  lost_update_rule  (#918, #923, from 1.12.59) a candidate is a lost update
                    only if the store is behind the LIST's own revision and
                    has applied everything up to it; otherwise it is cleared,
                    held pending until the store's applied position reaches
                    the read, or left undecided (store_divergence_cleared /
                    store_ahead / received / undecided / pending_*).

A snapshot's mechanism is the one whose OWN keys it publishes (any of
them), never one chosen by version; neither or both is no answer, and an
absent counter is never a zero.

Only quantities that ARE the same on both builds share a name (#996 review,
read in the Go at 64c1c0e3 and 102ed658):
  candidates          store_divergence_candidates_total (the same key on both)
  no_delivery_record  divergences the forced pass published for a GVR with no
                      delivery record (recheck: ..._recheck_unavailable_total;
                      the rule: ..._unrecorded_total)
The cleared candidates are NOT the same quantity, so each keeps its own name:
  recheck_cleared  forced-pass candidates (delivered while the pass was open,
                   still divergent after the confirm loop) the re-check found
                   the store already past; never the snapshot site
  rule_cleared     every raw candidate the lost-update rule cleared, at both
                   sites (agreed, ahead, or received at the snapshot)
Everything else is the mechanism's own, and `not_published` for the other.

STDLIB ONLY.
"""

from __future__ import annotations

VERIFY = "snowplow_store_verification"
PENDING_BY_GVR = "snowplow_store_verification_pending_by_gvr"

MECHANISMS = {
    "recheck": {"how": "a candidate is re-checked with a quorum GET (#872/#875, to 1.12.58)"},
    "lost_update_rule": {"how": ("a candidate is lost only if the store is behind the LIST's revision and has "
                                 "applied everything up to it (#918, #923, from 1.12.59)")},
}

# (report name, stat): the cumulative counters per mechanism
COMMON = {
    "recheck": (("no_delivery_record", "store_divergence_recheck_unavailable_total"),),
    "lost_update_rule": (("no_delivery_record", "store_divergence_unrecorded_total"),),
}
OWN = {
    "recheck": (("recheck_gets", "store_divergence_recheck_gets_total"),
                ("recheck_cleared", "store_divergence_recheck_cleared_total"),
                ("recheck_undelivered", "store_divergence_recheck_undelivered_total")),
    "lost_update_rule": (("rule_cleared", "store_divergence_cleared_total"),
                         ("store_ahead", "store_divergence_store_ahead_total"),
                         ("received", "store_divergence_received_total"),
                         ("undecided", "store_divergence_undecided_total"),
                         ("pending_resolved_lost", "store_divergence_pending_resolved_lost_total"),
                         ("pending_resolved_received", "store_divergence_pending_resolved_received_total"),
                         ("pending_superseded", "store_divergence_pending_superseded_total")),
}
GAUGES = {
    "recheck": (),
    "lost_update_rule": (("pending_keys", "store_divergence_pending_keys"),
                         ("pending_oldest_age_seconds", "store_divergence_pending_oldest_age_seconds"),
                         ("position_unavailable", "store_position_unavailable")),
}
# what each does not publish: every quantity another mechanism reports and this one does not
# the per-GVR maps a mechanism publishes beside its family, read at the last sample
MAPS = {"recheck": (), "lost_update_rule": (("pending_by_gvr", PENDING_BY_GVR),)}


def _names(m: str) -> set[str]:
    return {n for n, _ in OWN[m] + GAUGES[m]} | {n for n, _ in MAPS[m]}


NOT_PUBLISHED = {m: tuple(sorted(set().union(*(_names(o) for o in MECHANISMS if o != m)) - _names(m)))
                 for m in MECHANISMS}
NOTES = {
    "recheck": ("recheck_cleared: forced-pass candidates (delivered while the pass was open, still divergent "
                "after the confirm loop) the re-check found the store already past, never the snapshot site; "
                "no_delivery_record: forced-pass divergences published for a GVR with no delivery record"),
    "lost_update_rule": ("rule_cleared: raw candidates the lost-update rule cleared at either site (agreed, "
                         "ahead, or received at the snapshot): not recheck_cleared's quantity; "
                         "no_delivery_record: forced-pass divergences published for a GVR with no delivery "
                         "record (losses the resolver or a carried pass publishes later are not in it); "
                         "pending_*: keys held until the store's applied position reaches their read"),
}


def identifying(m: str) -> tuple[str, ...]:
    """Every stat only mechanism `m` publishes: a snapshot carrying any of
    them publishes `m` (#996 review F7: not one key, which a partial family
    would slip past)."""
    return tuple(stat for _, stat in COMMON[m] + OWN[m] + GAUGES[m])


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def mechanism(x: dict) -> tuple[str | None, str]:
    """(the judging mechanism this /debug/vars snapshot publishes, why)."""
    v = x.get(VERIFY)
    if not isinstance(v, dict):
        return None, f"publishes no {VERIFY}"
    hits = {m: [s for s in identifying(m) if s in v] for m in MECHANISMS}
    found = [m for m, k in hits.items() if k]
    if len(found) == 1:
        return found[0], f"publishes {len(hits[found[0]])} of its {VERIFY} keys ({hits[found[0]][0]}, ...)"
    if not found:
        return None, "publishes neither mechanism's keys: " + " nor ".join(f"{m}'s" for m in MECHANISMS)
    return None, "publishes both: " + "; ".join(f"{m}: {', '.join(k)}" for m, k in hits.items() if k)


def counters(v: dict, mech: str | None) -> dict:
    """The mechanism's cumulative counters of one store-verification document,
    by report name (None = not published by this build)."""
    if mech is None:
        return {}
    return {name: _num(v.get(stat)) for name, stat in COMMON[mech] + OWN[mech]}


def gauges(x: dict, mech: str | None) -> dict:
    """The mechanism's gauges and per-GVR maps of one sample (None = not published)."""
    if mech is None:
        return {}
    v = x.get(VERIFY) or {}
    out = {name: _num(v.get(stat)) for name, stat in GAUGES[mech]}
    for name, fam in MAPS[mech]:
        m = x.get(fam)
        out[name] = {k: _num(n) for k, n in m.items()} if isinstance(m, dict) else None
    return out
