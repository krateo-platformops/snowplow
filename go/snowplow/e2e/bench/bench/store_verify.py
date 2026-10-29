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

A snapshot's mechanism is the one whose identifying counter it publishes,
never one chosen by version; neither or both is no answer, and an absent
counter is never a zero.

The quantities both mechanisms publish an equivalent of are reported under
one name:
  candidates          store_divergence_candidates_total (the same key on both)
  candidates_cleared  recheck: re-check GETs that found the store already
                      past the LIST (forced pass only); lost_update_rule:
                      candidates the rule cleared (store agreed, ahead, or
                      had received the state; either site)
  no_delivery_record  recheck: candidates counted without a re-check because
                      the GVR has no delivery record; lost_update_rule:
                      divergences published for a GVR with no delivery record
Everything else is the mechanism's own, and `not_published` for the other.

STDLIB ONLY.
"""

from __future__ import annotations

VERIFY = "snowplow_store_verification"
PENDING_BY_GVR = "snowplow_store_verification_pending_by_gvr"

MECHANISMS = {
    "recheck": {"identified_by": ("store_divergence_recheck_gets_total",),
                "how": "a candidate is re-checked with a quorum GET (#872/#875, to 1.12.58)"},
    "lost_update_rule": {"identified_by": ("store_divergence_pending_keys", "store_divergence_cleared_total"),
                         "how": ("a candidate is lost only if the store is behind the LIST's revision and has "
                                 "applied everything up to it (#918, #923, from 1.12.59)")},
}

# (report name, stat): the cumulative counters per mechanism
COMMON = {
    "recheck": (("candidates_cleared", "store_divergence_recheck_cleared_total"),
                ("no_delivery_record", "store_divergence_recheck_unavailable_total")),
    "lost_update_rule": (("candidates_cleared", "store_divergence_cleared_total"),
                         ("no_delivery_record", "store_divergence_unrecorded_total")),
}
OWN = {
    "recheck": (("recheck_gets", "store_divergence_recheck_gets_total"),
                ("recheck_undelivered", "store_divergence_recheck_undelivered_total")),
    "lost_update_rule": (("store_ahead", "store_divergence_store_ahead_total"),
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
    "recheck": ("candidates_cleared: forced-pass candidates the re-check GET cleared; no_delivery_record: "
                "candidates counted without a re-check (the GVR has no delivery record)"),
    "lost_update_rule": ("candidates_cleared: candidates the lost-update rule cleared, at either site (store "
                         "agreed, ahead, or had received the state); no_delivery_record: divergences published "
                         "for a GVR with no delivery record; pending_*: keys held until the store's applied "
                         "position reaches their read"),
}


def _num(v):
    return v if isinstance(v, (int, float)) and not isinstance(v, bool) else None


def mechanism(x: dict) -> tuple[str | None, str]:
    """(the judging mechanism this /debug/vars snapshot publishes, why)."""
    v = x.get(VERIFY)
    if not isinstance(v, dict):
        return None, f"publishes no {VERIFY}"
    hits = {m: [s for s in d["identified_by"] if s in v] for m, d in MECHANISMS.items()}
    found = [m for m, k in hits.items() if k]
    if len(found) == 1:
        return found[0], f"publishes {', '.join(f'{VERIFY}.{s}' for s in hits[found[0]])}"
    if not found:
        return None, ("publishes neither " + " nor ".join("/".join(d["identified_by"])
                                                           for d in MECHANISMS.values()))
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
        out[name] = dict(m) if isinstance(m, dict) else None
    return out
