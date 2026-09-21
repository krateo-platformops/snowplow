# quantity — built-in jq module for Kubernetes resource quantities.
#
# WHAT IT IS FOR. `health` already carries the usage vocabulary (usage_pct,
# usage_health, usage_summary) and every one of those functions takes a
# NUMBER. The apiserver does not hand out numbers: a CPU request is "100m",
# a metrics.k8s.io sample is "4817987n", memory is "71192Ki". Nothing turned
# one into the other, so a RESTAction that wanted to compare what a workload
# RESERVED against what it USES had to either emit both as opaque strings —
# unsortable, unchartable, uncomparable — or open-code a suffix table in its
# own filter. This module is that missing layer, and nothing more: parse a
# quantity into base units, then hand it to the functions that already exist.
#
# Generic mechanism, no product opinion, per the same rule `health` follows.
#
# CASE IS LOAD-BEARING. "100m" is a tenth of a core; "100M" is a hundred
# million. The suffix table below is case-sensitive on purpose and there is
# no ascii_downcase anywhere in this file. Getting that wrong is a nine-order-
# of-magnitude error that renders as a plausible-looking number.

# _qmul — the multiplier for a Kubernetes quantity suffix, or null when the
# suffix is not one. Decimal SI (n/u/m//k/M/G/T/P/E) and binary SI
# (Ki/Mi/Gi/Ti/Pi/Ei), exactly the set the apiserver emits.
#
# `K` is accepted alongside the canonical lower-case `k` because it is a
# common hand-written variant and the intent is unambiguous; every other
# entry is the canonical spelling only.
def _qmul:
  { "n":  0.000000001,
    "u":  0.000001,
    "m":  0.001,
    "":   1,
    "k":  1000,
    "K":  1000,
    "M":  1000000,
    "G":  1000000000,
    "T":  1000000000000,
    "P":  1000000000000000,
    "E":  1000000000000000000,
    "Ki": 1024,
    "Mi": 1048576,
    "Gi": 1073741824,
    "Ti": 1099511627776,
    "Pi": 1125899906842624,
    "Ei": 1152921504606846976
  }[.];

# quantity — a Kubernetes quantity string to a number in BASE UNITS (cores
# for CPU, bytes for memory), or null.
#
# NULL RATHER THAN A GUESS, and never an error. A filter runs over every row
# of a live list; one pod with an unparseable value must not abort the whole
# table, and must not read as zero either — zero is a claim ("it uses
# nothing") that would flow straight into a utilisation ratio and label the
# row Overprovisioned. Absent stays absent all the way to the cell.
#
# A number passes through: already-normalised input is idempotent, so
# (quantity | quantity) == quantity.
def quantity:
  if . == null then null
  elif type == "number" then .
  elif type != "string" then null
  else . as $s
    | ($s | ltrimstr(" ") | rtrimstr(" ")) as $t
    | if $t == "" then null
      else
        # Longest numeric prefix, found by asking tonumber rather than by
        # matching a float grammar by hand — "1e3", "0.5" and ".5" are all
        # things the apiserver can emit and all things a hand-rolled regex
        # gets subtly wrong.
        ([ range(0; ($t | length) + 1) as $i
           | ($t[0:$i]) as $head
           | ($head | try tonumber catch null) as $n
           | select($n != null)
           | { n: $n, rest: ($t[$i:]) } ]
         | last) as $split
        | if $split == null then null
          else ($split.rest | _qmul) as $mul
            | if $mul == null then null else $split.n * $mul end
          end
      end
  end;

# millicores — a CPU quantity in millicores. The unit dashboards actually
# show, and the one that keeps small values legible: 4817987n is 4.817987m,
# where cores would be 0.004817987.
def millicores:
  quantity | if . == null then null else . * 1000 end;

# bytes — a memory or storage quantity in bytes. An alias for `quantity`,
# named so a filter says which dimension it is in.
def bytes: quantity;

# sum_quantities — input: an array of quantity strings (a pod's containers,
# say). Output: their total in base units, or null when NONE parsed.
#
# Unparseable entries are skipped rather than poisoning the total, but an
# array with nothing usable in it answers null, not 0 — see `quantity` for
# why a fabricated zero is the dangerous answer.
def sum_quantities:
  if type != "array" then null
  else [ .[] | quantity | select(. != null) ]
    | if length == 0 then null else add end
  end;

# utilization_pct(used; capacity) — percentage (0-100, one decimal) from two
# QUANTITIES, or null when either is absent or capacity is zero.
#
# Deliberately the same shape and rounding as health's usage_pct, so the two
# are interchangeable once the strings have become numbers; this one just
# does the parsing first.
def utilization_pct(used; capacity):
  (used | quantity) as $u
  | (capacity | quantity) as $c
  | if ($u == null or $c == null or $c == 0) then null
    else ((($u / $c) * 1000 | round) / 10)
    end;

# provisioning_verdict(pct; overAt; underAt) — a utilisation percentage onto
# the normalized vocabulary Overprovisioned / Right-sized / Underprovisioned
# / Unknown.
#
# NOT usage_health, which is the wrong axis. usage_health answers "is this
# filling up" and grades HIGH as Critical. Provisioning is two-sided: high
# utilisation risks throttling and eviction, low utilisation is reserved
# capacity nobody can use. A disk at 5% is fine; a request at 5% is waste.
def provisioning_verdict(pct; overAt; underAt):
  (pct) as $p
  | if $p == null then "Unknown"
    elif $p >= underAt then "Underprovisioned"
    elif $p <= overAt then "Overprovisioned"
    else "Right-sized"
    end;

# provisioning_verdict — the conventional thresholds: at or under 25% is
# Overprovisioned, at or over 80% is Underprovisioned.
#
# CONVENTIONAL, NOT DERIVED. They are a starting point for a fleet nobody has
# tuned, and they live here so that changing the policy is one edit rather
# than one edit per RESTAction — the same argument that put the health
# keyword lists in this package instead of in every caller.
def provisioning_verdict(pct): provisioning_verdict(pct; 25; 80);

# provisioning_severity — numeric rank of a verdict, for sorting.
#
# Underprovisioned outranks Overprovisioned because the failure modes are not
# comparable: one throttles or evicts a workload now, the other spends money.
# Unknown sits above Right-sized for the reason health does the same — an
# unreadable row must not settle to the bottom as though it were fine.
#
# The exact inverse of the ordering below, so a verdict round-trips.
def provisioning_severity:
  { "Underprovisioned": 3, "Overprovisioned": 2, "Unknown": 1, "Right-sized": 0 }[.] // 1;
