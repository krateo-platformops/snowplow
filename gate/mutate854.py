"""#854 logic-only mutations: apply one, run its named arm(s), expect RED, restore."""
import subprocess
import sys

ROOT = '/private/tmp/claude-501/-Users-diegobraga-krateo-snowplow-cache-snowplow--claude-worktrees-gate-118cv2/35e88d50-8fa5-4a2a-9780-d3b7ef3f70cf/scratchpad/wt-854/go/snowplow/'
API = './internal/resolvers/restactions/api/'
CACHE = './internal/cache/'
DISP = './internal/handlers/dispatchers/'
LC = 'internal/cache/list_cell.go'

M = [
 ('M1 pending recorded AFTER the seq bump', 'internal/cache/deps.go',
  '''	noteListCellsPending(gvr, namespace, name)
''', '', API, 'TestListCell854_F3a'),
 # M1 needs the call re-inserted after the bump; done as a second edit below.
 ('M2 selector not re-matched on a change', LC,
  '''			exists = m
		}
		ci, si, found := locate(chunks, k)''', '''			_ = m
		}
		ci, si, found := locate(chunks, k)''', API, 'TestListCell854_F1_'),
 ('M3 insert appended, not at its sorted position', LC,
  '''				sl = append(sl, listSlot{})
				copy(sl[si+1:], sl[si:])
				sl[si] = slot''', '''				sl = append(sl, slot)''', API, 'TestListCell854_F1g'),
 ('M4 servability epoch not checked', LC,
  '''	if rw.ServabilityEpoch(c.gvr) != c.servEpoch {''', '''	if rw.ServabilityEpoch(c.gvr) != c.servEpoch && false {''', API, 'TestListCell854_F2c'),
 ('M5 lost-event signals ignored', LC,
  '''	if depWatchLostEventSignal() != c.lostSig {''', '''	if depWatchLostEventSignal() != c.lostSig && false {''', API, 'TestListCell854_F2d'),
 ('M6 type-level event does not unanchor', 'internal/cache/deps.go',
  '''	unanchorListCellsForGVR(gvr, "type_event")
''', '', API, 'TestListCell854_F2t'),
 ('M7 comparator ignores dep-worker in-flight keys', LC,
  '''		if depWatchInFlight(depEventKey{gvr: c.gvr, namespace: k.ns, name: k.name}) {
			return true
		}''', '', API, 'TestListCell854_F5b_KeyMid'),
 ('M7b comparator ignores undelivered keys', LC,
  '''		return deliveredMismatch(c.gvr, k, idOf(idxRaw))''', '''		return false''', API, 'TestListCell854_F5b_Storm'),
 ('M8 comparator skips the obj-from-raw identity', LC,
  '''			if s.objSrc != idOf(s.raw) {''', '''			if s.objSrc != idOf(s.raw) && false {''', API, 'TestListCell854_F5c'),
 ('M9 comparator never scheduled by reads', LC,
  '''	r := c.reads.Add(1)
	last := c.lastPassAt.Load()''', '''	r := c.reads.Add(1)
	if r > 0 {
		return
	}
	last := c.lastPassAt.Load()''', API, 'TestListCell854_F5r'),
 ('M9b comparator never confirms a mismatch', LC,
  '''		if prev, seen := c.suspects[k]; seen && prev == ids {''', '''		if prev, seen := c.suspects[k]; seen && prev == ids && false {''', API, 'TestListCell854_F5a'),
 ('M10 no-op skip compares the wrong field', LC,
  '''			if idOf(old.raw) == idOf(raw) {
				continue // the same indexer slice: nothing changed''', '''			if len(old.raw) > 0 && len(raw) > 0 && old.raw[0] == raw[0] {
				continue // the same indexer slice: nothing changed''', API, 'TestListCell854_F1_'),
 ('M11 a resync builds a new body', LC,
  '''	if !changed {
		return cur, nil
	}''', '''	if !changed && false {
		return cur, nil
	}''', CACHE, 'TestListCell854_R0'),
 ('M12 byte-equal re-delivery keeps the old slice', LC,
  '''			ns := old
			ns.raw = raw''', '''			ns := old
			ns.raw = raw
			if bytes.Equal(old.raw, raw) {
				ns.raw = old.raw
			}''', CACHE, 'TestListCell854_C6'),
 ('M13 every generation copies the whole table', LC,
  '''	chunks := append([]*listChunk(nil), cur.chunks...)''', '''	chunks := make([]*listChunk, 0, len(cur.chunks))
	for _, ch := range cur.chunks {
		chunks = append(chunks, &listChunk{slots: append([]listSlot(nil), ch.slots...)})
	}''', CACHE, 'TestListCell854_CH'),
 ('M14 envLen omits the separators', LC,
  '''	if b.n > 1 {
		l += int64(b.n - 1)
	}''', '', API, 'TestListCell854_F1g'),
 ('M16 decode with apimachinery json (int64) not encoding/json', 'internal/resolvers/restactions/api/apistage.go',
  '''	var it map[string]any
	if err := json.Unmarshal(raw, &it); err != nil {
		return nil, err
	}
	stripManagedFields(it)
	return &unstructured.Unstructured{Object: it}, nil''', '''	u := &unstructured.Unstructured{}
	if err := u.UnmarshalJSON(raw); err != nil {
		return nil, err
	}
	stripManagedFields(u.Object)
	return u, nil''', API, 'TestListCell854_F1g'),
 ('M17 decoder keeps managedFields', 'internal/resolvers/restactions/api/apistage.go',
  '''	stripManagedFields(it)
	return &unstructured.Unstructured{Object: it}, nil''', '''	return &unstructured.Unstructured{Object: it}, nil''', API, 'TestListCell854_F1g'),
 ('M19 served-cell identity ignores the body', 'internal/cache/served_cell.go',
  '''		if !ok || seq != sc.PutSeq || bseq != sc.BodySeq {''', '''		if !ok || seq != sc.PutSeq || (bseq != sc.BodySeq && false) {''', API, 'TestListCell854_F7b'),
 ('M20 refresher re-dispatches an incremental cell', 'internal/handlers/dispatchers/resolve_populate.go',
  '''				if _, ok := lc.Snapshot(cache.RefreshTriggerAsOf(ctx)); ok {''', '''				if _, ok := lc.Snapshot(cache.RefreshTriggerAsOf(ctx)); ok && false {''', DISP, 'TestListCell854_F3dTwin'),
 ('M21 customer read does not drain (need 0)', 'internal/resolvers/restactions/api/apistage.go',
  '''			e, hit, served := cache.ServeCellAccept(ctx, store, k, true, func(e *cache.ResolvedEntry) bool {
				return acceptInc(e, cache.DepGenEpochNow())''', '''			e, hit, served := cache.ServeCellAccept(ctx, store, k, true, func(e *cache.ResolvedEntry) bool {
				return acceptInc(e, 0)''', API, 'TestListCell854_F3c'),
 ('M22 a consumer writes into a shared item', 'internal/resolvers/restactions/api/apistage.go',
  '''	items := make([]any, 0, len(parsed.items))
	for _, it := range parsed.items {''', '''	items := make([]any, 0, len(parsed.items))
	for _, it := range parsed.items {
		it.Object["f6-mutated"] = true''', API, 'TestListCell854_F6'),
 ('M23 refresher-read stamp ignores the generation (uses the stale entry stamp)', 'internal/resolvers/restactions/api/apistage.go',
  '''					return acceptInc(e, stamp.need) && inc.AsOf() >= stamp.need''', '''					return cache.ContentStampCovers(gvr, ns, name, e.ContentAsOf, stamp.need)''', API, 'TestListCell854_F3_'),
 ('M24 anchor skips the pairing proof', LC,
  '''	if !ok || len(keyed) != len(a.Items) || !shape.holds(a.Envelope, keyed) {''', '''	if !ok || len(keyed) != len(a.Items) {''', API, 'TestListCell854_F1r'),
 ('M25 shape accepts a multi-byte separator', LC,
  '''	if len(sh.sep) != 1 ||
		!sh.holds''', '''	if !sh.holds''', CACHE, 'TestListCell854_SH'),
 ('M26 pairing comparison always holds', LC,
  '''func (sh envelopeShape) holds(env []byte, keyed []keyedRaw) bool {
''', '''func (sh envelopeShape) holds(env []byte, keyed []keyedRaw) bool {
	return true
''', CACHE, 'TestListCell854_SH'),
 ('M27 filter keeps objects the selector dropped', LC,
  '''		case !m && out == nil:''', '''		case !m && out == nil && false:''', API, 'TestListCell854_F1_'),
 ('M28 D2 stale-serve read takes no Snapshot', 'internal/resolvers/restactions/api/apistage.go',
  '''		if lc := entry.ListCell(); lc != nil && !incOK {''', '''		if lc := entry.ListCell(); lc != nil && !incOK && false {''', DISP + ' -tags=integration', 'TestIssue710_D2_17i_'),
]

# M1 is a move: remove the early call and add it after the bump.
MOVE_M1 = ('''	d.bumpCoordinateGen(gvr, namespace, name)
	matched := d.collectMatchesForMark(gvr, namespace, name)''', '''	d.bumpCoordinateGen(gvr, namespace, name)
	noteListCellsPending(gvr, namespace, name)
	matched := d.collectMatchesForMark(gvr, namespace, name)''')


def run(name, path, old, new, pkg, pattern, extra=None):
    p = ROOT + path
    src = open(p).read()
    if src.count(old) != 1:
        return name, 'PATTERN-ERROR(%d)' % src.count(old)
    mut = src.replace(old, new)
    if extra:
        if mut.count(extra[0]) != 1:
            return name, 'PATTERN-ERROR(extra)'
        mut = mut.replace(extra[0], extra[1])
    open(p, 'w').write(mut)
    try:
        r = subprocess.run(['go', 'test'] + pkg.split() + ['-run', pattern, '-count=1'], cwd=ROOT,
                           capture_output=True, text=True, timeout=900)
        out = r.stdout + r.stderr
        if 'build failed' in out or '[setup failed]' in out:
            verdict = 'BUILD-FAILED'
        elif r.returncode != 0:
            verdict = 'RED'
        else:
            verdict = 'SURVIVED'
        line = ''
        for l in out.splitlines():
            if ('_test.go:' in l) and ('RED' in l or 'want' in l or 'F' in l):
                line = l.strip()[:200]
                break
        return name, verdict + ('  | ' + line if line else '')
    finally:
        open(p, 'w').write(src)


only = sys.argv[1:]
for name, path, old, new, pkg, pattern in M:
    if only and not any(name.startswith(o + ' ') for o in only):
        continue
    extra = MOVE_M1 if name.startswith('M1 ') else None
    n, v = run(name, path, old, new, pkg, pattern, extra)
    print('%-70s %s' % (n, v), flush=True)
