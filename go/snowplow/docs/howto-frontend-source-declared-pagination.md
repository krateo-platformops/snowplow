# Frontend integration: source-declared pagination (`status.resourcesRefs.slice`) — #874

**Audience:** a frontend session integrating the SPA (frontend-draganddrop) and portal chart authors.
**Status:** snowplow-side semantics shipped in the #874 PR. The SPA needs **no code change** for anything in this PR. The `cursor=` round-trip on `/call` is a **later, separate** change (see §5) and is **not** accepted by `/call` today.

## 1. What changed on the wire

For a widget whose items fan out from its apiRef RESTAction (`spec.apiRef` plus `spec.resourcesRefsTemplate`), snowplow emits pagination state in the resolved widget. It does so only when the `/call` carried pagination (`?page=N&perPage=M`, both > 0):

```json
"status": {
  "resourcesRefs": {
    "items": [ ... ],
    "slice": { "perPage": 5, "page": 2, "continue": true, "cursor": "abc" }
  }
}
```

| Field | Before #874 | After #874 |
|---|---|---|
| `continue` | `len(items) >= perPage`, an inference from the count | **the data source's declaration** (see §2); `false` when the source declares nothing |
| `page` | `page+1` when continue, else `page` | unchanged rule |
| `perPage` | echo | unchanged |
| `cursor` | absent | **new, additive**: the source's continuation token for the next page. Present only when `continue` is true **and** the source emitted a non-empty cursor |

The `opts.PerPage > 0 && opts.Page > 0` guard is unchanged: **an unpaginated `/call` gets no `slice` at all**, exactly as before.

## 2. The rule: who says "there is more"

The apiRef RESTAction says it, in its own filter output, under a top-level `slice` key:

1. `slice.continue` (bool) is taken as is. Use it when the RA windows by `.slice.offset` / `.slice.page`, or projects a Kubernetes LIST's `metadata.continue`.
2. Otherwise, `slice.cursor` (string): there is more iff it is non-empty.
3. Otherwise there is **exactly one page**. This holds by construction; it is not a cap.

### Chart-author recipes (RESTAction `spec.filter`)

Offset windowing over a list the RA already has:

```jq
(.items // []) as $all
| (.slice.offset // 0) as $o | (.slice.perPage // ($all|length)) as $pp
| { items: $all[$o : $o + $pp],
    slice: { continue: (($o + $pp) < ($all | length)) } }
```

Cursor sources (upstream `*-panels` RAs already do this):

```jq
{ compositionspanels: [ .compositionspanels.items[] | .raw ],
  slice: { cursor: .compositionspanels.cursor, perPage: .slice.perPage } }
```

The RA receives the cursor of the page being asked for as `.slice.cursor`, next to `.slice.page`, `.slice.perPage` and `.slice.offset`.

**A windowing RA that declares neither field now stops after page 1.** That is the point: a source that ignores the window (krateo-057's `sidebar-nav`) no longer looks endless.

## 3. What the SPA does today, and why it is unaffected

Read from `ui/src/hooks/useWidgetQuery.ts`, `components/WidgetRenderer`, `components/Pagination/*`:

- `getNextPageParam` reads `lastPage.status.resourcesRefs.slice.continue === true` and asks for `page+1`.
  - It returns `undefined` immediately when the request carried no page (`typeof pageParams.page !== 'number'`).
  - It also returns `undefined` in classic-pager mode (`defaultPageSize`, e.g. the compositions table, which pages through `widgetData` and never reads `continue`).
- **The Shell's Menu (sidebar-nav)** is fetched **without** page/perPage. snowplow emits no `slice`, so the SPA never paged it, before or after.
- **`ScrollPagination`** wraps every widget kind registered `paginated: true` (`WidgetRenderer.tsx:73-85`), and today that is only `Listy` (`widgets/List/index.ts:10`). **`ButtonPagination`** is not wired anywhere.
- Infinite scroll needs a request that carries a page. A widget gets one only when the ref that leads to it declares `resourcesRefs.items[].slice`, and a declared slice also puts `useWidgetQuery` into the classic-pager mode (`boundedWindow`), which never reads `continue`. The 1.8.84 portal deployed on krateo-057 (92 RAs, 361 widgets; reviewer-818's corpus scan) has no Listy paged that way.
- Upstream portal lists backed by the `*-panels` RAs emit `slice.cursor`. Their `continue` becomes `cursor != ""`, which is true while the presenter has more, as before.

So the SPA reads the same field with the same type. No SPA change ships with #874.

## 4. Edge cases

- **The source declares `continue: true` forever** (a buggy RA): the prewarm drain stops at its existing 20,000-page liveness backstop (unchanged). The SPA keeps offering "more" exactly as long as the source says so.
- **The source emits a cursor but `/call` cannot pass it back yet** (§5): in the SPA, page N re-reads the source's first window; this is today's behaviour for upstream cursor RAs, unchanged by #874. The prewarm drain **does** thread it (snowplow-internal).
- **4a full-list serve:** an RA that declares `continue` or `cursor` is never served from the page-independent full list (a page sliced from the full list would carry the full list's declaration). It is resolved page-keyed.

## 5. Later (NOT in #874): the `cursor=` round-trip

When snowplow accepts a `cursor` query parameter on `/call` (a separate change, with its own doc update), the SPA flow will be:

```ts
getNextPageParam: (lastPage, _all, pageParams) => {
  const s = typeof lastPage.status === 'object' ? lastPage.status?.resourcesRefs?.slice : undefined
  if (typeof pageParams.page !== 'number' || usesClassicPager || s?.continue !== true) return undefined
  return { page: pageParams.page + 1, perPage: pageParams.perPage, cursor: s.cursor }   // cursor may be undefined
}
// fetchWidget: if (cursor) requestUrl.searchParams.set('cursor', cursor)
```

Until that ships, **do not send `cursor=`**. No `/call` handler reads it today (the dispatchers read `page` and `perPage` only, `helpers.go` `paginationInfo`), so it would have no effect.
