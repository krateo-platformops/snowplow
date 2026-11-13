"""A Cloud Storage a fake gcloud plays (#1390): `gcloud storage cp` and `gcloud storage objects list`, as gcloud
561 runs them, over a directory standing in for every bucket (FAKE_GCS, else $FAKE/gcs): the object
gs://B/a/b is the file <root>/B/a/b. Modelled on gcloud's answers, not on an outcome:

  - `cp --recursive [--no-clobber] DIR gs://B/P/`: every regular file of DIR becomes P/<DIR's name>/<path>;
    `cp FILE gs://B/OBJ`: one object; `cp gs://B/OBJ FILE`: one object down ("One or more URLs matched no
    objects", rc 1, when there is none); `cp --recursive gs://B/P DEST/`: every object under P/ to
    DEST/<P's last part>/<path>. --no-clobber skips an object that exists, silently, as gcloud does;
  - `objects list gs://B/P/** --raw --exhaustive --format json`: the API's objects under P/: name, bucket,
    size (a string, as the JSON API gives it) and md5Hash (base64);
  - faults: FAKE_GCS_CP_FAIL=1 (an upload refused, rc 1); FAKE_GCS_LOSE=<suffix> (the upload answers rc 0 and
    the object whose name ends so never lands); FAKE_GCS_LIST_DROP=<suffix> (the listing answers rc 0 WITHOUT
    the object whose name ends so: what a listing that lost an object looks like); FAKE_GCS_LIST_FAIL=1 (rc 1);
    FAKE_GCS_CORRUPT=<suffix> (a download flips the last byte of that file); FAKE_GCS_DOWN_FAIL=1 (downloads
    refused, rc 1).

HANDLER is the source a fake gcloud puts in front of its body (it defines `gcs(argv)`: exits when it answered
the call, returns when the call is not a storage object call).
"""

from __future__ import annotations

HANDLER = r'''
def gcs(argv):
    import base64 as _b64, hashlib as _hl, json as _js, os as _os, shutil as _sh, sys as _sy
    if argv[:2] not in (["storage", "cp"], ["storage", "objects"]):
        return
    root = _os.environ.get("FAKE_GCS") or _os.path.join(_os.environ["FAKE"], "gcs")
    flags = [x for x in argv if x.startswith("-")]
    pos = [x for x in argv[2:] if not x.startswith("-")]
    def obj_path(url):
        b, _, name = url[len("gs://"):].partition("/")
        return b, name, _os.path.join(root, b, name)
    def md5(p):
        return _b64.b64encode(_hl.md5(open(p, "rb").read()).digest()).decode()
    def under(b, pfx):
        base = _os.path.join(root, b)
        out = []
        for dp, _dn, fn in _os.walk(base):
            for f in fn:
                name = _os.path.relpath(_os.path.join(dp, f), base).replace(_os.sep, "/")
                if name.startswith(pfx):
                    out.append(name)
        return sorted(out)
    def put(src, url):
        b, name, p = obj_path(url)
        if "--no-clobber" in flags and _os.path.exists(p):
            return
        if _os.environ.get("FAKE_GCS_LOSE") and name.endswith(_os.environ["FAKE_GCS_LOSE"]):
            return
        _os.makedirs(_os.path.dirname(p), exist_ok=True)
        _sh.copyfile(src, p)
    if argv[:3] == ["storage", "objects", "list"]:
        if _os.environ.get("FAKE_GCS_LIST_FAIL") == "1":
            _sy.stderr.write("ERROR: (gcloud.storage.objects.list) HTTPError 503: backend unavailable\n"); _sy.exit(1)
        b, pfx, _p = obj_path(pos[1])
        pfx = pfx[:-2] if pfx.endswith("**") else pfx
        rows = [{"kind": "storage#object", "bucket": b, "name": n, "size": str(_os.path.getsize(_os.path.join(root, b, n))),
                 "md5Hash": md5(_os.path.join(root, b, n))} for n in under(b, pfx)
                if not (_os.environ.get("FAKE_GCS_LIST_DROP") and n.endswith(_os.environ["FAKE_GCS_LIST_DROP"]))]
        if not rows:
            _sy.stderr.write("ERROR: (gcloud.storage.objects.list) One or more URLs matched no objects.\n"); _sy.exit(1)
        print(_js.dumps(rows)); _sy.exit(0)
    src, dst = pos[0], pos[1]
    if not src.startswith("gs://"):
        if _os.environ.get("FAKE_GCS_CP_FAIL") == "1":
            _sy.stderr.write("ERROR: (gcloud.storage.cp) HTTPError 403: storage.objects.create denied\n"); _sy.exit(1)
        if _os.path.isdir(src):
            name = _os.path.basename(_os.path.normpath(src))
            for dp, _dn, fn in _os.walk(src):
                for f in fn:
                    rel = _os.path.relpath(_os.path.join(dp, f), src).replace(_os.sep, "/")
                    put(_os.path.join(dp, f), dst.rstrip("/") + "/" + name + "/" + rel)
        else:
            put(src, dst)
        _sy.exit(0)
    if _os.environ.get("FAKE_GCS_DOWN_FAIL") == "1":
        _sy.stderr.write("ERROR: (gcloud.storage.cp) HTTPError 503: backend unavailable\n"); _sy.exit(1)
    b, name, p = obj_path(src)
    def get(from_p, to_p):
        _os.makedirs(_os.path.dirname(to_p) or ".", exist_ok=True)
        _sh.copyfile(from_p, to_p)
        c = _os.environ.get("FAKE_GCS_CORRUPT")
        if c and to_p.endswith(c):
            data = bytearray(open(to_p, "rb").read())
            data[-1] ^= 1
            open(to_p, "wb").write(bytes(data))
    if "--recursive" in flags or "-r" in flags:
        names = under(b, name.rstrip("/") + "/")
        if not names:
            _sy.stderr.write("ERROR: (gcloud.storage.cp) One or more URLs matched no objects.\n"); _sy.exit(1)
        last = name.rstrip("/").rsplit("/", 1)[-1]
        for n in names:
            get(_os.path.join(root, b, n), _os.path.join(dst, last, n[len(name.rstrip("/")) + 1:]))
        _sy.exit(0)
    if not _os.path.isfile(p):
        _sy.stderr.write("ERROR: (gcloud.storage.cp) One or more URLs matched no objects.\n"); _sy.exit(1)
    get(p, dst)
    _sy.exit(0)
'''
