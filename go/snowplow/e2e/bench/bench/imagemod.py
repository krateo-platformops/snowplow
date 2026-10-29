"""The Go modules a container image's binary was built with, read from the
image itself in its registry. STDLIB ONLY: plain HTTPS to the registry, no
Docker and no cluster exec.

A Go binary carries its build info (runtime/debug.BuildInfo) as text: one
`dep\t<module>\t<version>[\t<sum>]` line per dependency, and a `=>` line
after one that is replaced. So "which provider-runtime does this controller
carry" is answered from the bytes the cluster runs, the image digest the
pod reports (status.containerStatuses[].imageID), never from a pin file or
a release note.

The registry protocol: an anonymous pull token from the realm the registry
names in its 401 (WWW-Authenticate: Bearer realm=..,service=..,scope=..),
the manifest by digest (an index resolves to its linux/amd64 manifest),
then the layers top-down until a file that is an ELF executable carries the
module asked for.
"""

from __future__ import annotations

import io
import json
import re
import tarfile
import urllib.error
import urllib.parse
import urllib.request

_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))
_REF = re.compile(r"^(?:docker-pullable://)?(?P<reg>[^/]+)/(?P<repo>[^@:]+(?::[0-9]+)?[^@:]*)"
                  r"(?:@(?P<digest>sha256:[0-9a-f]{64})|:(?P<tag>[\w.-]+))$")


def parse_ref(ref: str) -> tuple[str, str, str]:
    """(registry, repository, digest-or-tag) of `ghcr.io/org/name@sha256:…` or `…:tag`."""
    m = _REF.match(ref.strip())
    if not m:
        raise ValueError(f"not an image reference: {ref!r}")
    return m.group("reg"), m.group("repo"), m.group("digest") or m.group("tag")


def build_deps(data: bytes) -> dict[str, dict]:
    """Every `dep` line of a Go binary's build info: {module: {"version", "replaced_by"?}}."""
    out: dict[str, dict] = {}
    for m in re.finditer(rb"\ndep\t([^\t\n]+)\t([^\t\n]+)(?:\t[^\n]*)?\n(=>\t([^\t\n]+)\t([^\t\n]+)[^\n]*\n)?", data):
        mod = m.group(1).decode("utf-8", "replace")
        rec = {"version": m.group(2).decode("utf-8", "replace")}
        if m.group(3):
            rec["replaced_by"] = {"module": m.group(4).decode("utf-8", "replace"),
                                  "version": m.group(5).decode("utf-8", "replace")}
        out[mod] = rec
    return out


def resolve(path: str, links: dict[str, str]) -> str:
    """`path` with every symlinked component resolved through the image's own
    symlinks (`/bin` -> `usr/bin` in a usr-merged base image)."""
    import posixpath
    parts, out, hops = [p for p in path.split("/") if p], "/", 0
    while parts:
        cur = posixpath.join(out, parts.pop(0))
        if cur in links and hops < 40:
            hops += 1
            tgt = links[cur]
            base = "/" if tgt.startswith("/") else out
            parts = [p for p in posixpath.normpath(posixpath.join(base, tgt)).split("/") if p] + parts
            out = "/"
        else:
            out = cur
    return out


class Oversize(RuntimeError):
    """A blob that sends more bytes than its manifest declared: refused, never read further."""


class _Bounded(io.RawIOBase):
    """A blob stream that refuses to yield more than the manifest's declared
    size: memory stays bounded by the chunk being read, and a registry (or a
    proxy) sending more than it declared is refused loudly."""

    def __init__(self, resp, limit: int, what: str):
        self.resp, self.limit, self.what, self.n = resp, limit, what, 0

    def readable(self):
        return True

    def readinto(self, b):
        k = self.resp.readinto(b) if hasattr(self.resp, "readinto") else None
        if k is None:
            chunk = self.resp.read(len(b))
            k = len(chunk)
            b[:k] = chunk
        self.n += k
        if self.n > self.limit:
            raise Oversize(f"{self.what}: more than its declared {self.limit} bytes; refused")
        return k


_CHUNK = 1 << 20
# a Go build-info `dep` line is a module path, a version and a sum: far shorter than this, so
# a line cut by a chunk boundary is whole in the overlap
_OVERLAP = 1 << 13


def _scan_deps(fh) -> dict[str, dict]:
    """build_deps over a member read in chunks (bounded memory), each chunk
    with the previous chunk's tail, so no line is lost at a boundary."""
    deps: dict[str, dict] = {}
    tail = b""
    while True:
        chunk = fh.read(_CHUNK)
        if not chunk:
            return deps
        data = tail + chunk
        deps.update(build_deps(data))
        tail = data[-_OVERLAP:]


class Registry:
    def __init__(self, registry: str, repo: str, *, opener=urllib.request.urlopen, scheme: str = "https"):
        self.base = f"{scheme}://{registry}/v2/{repo}"
        self.opener = opener
        self.token: str | None = None

    def _get(self, url: str, accept: str | None = None):
        def req():
            r = urllib.request.Request(url, headers={**({"Accept": accept} if accept else {}),
                                                     **({"Authorization": f"Bearer {self.token}"}
                                                        if self.token else {})})
            return self.opener(r, timeout=120)
        try:
            return req()
        except urllib.error.HTTPError as e:
            if e.code != 401 or self.token is not None:
                raise
            self.token = self._token(e.headers.get("WWW-Authenticate") or "")
            return req()

    def _token(self, challenge: str) -> str:
        params = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
        if "realm" not in params:
            raise RuntimeError(f"the registry asked for auth without a Bearer realm: {challenge[:120]!r}")
        q = urllib.parse.urlencode({k: v for k, v in params.items() if k in ("service", "scope")})
        with self.opener(urllib.request.Request(f"{params['realm']}?{q}"), timeout=60) as r:
            body = json.loads(r.read())
        return body.get("token") or body.get("access_token")

    def manifest(self, ref: str) -> dict:
        with self._get(f"{self.base}/manifests/{ref}", _ACCEPT) as r:
            m = json.loads(r.read())
        if m.get("manifests"):                                     # an index: its linux/amd64 image
            sub = next((x for x in m["manifests"] if (x.get("platform") or {}).get("os") == "linux"
                        and (x.get("platform") or {}).get("architecture") == "amd64"), None)
            if sub is None:
                raise RuntimeError("the image index has no linux/amd64 manifest")
            return self.manifest(sub["digest"])
        return m

    def blob(self, digest: str):
        return self._get(f"{self.base}/blobs/{digest}")


def _scheme(registry: str) -> str:
    """https, except for a loopback registry (Docker's own rule: a registry on
    localhost / 127.0.0.1 is reached over plain http)."""
    host = registry.rsplit(":", 1)[0] if registry.count(":") == 1 else registry
    return "http" if host in ("localhost", "127.0.0.1") else "https"


def image_deps(ref: str, modules: tuple[str, ...], *, command: list[str] | None = None,
               opener=urllib.request.urlopen) -> dict:
    """The version of the first of `modules` (one module under its known
    paths, e.g. an org rename) in the image's PROCESS binary: the container's
    `command` when the pod sets one, else the image config's Entrypoint (Cmd
    when there is none). An image can ship several Go binaries built with
    different versions (oasgen-provider 0.28.1 carries /usr/bin/oasgen-render
    beside its controller), so every ELF executable listing `module` is
    reported under "binaries", and "version" is the process binary's (None,
    with "why", when it does not list the module)."""
    reg, repo, ref_ = parse_ref(ref)
    r = Registry(reg, repo, opener=opener, scheme=_scheme(reg))
    man = r.manifest(ref_)

    def declared(d: dict, what: str) -> int:
        if not isinstance(d.get("size"), int):
            raise Oversize(f"{what} {d.get('digest')}: the manifest declares no size; refused")
        return d["size"]
    with r.blob(man["config"]["digest"]) as resp:
        cfg = (json.loads(_Bounded(resp, declared(man["config"], "config"), "config").read()
                          or b"{}").get("config") or {})
    argv = list(command or cfg.get("Entrypoint") or cfg.get("Cmd") or [])
    entry = argv[0] if argv else None
    binaries: dict[str, dict] = {}
    links: dict[str, str] = {}
    seen: set[str] = set()
    layers = man.get("layers") or []
    for layer in reversed(layers):                       # top layer first: an upper file hides a lower one
        mt = layer.get("mediaType") or ""
        if "zstd" in mt:
            raise Oversize(f"layer {layer.get('digest')}: {mt} is not a compression this reader streams; refused")
        limit = declared(layer, "layer")
        with r.blob(layer["digest"]) as resp, tarfile.open(
                fileobj=io.BufferedReader(_Bounded(resp, limit, f"layer {layer['digest']}"), _CHUNK),
                mode="r|*") as tar:                      # a stream: gzip or plain, never all in memory
            for mem in tar:
                path = "/" + mem.name.lstrip("./").rstrip("/")
                if path in seen:
                    continue
                seen.add(path)
                if mem.issym():
                    links[path] = mem.linkname
                    continue
                if not mem.isfile() or mem.size < 4:
                    continue
                fh = tar.extractfile(mem)
                if fh is None or fh.read(4) != b"\x7fELF":
                    continue
                deps = _scan_deps(fh)
                hit = next((m for m in modules if m in deps), None)
                if hit:
                    binaries[path] = {"module": hit, **deps[hit]}
    entry = resolve(entry, links) if entry else None
    out = {"image": ref, "process_binary": entry, "binaries": binaries}
    if entry in binaries:
        return {**out, **binaries[entry]}
    return {**out, "module": None, "version": None,
            "why": (f"the process binary {entry} lists none of {', '.join(modules)}" if entry else
                    "the image config names no Entrypoint or Cmd, and the pod no command")}
