"""prep_tree.py REPO REF DEST [HARNESS_FROM_TREE]: a fresh `git archive` of REPO@REF in DEST
(must not exist). With HARNESS_FROM_TREE (a directory of a tree that carries the harness), copy
paired_fresh_1570_test.go and the branch's patched paired_865_test.go from it, and write the
main.go-equivalent wiring shim for REF (the boolean predicate) when REF has none."""
import os, shutil, subprocess, sys
repo, ref, dest = sys.argv[1], sys.argv[2], sys.argv[3]
src = sys.argv[4] if len(sys.argv) > 4 else None
if os.path.exists(dest):
    sys.exit(f"{dest} exists")
os.makedirs(dest)
sha = subprocess.check_output(["git", "-C", repo, "rev-parse", ref], text=True).strip()
tar = subprocess.run(["git", "-C", repo, "archive", sha], check=True, capture_output=True).stdout
subprocess.run(["tar", "-x", "-C", dest], input=tar, check=True)
d = "go/snowplow/internal/handlers/dispatchers"
if src:
    for f in ("paired_fresh_1570_test.go", "paired_865_test.go"):
        shutil.copy(os.path.join(src, d, f), os.path.join(dest, d, f))
    shim = os.path.join(dest, d, "paired_wire_test.go")
    if not os.path.exists(shim):
        open(shim, "w").write(open(os.path.join(os.path.dirname(os.path.abspath(__file__)), "harness", "wire_main.go.txt")).read())
open(os.path.join(dest, "TREE_SHA"), "w").write(sha + "\n")
print(dest, sha)
