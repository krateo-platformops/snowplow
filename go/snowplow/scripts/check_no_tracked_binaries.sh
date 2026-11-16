#!/bin/bash
# check_no_tracked_binaries.sh — fail if any file tracked under go/snowplow is a
# compiled executable or object (ELF, Mach-O, Mach-O universal, PE/COFF).
#
# THE DEFECT: a `go build` run inside a scripts/<tool>/ directory leaves the
# tool's binary next to its source, and a broad `git add` commits it (a 7 MB
# Mach-O checker binary reached #912's history). The repo .gitignore now ignores
# those build outputs; this gate makes CI refuse one that gets through anyway.
#
# Detection is by content (the executable-format magic in the first bytes), not
# by name or mode: tracked shell scripts are mode 755 and must stay allowed, and a
# binary can be committed under any name.
#
#   bash scripts/check_no_tracked_binaries.sh            # the gate
#   bash scripts/check_no_tracked_binaries.sh --self-test # its discrimination proof
set -euo pipefail

cd "$(dirname "$0")/.."

# is_executable_binary FILE — true when FILE starts with an executable/object
# magic: ELF 7f454c46; Mach-O feedface/feedfacf (BE) and cefaedfe/cffaedfe (LE);
# Mach-O universal cafebabe/bebafeca; PE/COFF "MZ" (4d5a).
is_executable_binary() {
	local magic
	magic=$(head -c 4 "$1" 2>/dev/null | od -An -tx1 | tr -d ' \n')
	case "$magic" in
	7f454c46 | feedface | feedfacf | cefaedfe | cffaedfe | cafebabe | bebafeca) return 0 ;;
	4d5a*) return 0 ;;
	esac
	return 1
}

if [ "${1:-}" = "--self-test" ]; then
	tmp=$(mktemp -d)
	trap 'rm -rf "$tmp"' EXIT
	printf '\x7fELF\x02\x01\x01' >"$tmp/elf"
	printf '\xcf\xfa\xed\xfe\x0c\x00' >"$tmp/macho"
	printf 'MZ\x90\x00' >"$tmp/pe"
	printf '#!/bin/bash\necho ok\n' >"$tmp/script.sh"
	printf 'package main\n' >"$tmp/main.go"
	fail=0
	for f in elf macho pe; do
		is_executable_binary "$tmp/$f" || { echo "SELF-TEST RED: $f not detected"; fail=1; }
	done
	for f in script.sh main.go; do
		! is_executable_binary "$tmp/$f" || { echo "SELF-TEST RED: $f false-flagged"; fail=1; }
	done
	[ "$fail" -eq 0 ] && echo "OK: self-test — ELF, Mach-O and PE detected; a script and a Go source pass."
	exit "$fail"
fi

found=0
while IFS= read -r -d '' f; do
	[ -f "$f" ] || continue
	if is_executable_binary "$f"; then
		echo "ERROR: tracked executable binary: go/snowplow/$f" >&2
		found=$((found + 1))
	fi
done < <(git ls-files -z .)

if [ "$found" -gt 0 ]; then
	echo "       $found compiled executable(s) are tracked. Remove them from the history (they are build" >&2
	echo "       outputs, e.g. a 'go build' in scripts/<tool>/) — .gitignore ignores scripts/*/ build outputs." >&2
	exit 1
fi
echo "OK: no tracked file under go/snowplow is a compiled executable."
