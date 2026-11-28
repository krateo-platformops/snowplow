//go:build ignore

// gen_zones regenerates zones.txt from the Go toolchain's lib/time/zoneinfo.zip,
// the archive time/tzdata embeds. Run it with `go generate ./internal/viewercontext`
// whenever TestZonesListMatchesToolchainZip says the list and the toolchain differ.
package main

import (
	"archive/zip"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
)

func main() {
	zr, err := zip.OpenReader(filepath.Join(runtime.GOROOT(), "lib", "time", "zoneinfo.zip"))
	if err != nil {
		log.Fatal(err)
	}
	defer zr.Close()
	var names []string
	for _, f := range zr.File {
		// Factory is tzdata's placeholder, not a zone anyone is in.
		if f.Name == "Factory" || strings.HasSuffix(f.Name, "/") {
			continue
		}
		names = append(names, f.Name)
	}
	sort.Strings(names)
	if err := os.WriteFile("zones.txt", []byte(strings.Join(names, "\n")+"\n"), 0o644); err != nil {
		log.Fatal(err)
	}
}
