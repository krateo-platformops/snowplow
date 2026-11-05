package api

import (
	"fmt"
	"slices"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// topologicalSort is the order a RESTAction's stages run in (the resolve's stage
// loop and the inspect walk): dependencies first and, wherever they allow, the
// order the author declared. At each step it takes the earliest-declared stage
// whose dependency has run, so the same spec always runs the same way (#1152).
// Before, the stages with no dependency were read out of a Go map, so independent
// stages ran in a different order from one resolve to the next: which stage spent
// the page's budget, which failure the resolve reported, and the order of records
// under a shared errorKey all changed between identical requests.
//
// A dependency cycle, or a dependency on a stage the spec does not declare, is an
// error. A name declared twice is one stage.
func topologicalSort(items []*templates.API) ([]string, error) {
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	itemSet := make(map[string]bool)
	pending := make([]string, 0, len(items))

	for _, item := range items {
		if !itemSet[item.Name] {
			pending = append(pending, item.Name)
		}
		itemSet[item.Name] = true

		if item.DependsOn == nil {
			continue
		}

		if dep := item.DependsOn.Name; len(dep) > 0 {
			graph[dep] = append(graph[dep], item.Name)
			inDegree[item.Name]++
		}
	}

	sortedItems := make([]string, 0, len(pending))
	for len(pending) > 0 {
		next := slices.IndexFunc(pending, func(name string) bool { return inDegree[name] == 0 })
		if next < 0 {
			return nil, fmt.Errorf("cyclic dependency detected")
		}
		item := pending[next]
		pending = slices.Delete(pending, next, next+1)
		sortedItems = append(sortedItems, item)

		for _, dependent := range graph[item] {
			inDegree[dependent]--
		}
	}

	return sortedItems, nil
}
