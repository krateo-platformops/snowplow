package api

import (
	"fmt"

	templates "github.com/krateo-platformops/snowplow/apis/templates/v1"
)

// topologicalSort is the order a RESTAction's stages run in (the resolve's stage
// loop and the inspect walk): first every stage with no dependency, in declared
// order, then each dependent as it becomes ready, first ready first.
//
// #1152 — the first queue used to be read out of a Go map, so independent stages
// ran in a different order from one resolve to the next: which stage spent the
// page's budget, which failure the resolve reported, and the order of records
// under a shared errorKey all changed between identical requests. Only that
// randomness is gone: every order this returns is one the map could have
// produced, so no stage runs earlier relative to the independent stages than it
// could before.
//
// A dependency cycle, or a dependency on a stage the spec does not declare, is an
// error. A name declared twice is one stage.
func topologicalSort(items []*templates.API) ([]string, error) {
	graph := make(map[string][]string)
	inDegree := make(map[string]int)
	itemSet := make(map[string]bool)

	for _, item := range items {
		itemSet[item.Name] = true

		if item.DependsOn == nil {
			continue
		}

		if dep := item.DependsOn.Name; len(dep) > 0 {
			graph[dep] = append(graph[dep], item.Name)
			inDegree[item.Name]++
		}
	}

	var queue []string
	queued := make(map[string]bool, len(itemSet))
	for _, item := range items {
		if inDegree[item.Name] == 0 && !queued[item.Name] {
			queued[item.Name] = true
			queue = append(queue, item.Name)
		}
	}

	var sortedItems []string
	for len(queue) > 0 {
		item := queue[0]
		queue = queue[1:]
		sortedItems = append(sortedItems, item)

		for _, dependent := range graph[item] {
			inDegree[dependent]--
			if inDegree[dependent] == 0 {
				queue = append(queue, dependent)
			}
		}
	}

	if len(sortedItems) != len(itemSet) {
		return nil, fmt.Errorf("cyclic dependency detected")
	}

	return sortedItems, nil
}
