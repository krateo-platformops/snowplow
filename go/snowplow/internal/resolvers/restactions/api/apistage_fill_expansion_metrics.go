// apistage_fill_expansion_metrics.go — #787 step 1: the content-fill admission
// gate's LEARNED decode expansion, made visible on OTLP.
//
// THE LEAD (#787). The gate charges each fill raw × expansion(GVR), and the
// expansion self-calibrates from the live-heap delta across parse + Put
// (apistage_fill_admission.go). Under GC pressure a partial GC during the fill
// shrinks that delta, the asymmetric average decays toward the smaller sample,
// and the gate may undercharge exactly when memory is tight (in-process -race
// runs learned ≈9-10 where unit runs learned ≈14). The per-GVR expansion was on
// /debug/vars only, so neither 057's 7-day reading nor the 50K bench could see
// it next to the heap-pressure signal. This file exports, read-only:
//
//   - per GVR, the learned expansion (a gauge, gvr-labelled, capped like every
//     gvr family: the GVRs with the most fill bytes keep their series, the tail
//     folds into gvr="__other__" at its MINIMUM — the risk is a LOW value);
//   - a label-free summary {min, max, weighted}: weighted is the mean weighted
//     by each GVR's fill bytes;
//   - the calibration samples by outcome {seeded, raised, decayed, ignored}, so
//     a fall is attributable (decays under pressure) and its denominator is
//     visible.
//
// Read them beside the heap-pressure signals already exported: the admission
// ceiling's over_ceiling_bytes (+ _peak) and the runtime's go.memory gauges.
// Observation only: nothing reads these to decide anything.
package api

import (
	"sort"
	"sync/atomic"

	"github.com/krateo-platformops/snowplow/internal/cache"
)

// Calibration sample outcomes (closed enum, OTLP attribute outcome).
type calibrationOutcome uint8

const (
	calibSeeded  calibrationOutcome = iota // the GVR's first measurement replaced the seed
	calibRaised                            // a sample >= the current value, taken at once
	calibDecayed                           // a smaller sample, decayed toward
	calibIgnored                           // a sample <= 0 (no measurement)
	calibOutcomes
)

func (o calibrationOutcome) String() string {
	switch o {
	case calibSeeded:
		return "seeded"
	case calibRaised:
		return "raised"
	case calibDecayed:
		return "decayed"
	case calibIgnored:
		return "ignored"
	}
	return "unknown"
}

var contentFillCalibrationSamples [calibOutcomes]atomic.Uint64

// contentFillExpansionSeriesCap bounds the per-GVR gauge's series in ONE OTLP
// collection. The GVRs that ever had a content fill are bounded by the
// registered GVRs (169 on 057); the cap is headroom over that, and an overflow
// is counted on snowplow_metrics_series_truncated_total{family}.
const contentFillExpansionSeriesCap = 256

// contentFillExpansionFamily names the truncation family.
const contentFillExpansionFamily = "content_fill_expansion"

// ContentFillExpansionCell is one gvr's learned expansion for the OTLP mirror.
type ContentFillExpansionCell struct {
	GVR       string
	Expansion float64
}

// contentFillBytesByGVR returns each GVR's total content-fill bytes.
func contentFillBytesByGVR() map[string]uint64 {
	contentFillMu.Lock()
	defer contentFillMu.Unlock()
	out := make(map[string]uint64, len(contentFillByGVRMap))
	for gvr, g := range contentFillByGVRMap {
		out[gvr] = g.Bytes
	}
	return out
}

// ContentFillExpansionCells returns at most contentFillExpansionSeriesCap+1
// series: the GVRs with the most fill bytes (ties on the GVR name), and the
// tail folded into one gvr="__other__" series at the tail's MINIMUM expansion.
// Only GVRs with a measured expansion appear (a GVR still on the seed has no
// learned value to read). The fold is counted as truncation.
func ContentFillExpansionCells() []ContentFillExpansionCell {
	expansion, _ := contentFillCalibrationSnapshot()
	if len(expansion) == 0 {
		return nil
	}
	bytes := contentFillBytesByGVR()
	cells := make([]ContentFillExpansionCell, 0, len(expansion))
	for gvr, e := range expansion {
		cells = append(cells, ContentFillExpansionCell{GVR: gvr, Expansion: e})
	}
	sort.Slice(cells, func(i, j int) bool {
		bi, bj := bytes[cells[i].GVR], bytes[cells[j].GVR]
		if bi != bj {
			return bi > bj
		}
		return cells[i].GVR < cells[j].GVR
	})
	if len(cells) <= contentFillExpansionSeriesCap {
		return cells
	}
	kept := append([]ContentFillExpansionCell(nil), cells[:contentFillExpansionSeriesCap]...)
	tail := cells[contentFillExpansionSeriesCap:]
	minTail := tail[0].Expansion
	for _, c := range tail[1:] {
		minTail = min(minTail, c.Expansion)
	}
	cache.NoteSeriesTruncated(contentFillExpansionFamily, uint64(len(tail)))
	return append(kept, ContentFillExpansionCell{GVR: cache.OtherGVR, Expansion: minTail})
}

// ContentFillExpansionSummary is the label-free summary of the learned
// expansions: min, max, and the mean weighted by each GVR's fill bytes
// (a plain mean when no GVR has fill bytes yet). ok is false when no GVR has a
// measured expansion.
type ContentFillExpansionSummary struct {
	Min      float64 `json:"min"`
	Max      float64 `json:"max"`
	Weighted float64 `json:"weighted"`
	GVRs     int     `json:"gvrs"`
}

// ContentFillExpansionSummarySnapshot returns the summary and whether any GVR
// has a measured expansion.
func ContentFillExpansionSummarySnapshot() (ContentFillExpansionSummary, bool) {
	expansion, _ := contentFillCalibrationSnapshot()
	if len(expansion) == 0 {
		return ContentFillExpansionSummary{}, false
	}
	bytes := contentFillBytesByGVR()
	s := ContentFillExpansionSummary{GVRs: len(expansion)}
	first := true
	var wsum, wtot, sum float64
	for gvr, e := range expansion {
		if first {
			s.Min, s.Max, first = e, e, false
		}
		s.Min, s.Max = min(s.Min, e), max(s.Max, e)
		w := float64(bytes[gvr])
		wsum += w * e
		wtot += w
		sum += e
	}
	if wtot > 0 {
		s.Weighted = wsum / wtot
	} else {
		s.Weighted = sum / float64(len(expansion))
	}
	return s, true
}

// ContentFillCalibrationSample is one {outcome} cell of the sample counter.
type ContentFillCalibrationSample struct {
	Outcome string
	Count   uint64
}

// ContentFillCalibrationSamplesSnapshot returns every outcome (zeros included).
func ContentFillCalibrationSamplesSnapshot() []ContentFillCalibrationSample {
	out := make([]ContentFillCalibrationSample, 0, calibOutcomes)
	for o := calibrationOutcome(0); o < calibOutcomes; o++ {
		out = append(out, ContentFillCalibrationSample{Outcome: o.String(), Count: contentFillCalibrationSamples[o].Load()})
	}
	return out
}

func contentFillCalibrationSamplesMap() map[string]uint64 {
	out := make(map[string]uint64, calibOutcomes)
	for _, c := range ContentFillCalibrationSamplesSnapshot() {
		out[c.Outcome] = c.Count
	}
	return out
}
