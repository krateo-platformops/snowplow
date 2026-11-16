// Package green holds the shapes the guard must accept, and the look-alikes it
// must not count.
package green

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
)

type run struct {
	ctx context.Context
	log *slog.Logger
}

type fakeLogger struct{}

func (fakeLogger) Warn(string)   {}
func (fakeLogger) Error() string { return "" }

func passes(ctx context.Context, log *slog.Logger) {
	log.WarnContext(ctx, "method")
	slog.ErrorContext(ctx, "package function")
	slog.Default().WarnContext(ctx, "default logger")
	log.Log(ctx, slog.LevelWarn, "Log")
	log.LogAttrs(ctx, slog.LevelError, "LogAttrs")
}

func request(w http.ResponseWriter, req *http.Request) {
	slog.WarnContext(req.Context(), "from the request")
	http.Error(w, "http.Error is no log", http.StatusBadRequest)
	_ = errors.New("x").Error()
}

func (r *run) receiver() {
	r.log.ErrorContext(r.ctx, "from the receiver's field")
}

func notSlog(ctx context.Context) {
	var l fakeLogger
	l.Warn("not slog")
	_ = l.Error()
}

func belowWarn(ctx context.Context, log *slog.Logger) {
	log.Log(context.Background(), slog.LevelInfo, "below WARN")
	log.LogAttrs(context.TODO(), slog.LevelDebug, "below WARN")
	log.Info("plain INFO is out of the rule")
	log.Debug("plain DEBUG is out of the rule")
}

func waived() {
	// log-ctx-waiver: boot: the fixture's process start
	slog.Warn("no context to pass")
	// log-ctx-waiver: worker: the waiver may sit on the line above
	slog.ErrorContext(context.Background(), "a detached context is waived the same way")
	slog.Error("or on the line itself") // log-ctx-waiver: informer-event: same line
}

func logsWith(ctx context.Context) {
	slog.WarnContext(ctx, "reaches a record")
}

func discards(context.Context) {}

// No record is reachable from discards, so a literal is not this guard's business.
func literalToANonLogger(ctx context.Context) {
	discards(context.Background())
}

func deliberateCut(ctx context.Context) {
	// log-ctx-waiver: detached: the fixture's deliberate cut
	logsWith(context.Background())
}

// A waived cut in a function with no context means no caller needs one.
func waivedCut() {
	// log-ctx-waiver: detached: no caller of this needs a context
	logsWith(context.Background())
}

func callsAWaivedCut(ctx context.Context) {
	waivedCut()
}

// A named detached context is the author's statement.
func namedDetach(ctx context.Context) {
	bg := context.Background()
	logsWith(bg)
}

// With no context anywhere up the chain, a wrapper has nothing to drop.
func wrapper() {
	logsWithWaiverlessWrapper()
}

func logsWithWaiverlessWrapper() {
	// log-ctx-waiver: detached: the fixture's root
	logsWith(context.TODO())
}
