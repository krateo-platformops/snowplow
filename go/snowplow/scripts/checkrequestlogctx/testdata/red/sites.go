// Package red holds one instance of every shape the guard must flag.
package red

import (
	"context"
	"log/slog"
	"net/http"
)

type run struct {
	ctx context.Context
	log *slog.Logger
}

type bag struct{ ctx context.Context }

func paramPlainMethod(ctx context.Context, log *slog.Logger) {
	log.Warn("param, method")
}

func paramPlainPackage(ctx context.Context) {
	slog.Error("param, package function")
}

func defaultLogger(ctx context.Context) {
	slog.Default().Warn("param, the default logger")
}

func request(w http.ResponseWriter, req *http.Request) {
	slog.Warn("a request is in scope")
}

func (r *run) receiverField() {
	r.log.Error("the receiver holds a context")
}

func paramField(b *bag) {
	slog.Warn("a parameter holds a context")
}

func closure(ctx context.Context) func() {
	return func() {
		slog.Warn("captured by a closure")
	}
}

func blank(_ context.Context) {
	slog.Warn("a blank context parameter")
}

func unnamed(context.Context) {
	slog.Error("an unnamed context parameter")
}

func detachedWarn(ctx context.Context) {
	slog.WarnContext(context.Background(), "detached, with a context in scope")
}

func detachedLog(ctx context.Context, log *slog.Logger) {
	log.Log(context.TODO(), slog.LevelError, "detached Log")
}

func detachedLogComputed(ctx context.Context, log *slog.Logger, l slog.Level) {
	log.LogAttrs(context.Background(), l, "detached LogAttrs at a computed level")
}

func methodValue(ctx context.Context, log *slog.Logger) {
	warn := log.Warn
	warn("a method value")
}

func noContextNoWaiver() {
	slog.Warn("no context and no waiver")
}

func detachedNoContextNoWaiver() {
	slog.ErrorContext(context.Background(), "a literal Background is no context")
}

// log-ctx-waiver: worker: a waiver does not excuse a context in scope
func waiverAboveDecl(ctx context.Context) {
	// log-ctx-waiver: worker: a waiver does not excuse a context in scope
	slog.Warn("waived, with a context in scope")
}

func badClass() {
	// log-ctx-waiver: because: a class nobody declared
	slog.Warn("unknown class")
}

func noReason() {
	// log-ctx-waiver: worker:
	slog.Warn("no reason")
}

func staleWaiver() {
	// log-ctx-waiver: boot: the site this excused was removed
	_ = 1
}

func logsWith(ctx context.Context) {
	slog.WarnContext(ctx, "reaches a record")
}

func forwards(ctx context.Context) {
	logsWith(ctx)
}

func literalToALogger(ctx context.Context) {
	logsWith(context.Background())
}

func literalThroughAForward(req *http.Request) {
	forwards(context.TODO())
}

func wrapper() {
	forwards(context.Background())
}

func wrapperOfAWrapper() {
	wrapper()
}

func callsAWrapper(ctx context.Context) {
	wrapper()
}

func callsTheOuterWrapper(req *http.Request) {
	wrapperOfAWrapper()
}

func (r *run) callsAWrapperFromAField() {
	wrapper()
}

var wrapperValue = wrapper

func takesAFunc(f func()) { f() }

func passesAWrapperValue() {
	takesAFunc(wrapper)
}

func detachedOnAPlainSite() {
	// log-ctx-waiver: detached: this class cannot excuse a site with no context
	slog.Warn("no context")
}
