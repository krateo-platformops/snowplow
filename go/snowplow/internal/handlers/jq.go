package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	xcontext "github.com/krateo-platformops/plumbing/context"
	"github.com/krateo-platformops/plumbing/http/response"
	"github.com/krateo-platformops/plumbing/jqutil"
	jqsupport "github.com/krateo-platformops/snowplow/internal/support/jq"
)

const (
	MaxBodySize = 1 * 1024 * 1024 // 1MB
)

// @Summary     Evaluate a JQ query against JSON input
// @Description This endpoint accepts a JSON body containing a JQ `query` and some `data`.
// @Description It evaluates the query against the data and returns the result as formatted JSON.
// @Tags        jq
// @Accept      json
// @Produce     json
// @Param       body  body   jqin  true  "Input payload containing JQ query and JSON data"
// @Success     200   {object}  any    "Successfully evaluated JQ query"
// @Failure 400 {object} response.Status
// @Failure 401 {object} response.Status
// @Failure 404 {object} response.Status
// @Failure 500 {object} response.Status
// @Router      /jq [post]
func JQ() http.HandlerFunc {
	return func(wri http.ResponseWriter, req *http.Request) {

		if req.Method != http.MethodPost {
			response.MethodNotAllowed(wri, errors.New("only POST method is allowed"))
			return
		}

		log := xcontext.Logger(req.Context())

		_, err := xcontext.UserConfig(req.Context())
		if err != nil {
			log.ErrorContext(req.Context(), "unable to get user endpoint", slog.Any("err", err))
			response.Unauthorized(wri, err)
			return
		}

		contentType := req.Header.Get("Content-Type")
		if !strings.Contains(contentType, "application/json") {
			err := fmt.Errorf("unsupported content type '%s' use 'application/json'", contentType)
			log.ErrorContext(req.Context(), err.Error())
			response.NotAcceptable(wri, err)
			return
		}

		in := &jqin{}
		dec := json.NewDecoder(io.LimitReader(req.Body, MaxBodySize))
		err = dec.Decode(in)
		if err != nil {
			log.ErrorContext(req.Context(), "unable to decode JSON body", slog.Any("err", err))
			response.InternalError(wri, err)
			return
		}

		res, err := jqutil.Eval(req.Context(), jqutil.EvalOptions{
			Query:        in.Query,
			Data:         in.Data,
			ModuleLoader: jqsupport.ModuleLoader(),
		})
		if err != nil {
			log.ErrorContext(req.Context(), "unable to evaluate JQ query", slog.Any("err", err))
			response.InternalError(wri, err)
			return
		}

		val := jqutil.InferType(res)

		wri.Header().Set("Content-Type", "application/json")
		wri.WriteHeader(http.StatusOK)
		enc := json.NewEncoder(wri)
		enc.SetIndent("", "  ")
		enc.Encode(&val)
	}
}

type jqin struct {
	Query string `json:"query"`
	Data  any    `json:"data"`
}
