// Package api реализует HTTP API поверх движка BusyBee.
package api

import (
	"busybee/core/api/handlers"
	"busybee/core/api/httpx"
	"busybee/core/api/services"
	"net/http"

	"github.com/jackc/pgx/v5/pgxpool"
)

// API связывает service-слой с HTTP-маршрутизацией.
type API struct {
	pool    *pgxpool.Pool
	service *services.Service
}

// New создает корневой объект HTTP API поверх общего pgx pool.
func New(pool *pgxpool.Pool) *API {
	return &API{
		pool:    pool,
		service: services.New(pool),
	}
}

// Handler регистрирует минимальный набор маршрутов API.
func (a *API) Handler() http.Handler {
	mux := http.NewServeMux()
	h := handlers.New(newHandlerServiceAdapter(a.service))

	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("GET /ready", func(w http.ResponseWriter, r *http.Request) {
		if err := a.pool.Ping(r.Context()); err != nil {
			httpx.WriteJSON(w, http.StatusServiceUnavailable, map[string]string{"status": "unavailable"})
			return
		}
		httpx.WriteJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})

	mux.HandleFunc("GET /api/pipeline-groups/detailed", h.HandleListPipelineGroupsDetailed)
	mux.HandleFunc("GET /api/pipeline-groups", h.HandleListPipelineGroups)
	mux.HandleFunc("POST /api/pipeline-groups", h.HandleCreatePipelineGroup)
	mux.HandleFunc("PATCH /api/pipeline-groups/{id}", h.HandleUpdatePipelineGroup)
	mux.HandleFunc("DELETE /api/pipeline-groups/{id}", h.HandleDeletePipelineGroup)
	mux.HandleFunc("GET /api/pipeline-runs", h.HandleListPipelineRuns)
	mux.HandleFunc("GET /api/pipeline-runs/{runId}", h.HandleGetPipelineRun)
	mux.HandleFunc("GET /api/pipeline-node-runs/{nodeRunId}/log", h.HandleGetPipelineNodeRunLog)
	mux.HandleFunc("GET /api/pipelines", h.HandleListPipelines)
	mux.HandleFunc("GET /api/pipelines/{id}", h.HandleGetPipeline)
	mux.HandleFunc("GET /api/pipelines/{id}/graph", h.HandleGetPipelineGraph)
	mux.HandleFunc("GET /api/pipelines/{id}/nodes", h.HandleListPipelineNodes)
	mux.HandleFunc("GET /api/pipelines/{id}/nodes/{nodeId}", h.HandleGetPipelineNode)
	mux.HandleFunc("POST /api/pipelines/{id}/nodes", h.HandleCreatePipelineNode)
	mux.HandleFunc("PATCH /api/pipelines/{id}/nodes/{nodeId}", h.HandleUpdatePipelineNode)
	mux.HandleFunc("DELETE /api/pipelines/{id}/nodes/{nodeId}", h.HandleDeletePipelineNode)
	mux.HandleFunc("GET /api/pipelines/{id}/edges", h.HandleListPipelineEdges)
	mux.HandleFunc("POST /api/pipelines/{id}/edges", h.HandleCreatePipelineEdge)
	mux.HandleFunc("DELETE /api/pipelines/{id}/edges", h.HandleDeletePipelineEdge)
	mux.HandleFunc("POST /api/pipelines", h.HandleCreatePipeline)
	mux.HandleFunc("PATCH /api/pipelines/{id}", h.HandleUpdatePipeline)
	mux.HandleFunc("DELETE /api/pipelines/{id}", h.HandleDeletePipeline)

	return mux
}
