package handlers

import (
	"busybee/core/api/dto"
	"busybee/core/api/httpx"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// HandleListPipelineRuns возвращает список горячих pipeline_run с фильтрами.
func (h *Handler) HandleListPipelineRuns(w http.ResponseWriter, r *http.Request) {
	filters, err := parsePipelineRunListFilters(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	items, total, err := h.service.ListPipelineRuns(r.Context(), filters)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": total,
	})
}

// HandleGetPipelineRun возвращает полный runtime-снимок одного pipeline_run.
func (h *Handler) HandleGetPipelineRun(w http.ResponseWriter, r *http.Request) {
	runID := strings.TrimSpace(r.PathValue("runId"))
	if runID == "" {
		httpx.WriteError(w, httpx.ValidationError("runId is required"))
		return
	}

	run, err := h.service.GetPipelineRun(r.Context(), runID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, run)
}

// HandleGetPipelineNodeRunLog возвращает лог node_run как text/plain.
func (h *Handler) HandleGetPipelineNodeRunLog(w http.ResponseWriter, r *http.Request) {
	nodeRunID, err := httpx.PathInt(r, "nodeRunId")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	log, err := h.service.GetPipelineNodeRunLog(r.Context(), nodeRunID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(log.Content))
}

func parsePipelineRunListFilters(r *http.Request) (dto.PipelineRunListFilters, error) {
	query := r.URL.Query()
	filters := dto.PipelineRunListFilters{}

	if raw := strings.TrimSpace(query.Get("pipeline_id")); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return dto.PipelineRunListFilters{}, httpx.ValidationError("pipeline_id must be a positive integer")
		}
		filters.PipelineID = &value
	}

	if raw := strings.TrimSpace(query.Get("status")); raw != "" {
		filters.Status = &raw
	}

	if raw := strings.TrimSpace(query.Get("trigger_type")); raw != "" {
		filters.TriggerType = &raw
	}

	if raw := strings.TrimSpace(query.Get("created_from")); raw != "" {
		value, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return dto.PipelineRunListFilters{}, httpx.ValidationError("created_from must be a valid RFC3339 timestamp")
		}
		filters.CreatedFrom = &value
	}

	if raw := strings.TrimSpace(query.Get("created_to")); raw != "" {
		value, err := time.Parse(time.RFC3339, raw)
		if err != nil {
			return dto.PipelineRunListFilters{}, httpx.ValidationError("created_to must be a valid RFC3339 timestamp")
		}
		filters.CreatedTo = &value
	}

	return filters, nil
}
