package handlers

import (
	"busybee/core/api/dto"
	"busybee/core/api/dto/requests"
	"busybee/core/api/httpx"
	"net/http"
	"strconv"
)

// HandleListPipelines возвращает список pipeline с пагинацией.
func (h *Handler) HandleListPipelines(w http.ResponseWriter, r *http.Request) {
	filters, err := parsePipelineListFilters(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	items, total, err := h.service.ListPipelines(r.Context(), filters)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": total,
	})
}

// HandleGetPipeline возвращает один pipeline по id.
func (h *Handler) HandleGetPipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	pipeline, err := h.service.GetPipeline(r.Context(), pipelineID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, pipeline)
}

// HandleCreatePipeline создает новый pipeline из JSON body.
func (h *Handler) HandleCreatePipeline(w http.ResponseWriter, r *http.Request) {
	var req requests.CreatePipelineRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	pipeline, err := h.service.CreatePipeline(r.Context(), dto.CreatePipelineInput{
		Name:            req.Name,
		Description:     req.Description,
		PipelineGroupID: req.PipelineGroupID,
		Schedule:        req.Schedule,
		IsActive:        req.IsActive,
		MaxActiveRuns:   req.MaxActiveRuns,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, pipeline)
}

// HandleUpdatePipeline применяет partial update к pipeline.
func (h *Handler) HandleUpdatePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	var req requests.UpdatePipelineRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	pipeline, err := h.service.UpdatePipeline(r.Context(), pipelineID, dto.UpdatePipelineInput{
		Name:            req.Name,
		Description:     req.Description,
		PipelineGroupID: req.PipelineGroupID,
		Schedule:        req.Schedule,
		IsActive:        req.IsActive,
		MaxActiveRuns:   req.MaxActiveRuns,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, pipeline)
}

// HandleDeletePipeline удаляет pipeline и возвращает 204 без ответа.
func (h *Handler) HandleDeletePipeline(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	if err := h.service.DeletePipeline(r.Context(), pipelineID); err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteNoContent(w)
}

// HandleGetPipelineGraph возвращает pipeline вместе с его graph.
func (h *Handler) HandleGetPipelineGraph(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	graph, err := h.service.GetPipelineGraph(r.Context(), pipelineID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, graph)
}

func parsePipelineListFilters(r *http.Request) (dto.PipelineListFilters, error) {
	query := r.URL.Query()
	filters := dto.PipelineListFilters{
		Limit:  100,
		Offset: 0,
	}

	if raw := query.Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return dto.PipelineListFilters{}, httpx.ValidationError("limit must be a positive integer")
		}
		filters.Limit = value
	}

	if raw := query.Get("offset"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			return dto.PipelineListFilters{}, httpx.ValidationError("offset must be greater than or equal to 0")
		}
		filters.Offset = value
	}

	return filters, nil
}
