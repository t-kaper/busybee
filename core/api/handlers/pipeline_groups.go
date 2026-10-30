package handlers

import (
	"busybee/core/api/dto"
	"busybee/core/api/dto/requests"
	"busybee/core/api/httpx"
	"net/http"
	"strconv"
)

// HandleListPipelineGroups возвращает список групп pipeline с пагинацией.
func (h *Handler) HandleListPipelineGroups(w http.ResponseWriter, r *http.Request) {
	filters, err := parsePipelineGroupListFilters(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	items, total, err := h.service.ListPipelineGroups(r.Context(), filters)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": total,
	})
}

// HandleListPipelineGroupsDetailed возвращает группы вместе со связанными pipeline.
func (h *Handler) HandleListPipelineGroupsDetailed(w http.ResponseWriter, r *http.Request) {
	filters, err := parsePipelineGroupListFilters(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	items, total, err := h.service.ListPipelineGroupsDetailed(r.Context(), filters)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, map[string]any{
		"items": items,
		"total": total,
	})
}

// HandleCreatePipelineGroup создает новую группу pipeline из JSON body.
func (h *Handler) HandleCreatePipelineGroup(w http.ResponseWriter, r *http.Request) {
	var req requests.CreatePipelineGroupRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	group, err := h.service.CreatePipelineGroup(r.Context(), dto.CreatePipelineGroupInput{
		Name:        req.Name,
		Description: req.Description,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, group)
}

// HandleUpdatePipelineGroup применяет partial update к группе pipeline.
func (h *Handler) HandleUpdatePipelineGroup(w http.ResponseWriter, r *http.Request) {
	groupID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	var req requests.UpdatePipelineGroupRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	group, err := h.service.UpdatePipelineGroup(r.Context(), groupID, dto.UpdatePipelineGroupInput{
		Name:        req.Name,
		Description: req.Description,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, group)
}

// HandleDeletePipelineGroup удаляет группу и возвращает 204 без ответа.
func (h *Handler) HandleDeletePipelineGroup(w http.ResponseWriter, r *http.Request) {
	groupID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	if err := h.service.DeletePipelineGroup(r.Context(), groupID); err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteNoContent(w)
}

func parsePipelineGroupListFilters(r *http.Request) (dto.PipelineGroupListFilters, error) {
	query := r.URL.Query()
	filters := dto.PipelineGroupListFilters{
		Limit:  100,
		Offset: 0,
	}

	if raw := query.Get("limit"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value <= 0 {
			return dto.PipelineGroupListFilters{}, httpx.ValidationError("limit must be a positive integer")
		}
		filters.Limit = value
	}

	if raw := query.Get("offset"); raw != "" {
		value, err := strconv.Atoi(raw)
		if err != nil || value < 0 {
			return dto.PipelineGroupListFilters{}, httpx.ValidationError("offset must be greater than or equal to 0")
		}
		filters.Offset = value
	}

	return filters, nil
}
