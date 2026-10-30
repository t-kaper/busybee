package handlers

import (
	"busybee/core/api/dto"
	"busybee/core/api/dto/requests"
	"busybee/core/api/httpx"
	"net/http"
)

// HandleListPipelineEdges возвращает все edge одного pipeline.
func (h *Handler) HandleListPipelineEdges(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	edges, err := h.service.ListPipelineEdges(r.Context(), pipelineID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, edges)
}

// HandleCreatePipelineEdge создает новое ребро между двумя node pipeline.
func (h *Handler) HandleCreatePipelineEdge(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	var req requests.CreatePipelineEdgeRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	edge, err := h.service.CreatePipelineEdge(r.Context(), pipelineID, dto.CreatePipelineEdgeInput{
		ParentNodeID: req.ParentNodeID,
		ChildNodeID:  req.ChildNodeID,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, edge)
}

// HandleDeletePipelineEdge удаляет одно ребро pipeline и возвращает 204 без ответа.
func (h *Handler) HandleDeletePipelineEdge(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	var req requests.DeletePipelineEdgeRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	if err := h.service.DeletePipelineEdge(r.Context(), pipelineID, dto.DeletePipelineEdgeInput{
		ParentNodeID: req.ParentNodeID,
		ChildNodeID:  req.ChildNodeID,
	}); err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteNoContent(w)
}
