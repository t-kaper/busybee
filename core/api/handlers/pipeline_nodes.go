package handlers

import (
	"busybee/core/api/dto"
	"busybee/core/api/dto/requests"
	"busybee/core/api/httpx"
	"net/http"
)

// HandleListPipelineNodes возвращает все node одного pipeline.
func (h *Handler) HandleListPipelineNodes(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	nodes, err := h.service.ListPipelineNodes(r.Context(), pipelineID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, nodes)
}

// HandleGetPipelineNode возвращает один node pipeline по id.
func (h *Handler) HandleGetPipelineNode(w http.ResponseWriter, r *http.Request) {
	pipelineID, nodeID, err := parsePipelineNodePath(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	node, err := h.service.GetPipelineNode(r.Context(), pipelineID, nodeID)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, node)
}

// HandleCreatePipelineNode создает новый node внутри pipeline.
func (h *Handler) HandleCreatePipelineNode(w http.ResponseWriter, r *http.Request) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	var req requests.CreatePipelineNodeRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	node, err := h.service.CreatePipelineNode(r.Context(), pipelineID, dto.CreatePipelineNodeInput{
		Name:           req.Name,
		Description:    req.Description,
		Command:        req.Command,
		ExecutorType:   req.ExecutorType,
		MaxRetries:     req.MaxRetries,
		TimeoutSeconds: req.TimeoutSeconds,
		Meta:           req.Meta,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusCreated, node)
}

// HandleUpdatePipelineNode применяет partial update к node pipeline.
func (h *Handler) HandleUpdatePipelineNode(w http.ResponseWriter, r *http.Request) {
	pipelineID, nodeID, err := parsePipelineNodePath(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	var req requests.UpdatePipelineNodeRequest
	if err := httpx.DecodeJSON(r, &req); err != nil {
		httpx.WriteError(w, err)
		return
	}

	node, err := h.service.UpdatePipelineNode(r.Context(), pipelineID, nodeID, dto.UpdatePipelineNodeInput{
		Name:           req.Name,
		Description:    req.Description,
		Command:        req.Command,
		ExecutorType:   req.ExecutorType,
		MaxRetries:     req.MaxRetries,
		TimeoutSeconds: req.TimeoutSeconds,
		Meta:           req.Meta,
	})
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteJSON(w, http.StatusOK, node)
}

// HandleDeletePipelineNode удаляет node pipeline и возвращает 204 без ответа.
func (h *Handler) HandleDeletePipelineNode(w http.ResponseWriter, r *http.Request) {
	pipelineID, nodeID, err := parsePipelineNodePath(r)
	if err != nil {
		httpx.WriteError(w, err)
		return
	}

	if err := h.service.DeletePipelineNode(r.Context(), pipelineID, nodeID); err != nil {
		httpx.WriteError(w, err)
		return
	}

	httpx.WriteNoContent(w)
}

func parsePipelineNodePath(r *http.Request) (int, int, error) {
	pipelineID, err := httpx.PathInt(r, "id")
	if err != nil {
		return 0, 0, err
	}
	nodeID, err := httpx.PathInt(r, "nodeId")
	if err != nil {
		return 0, 0, err
	}
	return pipelineID, nodeID, nil
}
