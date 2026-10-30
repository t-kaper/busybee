package services

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
	"errors"
	"os"
	"sort"
	"strings"
	"time"
)

var allowedPipelineRunStatuses = map[string]struct{}{
	"active":    {},
	"completed": {},
	"failed":    {},
	"stopped":   {},
}

// ListPipelineRuns валидирует фильтры списка runtime run и делегирует чтение репозиторию.
func (s *Service) ListPipelineRuns(ctx context.Context, filters models.PipelineRunListFilters) ([]models.PipelineRunListItem, int, error) {
	if filters.PipelineID != nil {
		if *filters.PipelineID <= 0 {
			return nil, 0, httpx.ValidationError("pipeline_id must be greater than 0")
		}
		if _, err := s.repo.GetPipeline(ctx, *filters.PipelineID); err != nil {
			return nil, 0, err
		}
	}
	if filters.Status != nil {
		status := strings.TrimSpace(*filters.Status)
		if status == "" {
			filters.Status = nil
		} else {
			if _, ok := allowedPipelineRunStatuses[status]; !ok {
				return nil, 0, httpx.ValidationError("status must be one of: active, completed, failed, stopped")
			}
			filters.Status = &status
		}
	}
	if filters.TriggerType != nil {
		triggerType := strings.TrimSpace(*filters.TriggerType)
		if triggerType == "" {
			filters.TriggerType = nil
		} else {
			filters.TriggerType = &triggerType
		}
	}
	if filters.CreatedFrom != nil && filters.CreatedTo != nil && filters.CreatedFrom.After(*filters.CreatedTo) {
		return nil, 0, httpx.ValidationError("created_from must be less than or equal to created_to")
	}

	return s.repo.ListPipelineRuns(ctx, filters)
}

// GetPipelineRun возвращает полный runtime-снимок выбранного pipeline_run.
func (s *Service) GetPipelineRun(ctx context.Context, runID string) (models.PipelineRunDetail, error) {
	runID = strings.TrimSpace(runID)
	if runID == "" {
		return models.PipelineRunDetail{}, httpx.ValidationError("run id is required")
	}

	run, err := s.repo.GetPipelineRun(ctx, runID)
	if err != nil {
		return models.PipelineRunDetail{}, err
	}

	graph, err := s.repo.GetPipelineGraph(ctx, run.PipelineID)
	if err != nil {
		return models.PipelineRunDetail{}, err
	}

	attempts, err := s.repo.ListPipelineNodeRunAttempts(ctx, runID)
	if err != nil {
		return models.PipelineRunDetail{}, err
	}

	orderedNodes, err := topologicallySortPipelineNodes(graph.Nodes, graph.Edges)
	if err != nil {
		return models.PipelineRunDetail{}, err
	}

	attemptsByNode := make(map[int][]models.PipelineNodeRunAttempt, len(orderedNodes))
	for _, attempt := range attempts {
		attemptsByNode[attempt.PipelineNodeID] = append(attemptsByNode[attempt.PipelineNodeID], attempt)
	}

	nodes := make([]models.PipelineRunNode, 0, len(orderedNodes))
	for _, node := range orderedNodes {
		nodeAttempts := attemptsByNode[node.ID]
		if nodeAttempts == nil {
			nodeAttempts = []models.PipelineNodeRunAttempt{}
		}
		nodes = append(nodes, models.PipelineRunNode{
			PipelineNodeID:  node.ID,
			NodeName:        node.Name,
			NodeDescription: node.Description,
			Command:         node.Command,
			ExecutorType:    node.ExecutorType,
			MaxRetries:      node.MaxRetries,
			TimeoutSeconds:  node.TimeoutSeconds,
			Meta:            node.Meta,
			Status:          aggregatePipelineRunNodeStatus(nodeAttempts),
			Attempts:        nodeAttempts,
		})
	}

	return models.PipelineRunDetail{
		Run:        run,
		Stats:      run.Stats,
		Nodes:      nodes,
		SnapshotAt: time.Now().UTC(),
	}, nil
}

// GetPipelineNodeRunLog читает лог конкретного горячего node_run.
func (s *Service) GetPipelineNodeRunLog(ctx context.Context, nodeRunID int) (models.PipelineNodeRunLog, error) {
	if nodeRunID <= 0 {
		return models.PipelineNodeRunLog{}, httpx.ValidationError("node run id must be greater than 0")
	}

	logPath, err := s.repo.GetPipelineNodeRunLogPath(ctx, nodeRunID)
	if err != nil {
		return models.PipelineNodeRunLog{}, err
	}

	content, err := os.ReadFile(logPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return models.PipelineNodeRunLog{}, httpx.NotFoundError("pipeline node run log file not found")
		}
		return models.PipelineNodeRunLog{}, err
	}

	return models.PipelineNodeRunLog{
		NodeRunID: nodeRunID,
		LogPath:   logPath,
		Content:   string(content),
	}, nil
}

func aggregatePipelineRunNodeStatus(attempts []models.PipelineNodeRunAttempt) string {
	if len(attempts) == 0 {
		return "not_started"
	}

	return attempts[len(attempts)-1].Status
}

func topologicallySortPipelineNodes(nodes []models.PipelineNode, edges []models.PipelineEdge) ([]models.PipelineNode, error) {
	nodeByID := make(map[int]models.PipelineNode, len(nodes))
	inDegree := make(map[int]int, len(nodes))
	adjacency := make(map[int][]int, len(nodes))

	for _, node := range nodes {
		nodeByID[node.ID] = node
		inDegree[node.ID] = 0
	}

	for _, edge := range edges {
		if _, ok := nodeByID[edge.ParentNodeID]; !ok {
			return nil, httpx.ValidationError("pipeline graph contains edge with parent node outside of pipeline")
		}
		if _, ok := nodeByID[edge.ChildNodeID]; !ok {
			return nil, httpx.ValidationError("pipeline graph contains edge with child node outside of pipeline")
		}
		adjacency[edge.ParentNodeID] = append(adjacency[edge.ParentNodeID], edge.ChildNodeID)
		inDegree[edge.ChildNodeID]++
	}

	queue := make([]int, 0, len(nodes))
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}
	sort.Ints(queue)

	ordered := make([]models.PipelineNode, 0, len(nodes))
	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		ordered = append(ordered, nodeByID[nodeID])

		children := adjacency[nodeID]
		sort.Ints(children)
		for _, childID := range children {
			inDegree[childID]--
			if inDegree[childID] == 0 {
				queue = append(queue, childID)
				sort.Ints(queue)
			}
		}
	}

	if len(ordered) != len(nodes) {
		return nil, httpx.ValidationError("pipeline graph must be acyclic")
	}

	return ordered, nil
}
