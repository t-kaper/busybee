package services

import (
	"busybee/core/api/httpx"
	"busybee/core/api/models"
	"context"
)

type graphValidationOptions struct {
	requireNonEmpty  bool
	requireStartNode bool
}

func validatePipelineGraphDefinition(nodes []models.PipelineNode, edges []models.PipelineEdge) error {
	return validatePipelineGraph(nodes, edges, graphValidationOptions{})
}

func validatePipelineGraphActivatable(nodes []models.PipelineNode, edges []models.PipelineEdge) error {
	return validatePipelineGraph(nodes, edges, graphValidationOptions{
		requireNonEmpty:  true,
		requireStartNode: true,
	})
}

func validatePipelineGraph(nodes []models.PipelineNode, edges []models.PipelineEdge, opts graphValidationOptions) error {
	if opts.requireNonEmpty && len(nodes) == 0 {
		return httpx.ValidationError("pipeline graph must contain at least one node before activation")
	}

	nodeIDs := make(map[int]struct{}, len(nodes))
	inDegree := make(map[int]int, len(nodes))
	adjacency := make(map[int][]int, len(nodes))
	for _, node := range nodes {
		nodeIDs[node.ID] = struct{}{}
		inDegree[node.ID] = 0
	}

	for _, edge := range edges {
		if edge.ParentNodeID == edge.ChildNodeID {
			return httpx.ValidationError("pipeline graph must not contain self-loop edges")
		}
		if _, ok := nodeIDs[edge.ParentNodeID]; !ok {
			return httpx.ValidationError("pipeline graph contains edge with parent node outside of pipeline")
		}
		if _, ok := nodeIDs[edge.ChildNodeID]; !ok {
			return httpx.ValidationError("pipeline graph contains edge with child node outside of pipeline")
		}

		adjacency[edge.ParentNodeID] = append(adjacency[edge.ParentNodeID], edge.ChildNodeID)
		inDegree[edge.ChildNodeID]++
	}

	queue := make([]int, 0, len(nodes))
	visited := 0
	for nodeID, degree := range inDegree {
		if degree == 0 {
			queue = append(queue, nodeID)
		}
	}

	if opts.requireStartNode && len(nodes) > 0 && len(queue) == 0 {
		return httpx.ValidationError("pipeline graph must contain at least one start node before activation")
	}

	for len(queue) > 0 {
		nodeID := queue[0]
		queue = queue[1:]
		visited++

		for _, childID := range adjacency[nodeID] {
			inDegree[childID]--
			if inDegree[childID] == 0 {
				queue = append(queue, childID)
			}
		}
	}

	if visited != len(nodes) {
		return httpx.ValidationError("pipeline graph must be acyclic")
	}

	return nil
}

func (s *Service) validatePipelineGraphActivation(ctx context.Context, pipelineID int) error {
	graph, err := s.repo.GetPipelineGraph(ctx, pipelineID)
	if err != nil {
		return err
	}

	return validatePipelineGraphActivatable(graph.Nodes, graph.Edges)
}

func (s *Service) ensurePipelineGraphMutable(ctx context.Context, pipelineID int) error {
	activeRuns, err := s.repo.CountActivePipelineRuns(ctx, pipelineID)
	if err != nil {
		return err
	}
	if activeRuns > 0 {
		return httpx.ConflictError("pipeline with active runs cannot be modified")
	}
	return nil
}

func removeNodeFromGraph(nodes []models.PipelineNode, edges []models.PipelineEdge, nodeID int) ([]models.PipelineNode, []models.PipelineEdge) {
	filteredNodes := make([]models.PipelineNode, 0, len(nodes))
	for _, node := range nodes {
		if node.ID == nodeID {
			continue
		}
		filteredNodes = append(filteredNodes, node)
	}

	filteredEdges := make([]models.PipelineEdge, 0, len(edges))
	for _, edge := range edges {
		if edge.ParentNodeID == nodeID || edge.ChildNodeID == nodeID {
			continue
		}
		filteredEdges = append(filteredEdges, edge)
	}

	return filteredNodes, filteredEdges
}

func addEdgeToGraph(edges []models.PipelineEdge, edge models.PipelineEdge) []models.PipelineEdge {
	result := make([]models.PipelineEdge, 0, len(edges)+1)
	result = append(result, edges...)
	result = append(result, edge)
	return result
}

func removeEdgeFromGraph(edges []models.PipelineEdge, edge models.PipelineEdge) ([]models.PipelineEdge, bool) {
	result := make([]models.PipelineEdge, 0, len(edges))
	removed := false
	for _, item := range edges {
		if item.ParentNodeID == edge.ParentNodeID && item.ChildNodeID == edge.ChildNodeID && !removed {
			removed = true
			continue
		}
		result = append(result, item)
	}
	return result, removed
}
