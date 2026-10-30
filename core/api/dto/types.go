package dto

import "busybee/core/api/models"

// DTO слой переиспользует доменные модели API как транспортные типы.
type (
	PipelineGroup            = models.PipelineGroup
	PipelineGroupDetailed    = models.PipelineGroupDetailed
	PipelineGroupListFilters = models.PipelineGroupListFilters
	Pipeline                 = models.Pipeline
	PipelineNodeRunAttempt   = models.PipelineNodeRunAttempt
	PipelineNodeRunLog       = models.PipelineNodeRunLog
	PipelineEdge             = models.PipelineEdge
	PipelineGraph            = models.PipelineGraph
	PipelineListFilters      = models.PipelineListFilters
	PipelineListItem         = models.PipelineListItem
	PipelineNodeListItem     = models.PipelineNodeListItem
	PipelineNode             = models.PipelineNode
	PipelineRun              = models.PipelineRun
	PipelineRunDetail        = models.PipelineRunDetail
	PipelineRunListFilters   = models.PipelineRunListFilters
	PipelineRunListItem      = models.PipelineRunListItem
	PipelineRunNode          = models.PipelineRunNode
	PipelineRunStats         = models.PipelineRunStats
	PipelineSummary          = models.PipelineSummary
	CreatePipelineInput      = models.CreatePipelineInput
	CreatePipelineEdgeInput  = models.CreatePipelineEdgeInput
	CreatePipelineGroupInput = models.CreatePipelineGroupInput
	CreatePipelineNodeInput  = models.CreatePipelineNodeInput
	DeletePipelineEdgeInput  = models.DeletePipelineEdgeInput
	UpdatePipelineInput      = models.UpdatePipelineInput
	UpdatePipelineGroupInput = models.UpdatePipelineGroupInput
	UpdatePipelineNodeInput  = models.UpdatePipelineNodeInput
)
