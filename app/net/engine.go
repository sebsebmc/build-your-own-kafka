package net

import "github.com/google/uuid"

type Engine struct {
}

func (e *Engine) HandleFetchV16(fr FetchRequestV16) *FetchResponseV16Body {
	if len(fr.Topics) == 0 {
		return &FetchResponseV16Body{ThrottleTimeMs: 0, ErrorCode: 0, Responses: make([]TopicResponse, 0)}
	}
	resp := FetchResponseV16Body{ThrottleTimeMs: 0, ErrorCode: 0}
	topicResps := make([]TopicResponse, len(fr.Topics))
	for idx, v := range fr.Topics {
		if e.hasTopic(v.TopicId) {
		} else {
			topicResps[idx] = TopicResponse{
				TopicId: v.TopicId,
				Partitions: []FetchResponseV16Partition{
					{
						ErrorCode:      UNKNOWN_TOPIC_ID,
						PartitionIndex: 0,
					},
				},
			}
		}
	}
	resp.Responses = topicResps
	return &resp
}

func (e *Engine) hasTopic(id uuid.UUID) bool {
	return false
}
