package net

import (
	"log/slog"
	"slices"

	"github.com/sebsebmc/build-your-own-kafka/app/disk"

	"github.com/google/uuid"
)

type Engine struct {
	diskManager *disk.DiskManager
}

func (e *Engine) HandleProduceV11(reqBody *ProduceRequestV11) *ProduceResponseV11 {
	rbody := new(ProduceResponseV11)
	rbody.Responses = make([]ProduceResponse, len(reqBody.TopicData))
	for idx, t := range reqBody.TopicData {
		rbody.Responses[idx] = ProduceResponse{
			Name:               t.Name,
			PartitionResponses: make([]ProducePartitionResponse, 0),
		}
		for _, p := range t.PartitionData {
			slog.Debug("Handle Producev11", "topic", t.Name, "partitions", len(t.PartitionData))
			dt, err := e.diskManager.GetTopic(t.Name)
			if err != nil || !dt.HasPartition(p.Index) {
				rbody.Responses[idx].PartitionResponses = append(rbody.Responses[idx].PartitionResponses,
					ProducePartitionResponse{
						ErrorCode:       UNKNOWN_TOPIC_OR_PARTITION,
						Index:           p.Index,
						BaseOffset:      -1,
						LogAppendTimeMs: -1,
						LogStartOffset:  -1,
					})
			} else {
				err = e.diskManager.WriteRecord(dt, p.Index, p.Records)
				if err != nil {
					slog.Error("failed to write record", "error", err)
				}
				rbody.Responses[idx].PartitionResponses = append(rbody.Responses[idx].PartitionResponses,
					ProducePartitionResponse{

						ErrorCode:       0,
						Index:           p.Index, // TODO
						BaseOffset:      0,
						LogAppendTimeMs: -1,
						LogStartOffset:  0,
					})
			}
		}
	}
	return rbody
}

func NewEngine(dm *disk.DiskManager) *Engine {
	return &Engine{diskManager: dm}
}

func (e *Engine) HandleFetchV16(fr *FetchRequestV16) *FetchResponseV16Body {
	if len(fr.Topics) == 0 {
		return &FetchResponseV16Body{ThrottleTimeMs: 0, ErrorCode: 0, Responses: make([]TopicResponse, 0)}
	}
	resp := FetchResponseV16Body{ThrottleTimeMs: 0, ErrorCode: 0}
	topicResps := make([]TopicResponse, len(fr.Topics))
	for idx, v := range fr.Topics {
		topic, err := e.diskManager.GetTopicPartitions(v.TopicId)
		if err != nil {
			topicResps[idx] = TopicResponse{
				TopicId: v.TopicId,
				Partitions: []FetchResponseV16Partition{
					{
						ErrorCode:      UNKNOWN_TOPIC_ID,
						PartitionIndex: 0,
					},
				},
			}
		} else {
			for idx, p := range topic.Partitions {
				rb, _ := e.diskManager.LoadRecords(p, int32(idx))

				topicResps[idx] = TopicResponse{
					TopicId: v.TopicId,
					Partitions: []FetchResponseV16Partition{
						{
							PartitionIndex: 0,
							ErrorCode:      0,
							Records:        Record{Message: rb},
						},
					},
				}
			}
		}
	}
	resp.Responses = topicResps
	return &resp
}

func (e *Engine) hasTopic(id uuid.UUID) bool {
	return false
}

func (e *Engine) HandleDescribeTopicV0(dtr *DescribeTopicPartitionsRequestV0) []DescribeTopics {
	topics := make([]DescribeTopics, len(dtr.Topics))
	slices.SortFunc(dtr.Topics, func(a, b DescribeTopicNames) int {
		if a.Name < b.Name {
			return -1
		}
		if a.Name == b.Name {
			return 0
		}
		return 1
	})
	for idx, t := range dtr.Topics {
		topic, err := e.diskManager.GetTopic(t.Name)
		if err != nil {
			dt := DescribeTopics{
				ErrorCode:  UNKNOWN_TOPIC_OR_PARTITION,
				TopicName:  t.Name,
				TopicId:    uuid.Nil,
				IsInternal: false,
				Partitions: []DescribePartitions{},
			}
			topics[idx] = dt
		} else {
			partitions := make([]DescribePartitions, len(topic.Partitions))
			for pidx, p := range topic.Partitions {
				partitions[pidx] = DescribePartitions{
					PartitionIndex: p.PartitionId,
					ReplicaNodes:   p.Replicas,
					ISRNodes:       p.Isr,
				}
			}
			dt := DescribeTopics{
				TopicName:  t.Name,
				TopicId:    topic.Id,
				Partitions: partitions,
			}
			topics[idx] = dt
		}
	}
	return topics
}
