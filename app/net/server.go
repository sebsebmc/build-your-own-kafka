package net

import (
	"encoding/binary"
	"fmt"
	"log/slog"
	"maps"
	"net"
	"os"
	"slices"
	"time"

	"github.com/codecrafters-io/kafka-starter-go/app/disk"
	"github.com/google/uuid"
)

type Server struct {
	dm *disk.DiskManager
}

func NewServer(dm *disk.DiskManager) *Server {
	s := new(Server)
	s.dm = dm
	return s
}

func (s *Server) ListenAndServer() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	enc := Encoder{}
	for {
		conn.SetReadDeadline(time.Now().Add(10 * time.Second))
		msg_size := make([]byte, 4)
		n, err := conn.Read(msg_size)
		if err != nil {
			fmt.Printf("failed to read after %d bytes: %v\n", n, err)
			return
		}
		requestBytes := make([]byte, binary.BigEndian.Uint32(msg_size))
		conn.Read(requestBytes)
		r := new(RequestHeaderV2)
		// r.UnmarshalBinary(requestBytes)

		bytesRead, err := enc.Decode(requestBytes, r)
		if err != nil {
			fmt.Println(err)
			continue
		}
		// fmt.Printf("Requested API %d with version %d\n", r.request_api_key, r.request_api_version)
		resp := Message{MessageSize: 0, Header: &ResponseHeaderV0{r.CorrelationId}}

		support_info, ok := SUPPORTED_APIS[r.RequestApiKey]
		if !ok || (r.RequestApiVersion > support_info.MaxVersion || r.RequestApiVersion < support_info.MinVersion) {
			var rbody ApiVersionsV4ResponseBody

			rbody.ErrorCode = UNSUPPORTED_VERSION
			resp.Body = &rbody
			// respBytes, _ := resp.MarshalBinary()
			encBytes, err := enc.Encode(resp)
			if err != nil {
				return
			}
			conn.Write(encBytes)
			// continue
			return
		}

		switch r.RequestApiKey {
		case API_KEY_APIVERSIONS:
			var rbody ApiVersionsV4ResponseBody
			rbody.ApiKeys = slices.Collect(maps.Values(SUPPORTED_APIS))
			// fmt.Println("Sending APIVersions response")
			resp.Body = &rbody
		case API_KEY_DESCRIBETOPICPARTIONS:
			reqBody := new(DescribeTopicPartitionsRequestV0)
			enc.Decode(requestBytes[bytesRead:], reqBody)
			resp.Header = &ResponseHeaderV1{CorrelationId: r.CorrelationId}
			rbody := new(DescribeTopicPartitionsResponseV0)

			slog.Debug("requested topic", "name", reqBody.Topics[0].Name)
			topic, err := s.dm.GetTopicPartitions(reqBody.Topics[0].Name)
			rbody.NextCursor = -1
			if err != nil {
				rbody.Topics = make([]DescribeTopics, 1)
				dt := DescribeTopics{
					ErrorCode:  UNKNOWN_TOPIC_OR_PARTITION,
					TopicName:  reqBody.Topics[0].Name,
					TopicId:    uuid.Nil,
					IsInternal: false,
					Partitions: []DescribePartitions{},
				}
				rbody.Topics[0] = dt
			} else {
				rbody.Topics = make([]DescribeTopics, 1)
				dt := DescribeTopics{
					TopicName:  reqBody.Topics[0].Name,
					TopicId:    topic.Id,
					Partitions: []DescribePartitions{{PartitionIndex: topic.Partitions[0].PartitionId, ReplicaNodes: topic.Partitions[0].Replicas, ISRNodes: topic.Partitions[0].Isr}},
				}
				rbody.Topics[0] = dt
			}
			resp.Body = rbody

		case API_KEY_FETCH:
			reqBody := new(FetchRequestV16)
			enc.Decode(requestBytes[bytesRead:], reqBody)
			slog.Debug("Topics", "length", len(reqBody.Topics))

			resp.Header = &ResponseHeaderV1{CorrelationId: r.CorrelationId}

			e := Engine{}

			resp.Body = e.HandleFetchV16(*reqBody)
		}
		encBytes, err := enc.Encode(resp)
		if err != nil {
			slog.Error("Unable to encode response", "error", err)
			return
		}

		// fmt.Println(hex.Dump(encBytes))

		conn.Write(encBytes)
	}
}
