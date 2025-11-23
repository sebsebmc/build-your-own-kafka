package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"maps"
	"net"
	"os"
	"slices"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

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

		go handleConnection(conn)
	}

}

func handleConnection(conn net.Conn) {
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
			var rbody ResponseBody
			rbody.Body = make([]byte, 0)
			rbody.Body = binary.BigEndian.AppendUint16(rbody.Body, UNSUPPORTED_VERSION)
			resp.Body = &rbody
			// respBytes, _ := resp.MarshalBinary()
			encBytes, err := enc.Encode(resp)
			if err != nil {
				return
			}
			// fmt.Println(hex.Dump(encBytes))
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
		case API_KEY_FETCH:
			reqBody := new(FetchRequestV16)
			enc.Decode(requestBytes[bytesRead:], reqBody)
			fmt.Printf("Topics: %d\n", len(reqBody.Topics))

			resp.Header = &ResponseHeaderV1{CorrelationId: r.CorrelationId}

			var rbody FetchResponseV16Body
			rbody.Responses = make([]TopicResponses, 1)
			rbody.Responses[0].Partitions = make([]FetchResponseV16Partition, 1)
			rbody.Responses[0].Partitions[0].PartitionIndex = 0
			rbody.Responses[0].Partitions[0].ErrorCode = UNKNOWN_TOPIC_ID
			rbody.Responses[0].TopicId = reqBody.Topics[0].TopicId

			resp.Body = &rbody
		}
		encBytes, err := enc.Encode(resp)
		if err != nil {
			return
		}
		fmt.Println(hex.Dump(encBytes))
		respBytes, _ := resp.MarshalBinary()
		conn.Write(respBytes)
	}
}
