package main

import (
	"encoding/binary"
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

	for {
		conn.SetReadDeadline(time.Now().Add(1 * time.Second))
		msg_size := make([]byte, 4)
		conn.Read(msg_size)
		requestBytes := append(make([]byte, binary.BigEndian.Uint32(msg_size)+4), msg_size...)
		conn.Read(requestBytes)
		r := new(Request)
		r.UnmarshalBinary(requestBytes)
		// fmt.Printf("Requested API %d with version %d\n", r.request_api_key, r.request_api_version)
		resp := Message{message_size: 0, header: &ResponseHeader{r.correlation_id}}

		support_info, ok := SUPPORTED_APIS[r.request_api_key]
		if !ok || (r.request_api_version > support_info.max_version || r.request_api_version < support_info.min_version) {
			var rbody ResponseBody
			rbody.body = make([]byte, 0)
			rbody.body = binary.BigEndian.AppendUint16(rbody.body, UNSUPPORTED_VERSION)
			respBytes, _ := resp.MarshalBinary()
			conn.Write(respBytes)
			continue
		}

		switch r.request_api_key {
		case API_KEY_APIVERSIONS:
			var rbody ApiVersionsV4ResponseBody
			rbody.api_keys = slices.Collect(maps.Values(SUPPORTED_APIS))
			// fmt.Println("Sending APIVersions response")
			resp.body = &rbody
		case API_KEY_FETCH:
			var rbody FetchResponseV16Body
			rbody.responses = make([]TopicResponses, 0)
			resp.body = &rbody
		}

		respBytes, _ := resp.MarshalBinary()
		conn.Write(respBytes)
	}
}
