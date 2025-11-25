package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"log/slog"
	"maps"
	"net"
	"os"
	"slices"
	"time"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	slog.SetLogLoggerLevel(slog.LevelInfo)
	if os.Getenv("BYOK") == "debug" {
		slog.SetLogLoggerLevel(slog.LevelDebug)
	}

	if len(os.Args) == 2 {
		// the path of the setver properties file
		fh, err := os.Open(os.Args[1])
		defer fh.Close()
		if err != nil {
			log.Fatalf("%v\n", err)
		}
		fc, err := io.ReadAll(fh)
		if err != nil {
			log.Fatalf("Unable to read: %v\n", err)
		}
		fmt.Print(string(fc))
	}

	// engine := Engine{}

	// ctx := context.Background()
	// context.WithValue(ctx, )

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
			slog.Debug("Topics", "length", len(reqBody.Topics))

			resp.Header = &ResponseHeaderV1{CorrelationId: r.CorrelationId}

			e := Engine{}

			resp.Body = e.HandleFetchV16(*reqBody)
		}
		encBytes, err := enc.Encode(resp)
		if err != nil {
			return
		}

		conn.Write(encBytes)
	}
}
