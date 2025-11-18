package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

type Response struct {
	message_size int32
	header       int32
}

type Request struct {
	message_size   int32
	api_key        int16
	api_version    int16
	correlation_id int32
	client_id      string
}

func ParseRequest(in []byte) (Request, error) {
	r := Request{}
	r.message_size = int32(binary.BigEndian.Uint32(in))
	r.api_key = int16(binary.BigEndian.Uint16(in[4:]))
	r.api_version = int16(binary.BigEndian.Uint16(in[6:]))
	r.correlation_id = int32(binary.BigEndian.Uint32(in[8:]))

	return r, nil
}

func (m Response) ToBytes() []byte {
	out := make([]byte, 0)
	out = binary.BigEndian.AppendUint32(out, uint32(m.message_size))
	out = binary.BigEndian.AppendUint32(out, uint32(m.header))
	return out
}

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

	requestBytes := make([]byte, 12)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	conn.Read(requestBytes)
	r, _ := ParseRequest(requestBytes)
	temp := Response{0, r.correlation_id}
	conn.Write(temp.ToBytes())
}
