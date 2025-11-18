package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

const UNSUPPORTED_VERSION = 35

type Message struct {
	message_size int32
}

type Response struct {
	Message
	correlation_id int32
	body           []byte
}

type Request struct {
	Message
	request_api_key     int16
	request_api_version int16
	correlation_id      int32
	client_id           string
	body                []byte
}

func (m Message) AppendBinary(b []byte) ([]byte, error) {
	bytes, err := m.MarshalBinary()
	if err != nil {
		return nil, err
	}
	return append(b, bytes...), nil
}

func (m Message) MarshalBinary() (data []byte, err error) {
	data = make([]byte, 0)
	data = binary.BigEndian.AppendUint32(data, uint32(m.message_size))
	return
}

func (m *Message) UnmarshalBinary(in []byte) error {
	m.message_size = int32(binary.BigEndian.Uint32(in))
	return nil
}

func (r *Request) UnmarshalBinary(in []byte) error {
	r.Message.UnmarshalBinary(in)
	r.request_api_key = int16(binary.BigEndian.Uint16(in[4:]))
	r.request_api_version = int16(binary.BigEndian.Uint16(in[6:]))
	r.correlation_id = int32(binary.BigEndian.Uint32(in[8:]))

	return nil
}

func (r Response) MarshalBinary() ([]byte, error) {
	out, err := r.Message.MarshalBinary()
	if err != nil {
		return nil, err
	}
	out = binary.BigEndian.AppendUint32(out, uint32(r.correlation_id))
	out = append(out, r.body...)
	return out, nil
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
	r := new(Request)
	r.UnmarshalBinary(requestBytes)
	resp := Response{Message{0}, r.correlation_id, []byte{}}
	if r.request_api_version > 4 {
		resp.body = binary.BigEndian.AppendUint16(resp.body, 35)
	}

	respBytes, _ := resp.MarshalBinary()
	conn.Write(respBytes)
}
