package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"time"
)

type Message struct {
	message_size int
	header       int
}

func (m Message) ToBytes() []byte {
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

	request := make([]byte, 8)
	conn.SetReadDeadline(time.Now().Add(1 * time.Second))
	conn.Read(request)
	temp := Message{0, 7}
	conn.Write(temp.ToBytes())
}
