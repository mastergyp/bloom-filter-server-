package main

import (
	"fmt"
	"os"
	"net"
	"time"
	"strings"
	"github.com/bitly/dablooms/godablooms"
//	"strconv"
)


func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func handleClient(conn net.Conn, sb *dablooms.ScalingBloom) {
	conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
	request := make([]byte, 128)
	defer conn.Close()
	for {read_len, err := conn.Read(request)
		if err != nil || read_len == 0 {
			//connection already closed by client
			break
		}
		r := strings.Split(string(request), "\r\n")
		if r[0] == "*3" && r[1] == "$3" && strings.ToUpper(r[2]) == "SET"{
			//set
			fmt.Println(sb.Add([]byte(r[4]), 1))

			conn.Write([]byte("+1\r\n"))
		} else if r[0] == "*2" && r[1] == "$3" && strings.ToUpper(r[2]) == "GET"{
			//get
			score := sb.Check([]byte(r[4]))
			fmt.Println("score", score)

			if score == 1{
				conn.Write([]byte("+1\r\n"))
			}else{
				conn.Write([]byte("+0\r\n"))
			}

		}else if r[0] == "*2" && r[1] == "$3" && strings.ToUpper(r[2]) == "DEL"{
			//delete
			sb.Remove([]byte(r[4]), 1)

			conn.Write([]byte("+1\r\n"))
		} else {
			conn.Write([]byte("+OK\r\n"))
		}
		request = make([]byte, 128) // clear last read content
	}
}

func main() {
	sb := dablooms.NewScalingBloomFromFile(1000, 0.5, "testbloom.bin")

	if sb == nil{
		fmt.Println("fuck init bloom filter error")
	}
	service := ":1234"
	fmt.Println("starting in", service)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", service)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	for {
		conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn,sb)
	}
}
