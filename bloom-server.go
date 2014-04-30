package main


//set foo 1  -- key+1
//exists foo -- member exists
//get foo -- get key count
//setnx foo 1 -- key+1 if it is not a member
//del foo -- delete the key
//save -- flush to disk

import (
	"fmt"
	"os"
	"net"
	"time"
	"strings"
	"strconv"
	"flag"
	"dablooms"
	"github.com/larspensjo/config"
)

var Config = make(map[string]string)

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

		if len(r) >= 4 {
			cmd := strings.ToUpper(r[2])
			if cmd == "SET"{
				sb.Add([]byte(r[4]), 1)
				conn.Write([]byte("+1\r\n"))
			}else if cmd == "SETNX"{
				if !sb.Check([]byte(r[4])) {
					sb.Add([]byte(r[4]), 1)
				}
				conn.Write([]byte("+1\r\n"))
			}else if cmd == "GET"{
				cnt := 0
				for sb.Check([]byte(r[4])) && cnt < 16 {
					sb.Remove([]byte(r[4]), 1)
					cnt++
				}
				for i := 0; i < cnt; i++ {
					sb.Add([]byte(r[4]), 1)
				}

				conn.Write([]byte("+" + strconv.Itoa(cnt) + "\r\n"))
			} else if cmd == "DEL"{
				cnt := 0
				for sb.Check([]byte(r[4])) && cnt<16{
					sb.Remove([]byte(r[4]), 1)
					cnt++
				}
				conn.Write([]byte("+1\r\n"))
			} else if cmd == "SAVE"{
				sb.Flush()
				conn.Write([]byte("+1\r\n"))
			} else if cmd == "EXISTS" {
				if sb.Check([]byte(r[4])) {
					conn.Write([]byte("+1\r\n"))
				}else {
					conn.Write([]byte("+0\r\n"))
				}
			}else {
				conn.Write([]byte("+OK\r\n"))
			}
		}else{
			conn.Write([]byte("+OK\r\n"))
		}
		request = make([]byte, 128) // clear last read content
	}
}

func init_sb() *dablooms.ScalingBloom {
	flag.Parse()
	//set config file std
	cfg, err := config.ReadDefault(*flag.String("configfile", "./bloom_config.ini", "General configuration file"))
	if err != nil {
		fmt.Println("Fail to find", "./bloom_config.ini", err)
	}

	//Initialized topic from the configuration
	if cfg.HasSection("Settings") {
		section, err := cfg.SectionOptions("Settings")
		if err == nil {
			for _, v := range section {
				options, err := cfg.String("Settings", v)
				if err == nil {
					Config[v] = options
				}
			}
		}
	}
	fmt.Println(Config)

	filename := Config["filename"]
	capacity, _ := strconv.Atoi(Config["capacity"])
	error_rate, _ := strconv.ParseFloat(Config["error_rate"], 64)

	_, err = os.Stat(filename)
	if err != nil && !os.IsExist(err) {
		fmt.Println("No file exists.")
		return dablooms.NewScalingBloom(dablooms.ParseUint(capacity), dablooms.ParseFloat(error_rate), filename)
	}else {
		return dablooms.NewScalingBloomFromFile(dablooms.ParseUint(capacity), dablooms.ParseFloat(error_rate), filename)
	}
}

func main() {

	var sb *dablooms.ScalingBloom = init_sb()
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
		go handleClient(conn, sb)
	}
}
