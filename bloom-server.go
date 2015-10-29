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
	"io"
	"bufio"
	"io/ioutil"
)

var Config = make(map[string]string)
var SericePort = ":1234"
type Request struct {
	Name       string
	Args       [][]byte
	Host       string
	Body       io.ReadCloser
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func parseRequest(conn io.ReadCloser) (*Request, error) {
	r := bufio.NewReader(conn)
	// first line of redis request should be:
	// *<number of arguments>CRLF
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	// note that this line also protects us from negative integers
	var argsCount int

	// Multiline request:
	if line[0] == '*' {
		if _, err := fmt.Sscanf(line, "*%d\r", &argsCount); err != nil {
			return nil, malformed("*<numberOfArguments>", line)
		}
		// All next lines are pairs of:
		//$<number of bytes of argument 1> CR LF
		//<argument data> CR LF
		// first argument is a command name, so just convert
		firstArg, err := readArgument(r)
		if err != nil {
			return nil, err
		}

		args := make([][]byte, argsCount-1)
		for i := 0; i < argsCount-1; i += 1 {
			if args[i], err = readArgument(r); err != nil {
				return nil, err
			}
		}

		return &Request{
			Name: strings.ToUpper(string(firstArg)),
			Args: args,
		}, nil
	}

	// Inline request:
	fields := strings.Split(strings.Trim(line, "\r\n"), " ")

	var args [][]byte
	if len(fields) > 1 {
		for _, arg := range fields[1:] {
			args = append(args, []byte(arg))
		}
	}
	return &Request{
		Name: strings.ToUpper(string(fields[0])),
		Args: args,
	}, nil

}

func readArgument(r *bufio.Reader) ([]byte, error) {

	line, err := r.ReadString('\n')
	if err != nil {
		return nil, malformed("$<argumentLength>", line)
	}
	var argSize int
	if _, err := fmt.Sscanf(line, "$%d\r", &argSize); err != nil {
		return nil, malformed("$<argumentSize>", line)
	}

	// I think int is safe here as the max length of request
	// should be less then max int value?
	data, err := ioutil.ReadAll(io.LimitReader(r, int64(argSize)))
	if err != nil {
		return nil, err
	}

	if len(data) != argSize {
		return nil, malformedLength(argSize, len(data))
	}

	// Now check for trailing CR
	if b, err := r.ReadByte(); err != nil || b != '\r' {
		return nil, malformedMissingCRLF()
	}

	// And LF
	if b, err := r.ReadByte(); err != nil || b != '\n' {
		return nil, malformedMissingCRLF()
	}

	return data, nil
}

func malformed(expected string, got string) error {
	return fmt.Errorf("Mailformed request:'%s does not match %s\\r\\n'", got, expected)
}

func malformedLength(expected int, got int) error {
	return fmt.Errorf(
		"Mailformed request: argument length '%d does not match %d\\r\\n'",
		got, expected)
}

func malformedMissingCRLF() error {
	return fmt.Errorf("Mailformed request: line should end with \\r\\n")
}

func handleClient(conn net.Conn, sb *dablooms.ScalingBloom) (err error) {
	conn.SetReadDeadline(time.Now().Add(2 * time.Minute))
	//	request := make([]byte, 1024*4)
	defer conn.Close()

	for {
		request, err := parseRequest(conn)
		if err != nil {
			return err
		}
//		fmt.Println(request.Args)
//		fmt.Println(request.Name)

		switch request.Name{
			case "SET": {
				sb.Add(request.Args[0], 1)
				conn.Write([]byte("+1\r\n"))
			}
			case "SETNX": {
				if !sb.Check(request.Args[0]) {
					sb.Add(request.Args[0], 1)
				}
				conn.Write([]byte("+1\r\n"))
			}
			case "GET": {
				cnt := 0
				for sb.Check(request.Args[0]) && cnt < 16 {
					sb.Remove(request.Args[0], 1)
					cnt++
				}
				for i := 0; i < cnt; i++ {
					sb.Add(request.Args[0], 1)
				}

				conn.Write([]byte("+" + strconv.Itoa(cnt) + "\r\n"))
			}
			case "DEL": {
				cnt := 0
				for sb.Check(request.Args[0]) && cnt < 16 {
					sb.Remove(request.Args[0], 1)
					cnt++
				}
				conn.Write([]byte("+1\r\n"))
			}
			case "SAVE": {
				sb.Flush()
				conn.Write([]byte("+1\r\n"))
			}
			case "EXISTS": {
				if sb.Check(request.Args[0]) {
					conn.Write([]byte(":1\r\n"))
				}else {
					conn.Write([]byte(":0\r\n"))
				}
			}
			case "MGET": {
				rest := ""
				cnt := 0
				for i := 0; i < len(request.Args); i++ {
					cnt += 1
					if sb.Check(request.Args[i]) {
						rest += "$1\r\n1\r\n"
					}else {
						rest += "$1\r\n0\r\n"
					}
				}
				str := "*" + strconv.Itoa(cnt) + "\r\n"
				str += rest
				conn.Write([]byte(str))
			}
			default: {
				conn.Write([]byte("+OK\r\n"))
			}
		}
	}
	return nil
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
	SericePort = Config["service_port"]


	_, err = os.Stat(filename)
	if err != nil && !os.IsExist(err) {
		fmt.Println("No file exists.")
		return dablooms.NewScalingBloom(dablooms.ParseUint(capacity), dablooms.ParseFloat(error_rate), filename)
	}
	return dablooms.NewScalingBloomFromFile(dablooms.ParseUint(capacity), dablooms.ParseFloat(error_rate), filename)
}

func main() {

	var sb *dablooms.ScalingBloom = init_sb()
	fmt.Println("starting in", SericePort)
	tcpAddr, err := net.ResolveTCPAddr("tcp4", SericePort)
	checkError(err)
	listener, err := net.ListenTCP("tcp", tcpAddr)
	checkError(err)
	for {conn, err := listener.Accept()
		if err != nil {
			continue
		}
		go handleClient(conn, sb)
	}
}
