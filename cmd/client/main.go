package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"unicode"

	"github.com/TZJ-BYTE/Kerniva/internal/respclient"
)

func main() {
	host, port := "127.0.0.1", "16379"
	if len(os.Args) > 1 {
		host = os.Args[1]
	}
	if len(os.Args) > 2 {
		port = os.Args[2]
	}
	if len(os.Args) > 3 {
		fmt.Fprintln(os.Stderr, "用法 kerniva-client [host] [port]")
		os.Exit(1)
	}
	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		fmt.Fprintln(os.Stderr, "端口必须在 1 到 65535 之间")
		os.Exit(1)
	}
	address := net.JoinHostPort(host, port)
	conn, err := net.DialTimeout("tcp", address, 5*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "连接失败 %v\n", err)
		os.Exit(1)
	}
	defer conn.Close()
	fmt.Printf("Kerniva 客户端，已连接 %s\n", address)
	fmt.Println(`输入 exit 退出。空格参数可用引号，空字符串写为 ""，支持 \n、\r、\t、\\ 和 \xHH。`)
	if err := interact(conn, os.Stdin, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func interact(conn net.Conn, input io.Reader, output io.Writer) error {
	commands := bufio.NewReader(input)
	replies := bufio.NewReader(conn)
	for {
		fmt.Fprint(output, "kerniva> ")
		line, inputErr := commands.ReadString('\n')
		if inputErr != nil && !errors.Is(inputErr, io.EOF) {
			return fmt.Errorf("读取输入失败 %w", inputErr)
		}
		arguments, err := splitCommand(line)
		if err != nil {
			fmt.Fprintf(output, "命令格式错误 %v\n", err)
		} else if len(arguments) > 0 {
			if len(arguments) == 1 && (strings.EqualFold(arguments[0], "exit") || strings.EqualFold(arguments[0], "quit")) {
				return nil
			}
			if err := respclient.WriteRequest(conn, arguments...); err != nil {
				return fmt.Errorf("发送命令失败 %w", err)
			}
			reply, err := respclient.ReadReply(replies)
			if err != nil {
				return fmt.Errorf("接收响应失败 %w", err)
			}
			fmt.Fprintln(output, reply.String())
		}
		if errors.Is(inputErr, io.EOF) {
			return nil
		}
	}
}

func splitCommand(input string) ([]string, error) {
	var arguments []string
	var argument strings.Builder
	var quote rune
	active := false
	characters := []rune(input)
	for index := 0; index < len(characters); index++ {
		character := characters[index]
		if character == '\\' {
			index++
			if index == len(characters) {
				return nil, fmt.Errorf("行尾反斜杠缺少转义字符")
			}
			active = true
			switch characters[index] {
			case 'n':
				argument.WriteByte('\n')
			case 'r':
				argument.WriteByte('\r')
			case 't':
				argument.WriteByte('\t')
			case 'b':
				argument.WriteByte('\b')
			case 'a':
				argument.WriteByte('\a')
			case 'x':
				if index+2 >= len(characters) {
					return nil, fmt.Errorf("\\x 后需要两位十六进制数字")
				}
				value, err := strconv.ParseUint(string(characters[index+1:index+3]), 16, 8)
				if err != nil {
					return nil, fmt.Errorf("无效的十六进制转义")
				}
				argument.WriteByte(byte(value))
				index += 2
			default:
				argument.WriteRune(characters[index])
			}
			continue
		}
		if quote != 0 {
			if character == quote {
				quote = 0
			} else {
				argument.WriteRune(character)
			}
			continue
		}
		switch {
		case character == '\'' || character == '"':
			quote = character
			active = true
		case unicode.IsSpace(character):
			if active {
				arguments = append(arguments, argument.String())
				argument.Reset()
				active = false
			}
		default:
			argument.WriteRune(character)
			active = true
		}
	}
	if quote != 0 {
		return nil, fmt.Errorf("引号未闭合")
	}
	if active {
		arguments = append(arguments, argument.String())
	}
	return arguments, nil
}
