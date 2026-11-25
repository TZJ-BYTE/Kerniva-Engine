package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

type options struct {
	attach, source, config, demoURL string
}

func workbenchOrigin(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err == nil && u.Scheme == "http" && u.User == nil && (u.Path == "" || u.Path == "/") &&
		u.RawQuery == "" && !u.ForceQuery && u.Fragment == "" && u.RawFragment == "" &&
		(u.Hostname() == "127.0.0.1" || u.Hostname() == "localhost") {
		port, err := strconv.Atoi(u.Port())
		if err == nil && port > 0 && port <= 65535 {
			return fmt.Sprintf("http://127.0.0.1:%d", port), nil
		}
	}
	return "", errors.New("请输入本地工作台地址，例如 http://127.0.0.1:18782")
}

func (o options) command() (*exec.Cmd, error) {
	args := []string{"--port", "0", "--parent-stdin"}
	if o.config != "" {
		args = append(args, "--config", o.config)
	}
	if o.demoURL != "" {
		origin, err := workbenchOrigin(o.demoURL)
		if err != nil {
			return nil, fmt.Errorf("复制控制器地址无效：%w", err)
		}
		args = append(args, "--demo-url", origin)
	}
	if o.source != "" {
		script, err := filepath.Abs(filepath.Join(o.source, "scripts", "kerniva-workbench.py"))
		if err != nil {
			return nil, err
		}
		if info, err := os.Stat(script); err != nil || !info.Mode().IsRegular() {
			return nil, fmt.Errorf("找不到工作台脚本 %s", script)
		}
		python := []string{"python3", "python"}
		if runtime.GOOS == "windows" {
			python = []string{"python", "py", "python3"}
		}
		for _, name := range python {
			path, err := exec.LookPath(name)
			if err != nil {
				continue
			}
			prefix := []string{"-B", "-u", script}
			if name == "py" {
				prefix = append([]string{"-3"}, prefix...)
			}
			return exec.Command(path, append(prefix, args...)...), nil
		}
		return nil, errors.New("源码模式需要 Python 3.9 或更新版本")
	}
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	name := "kerniva-workbench-service"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	dir := filepath.Dir(exe)
	paths := []string{filepath.Join(dir, "backend", name)}
	if runtime.GOOS == "darwin" {
		paths = append([]string{filepath.Join(dir, "..", "Resources", "backend", name)}, paths...)
	}
	for _, path := range paths {
		if info, err := os.Stat(path); err == nil && info.Mode().IsRegular() {
			return exec.Command(path, args...), nil
		}
	}
	return nil, fmt.Errorf("找不到随应用安装的管理服务 %s，请重新解压完整安装包", paths[0])
}

type child struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	done  chan struct{}
}

func (c *child) stop() {
	_ = c.stdin.Close()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
	}
}

type backend struct {
	chooseDirectory func() (string, error)
	dialogMu        sync.Mutex
	options         options
	ctx             context.Context
	cancel          context.CancelFunc
	mu              sync.Mutex
	origin          string
	owned           *child
	client          *http.Client
}

func newBackend(o options) *backend {
	ctx, cancel := context.WithCancel(context.Background())
	return &backend{
		options: o, ctx: ctx, cancel: cancel,
		client: &http.Client{
			Transport:     &http.Transport{}, // Loopback traffic must stay on this host.
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

func (b *backend) close() {
	b.cancel()
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.owned != nil {
		b.owned.stop()
		b.owned = nil
	}
	b.client.CloseIdleConnections()
}

func (b *backend) check(ctx context.Context, origin string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, origin+"/api/state", nil)
	if err != nil {
		return err
	}
	resp, err := b.client.Do(req)
	if err != nil {
		return fmt.Errorf("无法连接工作台 %s：%w", origin, err)
	}
	defer resp.Body.Close()
	var state struct {
		APIVersion int    `json:"apiVersion"`
		ConfigID   string `json:"configId"`
	}
	if resp.StatusCode != http.StatusOK || json.NewDecoder(io.LimitReader(resp.Body, 4<<20)).Decode(&state) != nil ||
		state.APIVersion != 2 || state.ConfigID == "" {
		return fmt.Errorf("%s 未提供兼容的 Kerniva 工作台管理服务", origin)
	}
	return nil
}

// endpoint creates a service only while opening the window. API operations never
// restart a process or replay a write after losing the original session.
func (b *backend) endpoint(opening bool) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := b.ctx.Err(); err != nil {
		return "", err
	}
	if c := b.owned; c != nil {
		select {
		case <-c.done:
			b.origin, b.owned = "", nil
		default:
		}
	}
	if b.origin != "" && !opening {
		return b.origin, nil
	}
	if !opening {
		return "", errors.New("工作台连接已断开，请刷新窗口以重新连接")
	}
	ctx, cancel := context.WithTimeout(b.ctx, 20*time.Second)
	defer cancel()
	if b.origin != "" {
		return b.origin, b.check(ctx, b.origin)
	}
	if b.options.attach != "" {
		if b.options.config != "" || b.options.demoURL != "" {
			return "", errors.New("附着现有工作台时，配置文件和复制控制器由该工作台管理")
		}
		origin, err := workbenchOrigin(b.options.attach)
		if err != nil {
			return "", err
		}
		if err := b.check(ctx, origin); err != nil {
			return "", err
		}
		b.origin = origin
		return origin, nil
	}
	cmd, err := b.options.command()
	if err != nil {
		return "", err
	}
	prepareChild(cmd)
	input, err := cmd.StdinPipe()
	if err != nil {
		return "", err
	}
	output, err := cmd.StdoutPipe()
	if err != nil {
		input.Close()
		return "", err
	}
	var stderr strings.Builder
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		input.Close()
		output.Close()
		return "", err
	}
	c := &child{cmd: cmd, stdin: input, done: make(chan struct{})}
	ready := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(output)
		for scanner.Scan() {
			if origin, ok := strings.CutPrefix(scanner.Text(), "Open "); ok {
				select {
				case ready <- origin:
				default:
				}
			}
		}
		output.Close()
		_ = cmd.Wait()
		close(c.done)
	}()
	var origin string
	select {
	case <-ctx.Done():
		err = fmt.Errorf("等候管理服务启动：%w", ctx.Err())
	case <-c.done:
		err = fmt.Errorf("管理服务启动后退出（%s）%s", cmd.ProcessState, strings.TrimSpace(stderr.String()))
	case raw := <-ready:
		origin, err = workbenchOrigin(raw)
		if err == nil {
			err = b.check(ctx, origin)
		}
	}
	if err != nil {
		c.stop()
		return "", err
	}
	b.origin, b.owned = origin, c
	return origin, nil
}
