package registry

import (
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"
)

type Registry struct {
	timeout time.Duration
	mu      sync.Mutex
	servers map[string]*ServerItem
}

type ServerItem struct {
	Addr  string
	start time.Time
}

const (
	defaultTimeout = time.Minute * 5
	defaultPath    = "/_rpc/registry"
)

func NewRegistry(timeout time.Duration) *Registry {
	return &Registry{
		timeout: timeout,
		servers: make(map[string]*ServerItem),
	}
}

var DefaultRegister = NewRegistry(defaultTimeout)

func (r *Registry) registerServer(addr string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	server, ok := r.servers[addr]
	if !ok {
		r.servers[addr] = &ServerItem{
			Addr: addr,
		}
	}
	server.start = time.Now()
}

func (r *Registry) aliveServer() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	var alive []string
	for _, server := range r.servers {
		if r.timeout == 0 || server.start.Add(r.timeout).After(time.Now()) {
			alive = append(alive, server.Addr)
		} else {
			delete(r.servers, server.Addr)
		}
	}
	sort.Strings(alive)
	return alive
}

func (r *Registry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	switch req.Method {
	case "GET":
		w.Header().Set("RPC-Servers", strings.Join(r.aliveServer(), ","))
	case "POST":
		addr := req.Header.Get("RPC-Server")
		if addr == "" {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		r.registerServer(addr)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (r *Registry) HandleHttp(path string) {
	http.Handle(path, r)
}

func HandleHTTP() {
	DefaultRegister.HandleHttp(defaultPath)
}

func HeartBeat(path, addr string, duration time.Duration) {
	if duration == 0 {
		duration = defaultTimeout - time.Duration(1)*time.Minute
	}
	var err error
	go func() {
		t := time.NewTicker(duration)
		for err == nil {
			<-t.C
			err = sendHeartbeat(path, addr)
		}
	}()
}

func sendHeartbeat(path, addr string) error {
	client := &http.Client{}
	req, _ := http.NewRequest("POST", path, nil)
	req.Header.Set("RPC-Server", addr)
	if _, err := client.Do(req); err != nil {
		log.Println("rpc server: heart beat err:", err)
		return err
	}
	return nil
}
