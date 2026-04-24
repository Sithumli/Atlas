package rpc

import (
	"bytes"
	"encoding/gob"
	"errors"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type reqMsg struct {
	endname  any
	svcMeth  string
	argsType reflect.Type
	args     []byte
	replyCh  chan replyMsg
}

type replyMsg struct {
	ok    bool
	reply []byte
}

type LabNetwork struct {
	mu             sync.Mutex
	reliable       bool
	longDelays     bool
	longReordering bool
	ends           map[any]*LabClientEnd
	enabled        map[any]bool
	servers        map[any]*LabServer
	connections    map[any]any
	endCh          chan reqMsg
	done           chan struct{}
	count          int32
	bytes          int64
}

func NewLabNetwork() *LabNetwork {
	rn := &LabNetwork{
		reliable:    true,
		ends:        make(map[any]*LabClientEnd),
		enabled:     make(map[any]bool),
		servers:     make(map[any]*LabServer),
		connections: make(map[any]any),
		endCh:       make(chan reqMsg),
		done:        make(chan struct{}),
	}
	go func() {
		for {
			select {
			case <-rn.done:
				return
			case xreq := <-rn.endCh:
				atomic.AddInt32(&rn.count, 1)
				atomic.AddInt64(&rn.bytes, int64(len(xreq.args)))
				go rn.processReq(xreq)
			}
		}
	}()
	return rn
}

func (rn *LabNetwork) Cleanup() {
	close(rn.done)
}

func (rn *LabNetwork) SetReliable(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.reliable = yes
}

func (rn *LabNetwork) SetLongReordering(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.longReordering = yes
}

func (rn *LabNetwork) SetLongDelays(yes bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.longDelays = yes
}

func (rn *LabNetwork) readEndnameInfo(endname any) (enabled bool,
	servername any, server *LabServer, reliable bool, longreordering bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	enabled = rn.enabled[endname]
	servername = rn.connections[endname]
	if servername != nil {
		server = rn.servers[servername]
	}
	reliable = rn.reliable
	longreordering = rn.longReordering
	return
}

func (rn *LabNetwork) isServerDead(endname any, servername any, server *LabServer) bool {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if !rn.enabled[endname] || rn.servers[servername] != server {
		return true
	}
	return false
}

func (rn *LabNetwork) processReq(req reqMsg) {
	enabled, servername, server, reliable, longreordering := rn.readEndnameInfo(req.endname)

	if enabled && servername != nil && server != nil {
		if !reliable {
			ms := rand.Int63n(27)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		if !reliable && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
			return
		}

		ech := make(chan replyMsg)
		go func() {
			r := server.dispatch(req)
			ech <- r
		}()

		var reply replyMsg
		replyOK := false
		serverDead := false
		for !replyOK && !serverDead {
			select {
			case reply = <-ech:
				replyOK = true
			case <-time.After(100 * time.Millisecond):
				serverDead = rn.isServerDead(req.endname, servername, server)
				if serverDead {
					go func() {
						<-ech
					}()
				}
			}
		}

		serverDead = rn.isServerDead(req.endname, servername, server)

		if !replyOK || serverDead {
			req.replyCh <- replyMsg{false, nil}
		} else if !reliable && (rand.Int()%1000) < 100 {
			req.replyCh <- replyMsg{false, nil}
		} else if longreordering && rand.Intn(900) < 600 {
			ms := 200 + rand.Int63n(1+rand.Int63n(2000))
			time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
				atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
				req.replyCh <- reply
			})
		} else {
			atomic.AddInt64(&rn.bytes, int64(len(reply.reply)))
			req.replyCh <- reply
		}
	} else {
		ms := 0
		if rn.longDelays {
			ms = rand.Int() % 7000
		} else {
			ms = rand.Int() % 100
		}
		time.AfterFunc(time.Duration(ms)*time.Millisecond, func() {
			req.replyCh <- replyMsg{false, nil}
		})
	}
}

func (rn *LabNetwork) MakeEnd(endname any) *LabClientEnd {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	if _, ok := rn.ends[endname]; ok {
		panic("MakeEnd: " + endname.(string) + " already exists")
	}
	e := &LabClientEnd{
		endname: endname,
		ch:      rn.endCh,
		done:    rn.done,
	}
	rn.ends[endname] = e
	rn.enabled[endname] = false
	rn.connections[endname] = nil
	return e
}

func (rn *LabNetwork) AddServer(servername any, srv *LabServer) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.servers[servername] = srv
}

func (rn *LabNetwork) DeleteServer(servername any) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.servers[servername] = nil
}

func (rn *LabNetwork) Connect(endname any, servername any) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.connections[endname] = servername
}

func (rn *LabNetwork) Enable(endname any, enabled bool) {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	rn.enabled[endname] = enabled
}

func (rn *LabNetwork) GetCount(servername any) int {
	rn.mu.Lock()
	defer rn.mu.Unlock()
	svr := rn.servers[servername]
	if svr == nil {
		return 0
	}
	return svr.GetCount()
}

func (rn *LabNetwork) GetTotalCount() int {
	return int(atomic.LoadInt32(&rn.count))
}

func (rn *LabNetwork) GetTotalBytes() int64 {
	return atomic.LoadInt64(&rn.bytes)
}

type LabClientEnd struct {
	endname any
	ch      chan reqMsg
	done    chan struct{}
}

func (e *LabClientEnd) Call(svcMeth string, args any, reply any) bool {
	req := reqMsg{
		endname:  e.endname,
		svcMeth:  svcMeth,
		argsType: reflect.TypeOf(args),
		replyCh:  make(chan replyMsg),
	}

	qb := new(bytes.Buffer)
	qe := gob.NewEncoder(qb)
	if err := qe.Encode(args); err != nil {
		panic(err)
	}
	req.args = qb.Bytes()

	select {
	case e.ch <- req:
	case <-e.done:
		return false
	}

	rep := <-req.replyCh
	if !rep.ok {
		return false
	}
	rb := bytes.NewBuffer(rep.reply)
	rd := gob.NewDecoder(rb)
	if err := rd.Decode(reply); err != nil {
		return false
	}
	return true
}

type LabServer struct {
	mu       sync.Mutex
	services map[string]*LabService
	count    int
}

func NewLabServer() *LabServer {
	return &LabServer{services: make(map[string]*LabService)}
}

func (rs *LabServer) AddService(svc *LabService) {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	rs.services[svc.name] = svc
}

func (rs *LabServer) dispatch(req reqMsg) replyMsg {
	rs.mu.Lock()
	rs.count += 1
	dot := strings.LastIndex(req.svcMeth, ".")
	serviceName := req.svcMeth[:dot]
	methodName := req.svcMeth[dot+1:]
	service, ok := rs.services[serviceName]
	rs.mu.Unlock()
	if ok {
		return service.dispatch(methodName, req)
	}
	choices := []string{}
	for k := range rs.services {
		choices = append(choices, k)
	}
	_ = choices
	return replyMsg{false, nil}
}

func (rs *LabServer) GetCount() int {
	rs.mu.Lock()
	defer rs.mu.Unlock()
	return rs.count
}

type LabService struct {
	name     string
	rcvr     reflect.Value
	typ      reflect.Type
	methods  map[string]reflect.Method
}

func NewLabService(rcvr any) *LabService {
	svc := &LabService{
		typ:     reflect.TypeOf(rcvr),
		rcvr:    reflect.ValueOf(rcvr),
		methods: make(map[string]reflect.Method),
	}
	svc.name = reflect.Indirect(svc.rcvr).Type().Name()
	for m := 0; m < svc.typ.NumMethod(); m++ {
		method := svc.typ.Method(m)
		if method.PkgPath != "" {
			continue
		}
		if method.Type.NumIn() != 3 || method.Type.NumOut() != 0 {
			continue
		}
		svc.methods[method.Name] = method
	}
	return svc
}

func (svc *LabService) dispatch(methname string, req reqMsg) replyMsg {
	method, ok := svc.methods[methname]
	if !ok {
		return replyMsg{false, nil}
	}

	args := reflect.New(method.Type.In(1).Elem())
	ab := bytes.NewBuffer(req.args)
	ad := gob.NewDecoder(ab)
	if err := ad.Decode(args.Interface()); err != nil {
		return replyMsg{false, nil}
	}

	replyType := method.Type.In(2).Elem()
	replyv := reflect.New(replyType)

	function := method.Func
	function.Call([]reflect.Value{svc.rcvr, args, replyv})

	rb := new(bytes.Buffer)
	re := gob.NewEncoder(rb)
	if err := re.EncodeValue(replyv); err != nil {
		return replyMsg{false, nil}
	}
	return replyMsg{true, rb.Bytes()}
}

var ErrUnknownMethod = errors.New("unknown rpc method")
