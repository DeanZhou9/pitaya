package cluster

import (
	"fmt"
	"sync"
	"time"

	"github.com/go-zookeeper/zk"
	"github.com/topfreegames/pitaya/v2/constants"
	"github.com/topfreegames/pitaya/v2/logger"
	"golang.org/x/exp/slices"
)

type zookeeperServiceDiscovery struct {
	loadTypeLock    sync.Mutex
	mapByTypeLock   sync.RWMutex
	serverMapByType map[string]map[string]*Server
	serverMapByID   sync.Map
	server          *Server
	conn            *zk.Conn
	listeners       []SDListener
	masterPath      string
	serverPath      string
	typePath        string
	stopChan        chan bool
	resetChan       chan bool
	connTimeout     time.Duration
	watchedTypes    []string
	knowAllServer   bool
}

// NewZookeeperServiceDiscovery ctor
func NewZookeeperServiceDiscovery(
	config interface{},
	server *Server) (ServiceDiscovery, error) {
	sd := &zookeeperServiceDiscovery{
		serverMapByType: make(map[string]map[string]*Server),
		server:          server,
		listeners:       make([]SDListener, 0),
		masterPath:      "/game1",
		serverPath:      "/game1/servers",
		typePath:        "/game1/types",
		stopChan:        make(chan bool),
		resetChan:       make(chan bool),
		connTimeout:     time.Duration(time.Second * 10),
		watchedTypes:    []string{},
		knowAllServer:   false,
	}

	// append self to server list
	sd.serverMapByID.Store(sd.server.ID, sd.server)
	sd.serverMapByType[sd.server.Type] = make(map[string]*Server)
	sd.serverMapByType[sd.server.Type][sd.server.ID] = sd.server

	return sd, nil
}

func (sd *zookeeperServiceDiscovery) Init() error {
	logger.Log.Info("zookeeper Init")

	conn, event, err := zk.Connect([]string{"127.0.0.1"}, time.Second*10)
	if err != nil {
		return err
	}
	sd.conn = conn
	err = sd.connectToServer(event)
	if err != nil {
		return err
	}

	// watch for event
	go func() {
		for {
			select {
			case e := <-event:
				logger.Log.Infof("zk event %+v", e)

				switch e.State {
				case zk.State(zk.StateExpired):
					close(sd.resetChan)
					sd.resetChan = make(chan bool)
					sd.registerSelf()
				}
			case <-sd.stopChan:
				return
			}
		}
	}()

	return nil
}

func (sd *zookeeperServiceDiscovery) AfterInit() {
	logger.Log.Info("zookeeper AfterInit")
	sd.registerSelf()
}

func (sd *zookeeperServiceDiscovery) BeforeShutdown() {
	logger.Log.Info("zookeeper BeforeShutdown")
	close(sd.stopChan)
}

func (sd *zookeeperServiceDiscovery) Shutdown() error {
	logger.Log.Info("zookeeper Shutdown")
	sd.conn.Close()

	return nil
}

func (sd *zookeeperServiceDiscovery) GetServersByType(serverType string) (map[string]*Server, error) {
	sd.mapByTypeLock.RLock()
	defer sd.mapByTypeLock.RUnlock()
	if m, ok := sd.serverMapByType[serverType]; ok && len(m) > 0 {
		ret := make(map[string]*Server, len(m))
		for k, v := range m {
			ret[k] = v
		}
		return ret, nil
	}

	return nil, constants.ErrNoServersAvailableOfType
}

func (sd *zookeeperServiceDiscovery) GetServer(id string) (*Server, error) {
	if sv, ok := sd.serverMapByID.Load(id); ok {
		return sv.(*Server), nil
	}
	return nil, constants.ErrNoServerWithID
}

func (sd *zookeeperServiceDiscovery) GetServers() []*Server {
	ret := make([]*Server, 0)
	sd.serverMapByID.Range(func(k, v interface{}) bool {
		ret = append(ret, v.(*Server))

		return true
	})

	return ret
}

func (sd *zookeeperServiceDiscovery) AddListener(listener SDListener) {
	sd.listeners = append(sd.listeners, listener)
}

func (sd *zookeeperServiceDiscovery) SyncServers(firstSync bool) error {
	return nil
}

func (sd *zookeeperServiceDiscovery) LoadServersByType(serverType string, force bool) error {
	if sd.knowAllServer {
		return nil
	}

	if !force && slices.Contains(sd.watchedTypes, serverType) {
		return nil
	}

	sd.loadTypeLock.Lock()
	defer sd.loadTypeLock.Unlock()

	// double check
	if slices.Contains(sd.watchedTypes, serverType) {
		return nil
	}

	if err := sd.loadData(sd.formatTypePath(serverType)); err != nil {
		return err
	}

	// mark as already watched
	sd.watchedTypes = append(sd.watchedTypes, serverType)

	return nil
}

//// private func

func (sd *zookeeperServiceDiscovery) formatTypePath(serverType string) string {
	return fmt.Sprintf("%s/%s", sd.typePath, serverType)
}

func (sd *zookeeperServiceDiscovery) connectToServer(event <-chan zk.Event) error {
	// wait for session ready
	for {
		select {
		case e := <-event:
			logger.Log.Infof("zk event %+v", e)
			switch e.State {
			case zk.State(zk.StateHasSession):
				// ensure path exists in the server
				if err := sd.ensurePath(sd.masterPath); err != nil {
					return err
				}

				// ensure all servers path
				if err := sd.ensurePath(sd.serverPath); err != nil {
					return err
				}
				// ensure type parent path
				if err := sd.ensurePath(sd.typePath); err != nil {
					return err
				}
				// ensure type exits
				if err := sd.ensurePath(sd.formatTypePath(sd.server.Type)); err != nil {
					return err
				}

				return nil
			}
		case <-sd.stopChan:
			return nil
		case <-time.After(sd.connTimeout):
			return constants.ErrEtcdGrantLeaseTimeout
		}
	}
}

func (sd *zookeeperServiceDiscovery) ensurePath(path string) error {
	exist, _, err := sd.conn.Exists(path)
	if err != nil {
		logger.Log.Errorf("error ensure path: %s, error: %s", path, err.Error())
		return err
	}

	if !exist {
		if _, err := sd.conn.Create(path, nil, 0, zk.WorldACL(zk.PermAll)); err != nil {
			logger.Log.Errorf("error ensure create path: %s, error: %s", path, err.Error())
			return err
		}
		logger.Log.Infof("create path: %s", path)
	}

	return nil
}

func (sd *zookeeperServiceDiscovery) loadData(serverPath string) error {
	servers, _, event, err := sd.conn.ChildrenW(serverPath)
	if err != nil {
		logger.Log.Errorf("load data path: %s, error sync servers: %s", serverPath, err.Error())

		return err
	}

	logger.Log.Debugf("load data path: %s, servers: %v", serverPath, servers)

	// get server info
	if len(servers) > 0 {
		// remove servers
		sd.serverMapByID.Range(func(key, value any) bool {
			k := key.(string)
			if !slices.Contains(servers, k) {
				sd.deleteServer(k)
			}

			return true
		})

		// add servers
		added := []string{}
		for _, v := range servers {
			if _, ok := sd.serverMapByID.Load(v); !ok {
				added = append(added, v)
			}
		}
		if len(added) > 0 {
			concurrency := newConcurrency(sd)
			for _, v := range added {
				concurrency.addWork(fmt.Sprintf("%s/%s", serverPath, v))
			}
			result := concurrency.waitAndGetResult()

			// add server
			sd.addServer(result)
		}
	}

	sd.printServers()

	// watch for something changed
	go func() {
		select {
		case e := <-event:
			logger.Log.Warnf("watch event callback: %s, %v", e.State.String(), e)
			sd.loadData(serverPath)
			return
		case <-sd.stopChan:
			return
		case <-sd.resetChan:
			return
		}
	}()

	return nil
}

func (sd *zookeeperServiceDiscovery) registerSelf() error {
	// add server to sd
	data := sd.server.AsJSONString()
	if err := sd.registerSelfByPath(fmt.Sprintf("%s/%s", sd.serverPath, sd.server.ID), data); err != nil {
		return err
	}

	if err := sd.registerSelfByPath(fmt.Sprintf("%s/%s", sd.formatTypePath(sd.server.Type), sd.server.ID), data); err != nil {
		return err
	}

	// sync servers
	if sd.knowAllServer {
		if err := sd.loadData(sd.serverPath); err != nil {
			return err
		}
	} else {
		// sync all watched types
		wg := sync.WaitGroup{}
		for _, v := range sd.watchedTypes {
			wg.Add(1)
			go func(serverType string) {
				defer wg.Done()
				sd.loadData(sd.formatTypePath(serverType))
			}(v)
		}
		wg.Wait()
	}

	return nil
}

func (sd *zookeeperServiceDiscovery) registerSelfByPath(path string, data string) error {
	acls := zk.WorldACL(zk.PermAll)
	_, err := sd.conn.Create(path, []byte(data), zk.FlagEphemeral, acls)
	if err != nil {
		logger.Log.Errorf("error create server path: %s, error: %s", path, err.Error())
		return err
	}

	return nil
}

func (sd *zookeeperServiceDiscovery) writeLockScope(f func()) {
	sd.mapByTypeLock.Lock()
	defer sd.mapByTypeLock.Unlock()
	f()
}

func (sd *zookeeperServiceDiscovery) notifyListeners(act Action, sv *Server) {
	for _, l := range sd.listeners {
		if act == DEL {
			l.RemoveServer(sv)
		} else if act == ADD {
			l.AddServer(sv)
		}
	}
}

func (sd *zookeeperServiceDiscovery) addServer(servers []*Server) {
	for _, v := range servers {
		if _, loaded := sd.serverMapByID.LoadOrStore(v.ID, v); !loaded {
			// store to serverMapByType
			sd.writeLockScope(func() {
				mapByType, ok := sd.serverMapByType[v.Type]
				if !ok {
					mapByType = make(map[string]*Server)
					sd.serverMapByType[v.Type] = mapByType
				}
				mapByType[v.ID] = v
			})
			if v.ID != sd.server.ID {
				sd.notifyListeners(ADD, v)
			}
		}
	}
}

func (sd *zookeeperServiceDiscovery) deleteServer(serverID string) {
	if sv, ok := sd.serverMapByID.Load(serverID); ok {
		server := sv.(*Server)
		sd.serverMapByID.Delete(server.ID)
		sd.writeLockScope(func() {
			if svMap, ok := sd.serverMapByType[server.Type]; ok {
				delete(svMap, serverID)
			}
		})
		sd.notifyListeners(DEL, server)
	}
}

func (sd *zookeeperServiceDiscovery) printServers() {
	sd.mapByTypeLock.RLock()
	defer sd.mapByTypeLock.RUnlock()
	for k, v := range sd.serverMapByType {
		logger.Log.Debugf("type: %s, servers: %+v", k, v)
	}
}
