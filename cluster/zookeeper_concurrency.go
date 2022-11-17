package cluster

import (
	"encoding/json"
	"sync"

	"github.com/topfreegames/pitaya/v2/logger"
)

//
func newConcurrency(sd *zookeeperServiceDiscovery) *concurrency {
	c := &concurrency{
		sd:            sd,
		numberWorkers: 10,
		wg:            new(sync.WaitGroup),
		result:        new([]*Server),
		workChan:      make(chan concurrencyWork),
	}

	c.start()
	return c
}

type concurrencyWork struct {
	serverPath string
	payload    []byte
}

type concurrency struct {
	sd            *zookeeperServiceDiscovery
	numberWorkers int
	wg            *sync.WaitGroup
	resultMutex   sync.Mutex
	result        *[]*Server
	workChan      chan concurrencyWork
}

func (c *concurrency) start() {
	for i := 0; i < c.numberWorkers; i++ {
		go func() {
			for work := range c.workChan {
				logger.Log.Debugf("load info from zookeeper: %s", work.serverPath)
				data, _, err := c.sd.conn.Get(work.serverPath)
				if err != nil {
					logger.Log.Errorf("error load info from zookeeper, server: %s, error: %s", work.serverPath, err.Error())
					c.wg.Done()
					continue
				}
				// parse data
				var s *Server
				json.Unmarshal(data, &s)

				c.resultMutex.Lock()
				*c.result = append(*c.result, s)
				c.resultMutex.Unlock()
				c.wg.Done()
			}
		}()
	}
}

func (c *concurrency) addWork(serverPath string) {
	c.wg.Add(1)
	c.workChan <- concurrencyWork{
		serverPath: serverPath,
	}
}

func (c *concurrency) waitAndGetResult() []*Server {
	c.wg.Wait()
	close(c.workChan)
	return *c.result
}
