package broadcast

import "github.com/golang/glog"

const (
	addListenerCmd = iota + 1
	removeListenerCmd
	closeCmd
)

type controlMsg struct {
	cmd            int
	id             int
	c              chan interface{}
	needSync       bool
	backNotifyChan chan struct{}
}

func (br *ChanBroadcaster) processAddListener(msg controlMsg) {
	if glog.V(2) {
		glog.Infof("ChanBroadcaster(%s): add listener with id=%d need_sync=%t", br.name, msg.id, msg.needSync)
	}

	br.listeners[msg.id] = &listenerState{id: msg.id, synced: msg.needSync, data: msg.c}
	close(msg.backNotifyChan)
}

func (br *ChanBroadcaster) processDelListener(msg controlMsg) {
	if glog.V(2) {
		glog.Infof("ChanBroadcaster(%s): del listener with id=%d", br.name, msg.id)
	}

	br.delListener(msg.id)
	close(msg.backNotifyChan)
}

func (br *ChanBroadcaster) processClose(msg controlMsg) {
	for id := range br.listeners {
		br.delListener(id)
	}

	close(br.dataChan)
	close(br.controlChan)
	close(msg.backNotifyChan)
}

func (br *ChanBroadcaster) delListener(id int) {
	lsn, ok := br.listeners[id]
	if !ok {
		glog.Errorf("ChanBroadcaster(%s): listener with id=%d not found", br.name, id)
		return
	}

	if glog.V(2) {
		glog.Infof("ChanBroadcaster(%s): remove listener with id=%d", br.name, id)
	}

	close(lsn.data)
	delete(br.listeners, id)
}
