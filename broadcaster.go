package broadcast

import (
	"context"
	"sync/atomic"

	"github.com/golang/glog"
)

type ChanBroadcaster struct {
	idCounter uint32
	name      string

	controlChan chan controlMsg
	dataChan    chan interface{}

	listenerChanCapacity int
	listeners            map[int]*listenerState
}

func NewChanBroadcaster(ctx context.Context, capacity, listenerChanCapacity int, name string) *ChanBroadcaster {
	br := &ChanBroadcaster{
		name:                 name,
		controlChan:          make(chan controlMsg),
		dataChan:             make(chan interface{}, capacity),
		listenerChanCapacity: listenerChanCapacity,
		listeners:            make(map[int]*listenerState),
	}

	go br.run(ctx)

	return br
}

func (br *ChanBroadcaster) Write(v interface{}, syncReachedLsn bool) {
	select {
	case br.dataChan <- msgForAll{syncLsn: syncLsn{sync: syncReachedLsn},
		v: v,
	}:

	default:
		glog.Errorf("ChanBroadcaster(%s): unable to write(forAll), value=%v", br.name, v)
	}
}

func (br *ChanBroadcaster) WriteForListener(id int, v interface{}, syncReachedLsn bool) {
	select {
	case br.dataChan <- msgForListener{syncLsn: syncLsn{sync: syncReachedLsn},
		id: id,
		v:  v,
	}:
	default:
		glog.Errorf("ChanBroadcaster(%s): unable to write(listener), value=%v", br.name, v)
	}
}

func (br *ChanBroadcaster) WriteForListenerAndEveryoneElse(id int, v, velse interface{}, syncReachedLsn bool) {
	select {
	case br.dataChan <- msgForListenerAndEveryoneElse{syncLsn: syncLsn{sync: syncReachedLsn},
		id:    id,
		v:     v,
		velse: velse,
	}:
	default:
		glog.Errorf("ChanBroadcaster(%s): unable to write(listenerAndEveryoneElse), value=%v", br.name, v)
	}
}

func (br *ChanBroadcaster) AddSyncedListener() Listener {
	return br.addListener(true)
}

func (br *ChanBroadcaster) AddNotSyncedListener() Listener {
	return br.addListener(false)
}

func (br *ChanBroadcaster) Close() {
	backNotifyChan := make(chan struct{}, 0)
	br.controlChan <- controlMsg{cmd: closeCmd, backNotifyChan: backNotifyChan}
	<-backNotifyChan
}

func (br *ChanBroadcaster) addListener(needSync bool) Listener {
	id := int(atomic.AddUint32(&br.idCounter, 1))
	c := make(chan interface{}, br.listenerChanCapacity)
	backNotifyChan := make(chan struct{}, 0)

	br.controlChan <- controlMsg{cmd: addListenerCmd, id: id, c: c, needSync: needSync, backNotifyChan: backNotifyChan}
	<-backNotifyChan

	return &broadcastListener{c: c, id: id, br: br}
}

func (br *ChanBroadcaster) removeListener(id int) {
	backNotifyChan := make(chan struct{}, 0)
	br.controlChan <- controlMsg{cmd: removeListenerCmd, id: id, backNotifyChan: backNotifyChan}
	<-backNotifyChan
}

func (br *ChanBroadcaster) run(ctx context.Context) {
Loop:
	for {
		select {
		case <-ctx.Done():
			glog.Warningf("ChanBroadcaster(%s): ctx is closed: %v", br.name, ctx.Err())
			br.Close()

		case v, ok := <-br.dataChan:
			if !ok {
				if glog.V(2) {
					glog.Infof("ChanBroadcaster(%s): data channel is closed", br.name)
				}
				break Loop
			}

			switch msg := v.(type) {
			case msgForAll:
				br.processMsgForAll(msg)
			case msgForListener:
				br.processMsgForListener(msg)
			case msgForListenerAndEveryoneElse:
				br.processMsgForListenerAndEveryoneElse(msg)
			}

		case msg := <-br.controlChan:
			switch msg.cmd {
			case addListenerCmd:
				br.processAddListener(msg)
			case removeListenerCmd:
				br.processDelListener(msg)
			case closeCmd:
				br.processClose(msg)
			}
		}
	}
}
