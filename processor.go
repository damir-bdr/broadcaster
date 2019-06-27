package broadcast

import "github.com/golang/glog"

type listenerState struct {
	id     int
	synced bool
	data   chan interface{}
}

type syncLsn struct {
	sync bool
}

type msgForAll struct {
	syncLsn
	v interface{}
}

type msgForListener struct {
	syncLsn
	id int
	v  interface{}
}

type msgForListenerAndEveryoneElse struct {
	syncLsn
	id       int
	v, velse interface{}
}

func (br *ChanBroadcaster) processMsgForAll(msg msgForAll) {
	for _, lsn := range br.listeners {
		if msg.sync {
			lsn.synced = true
		}
		br.pushMsg(msg.v, lsn)
	}
}

func (br *ChanBroadcaster) processMsgForListener(msg msgForListener) {
	if lsn, ok := br.listeners[msg.id]; ok {
		if msg.sync {
			lsn.synced = true
		}
		br.pushMsg(msg.v, lsn)
	}
}

func (br *ChanBroadcaster) processMsgForListenerAndEveryoneElse(msg msgForListenerAndEveryoneElse) {
	for id, lsn := range br.listeners {
		val := msg.velse
		if id == msg.id {
			if msg.sync {
				lsn.synced = true
			}
			val = msg.v
		}
		br.pushMsg(val, lsn)
	}
}

func (br *ChanBroadcaster) pushMsg(msg interface{}, lsn *listenerState) {
	if !lsn.synced {
		if glog.V(2) {
			glog.Infof("ChanBroadcaster(%s): skip: msg=%+v, listener_id=%d", br.name, msg, lsn.id)
		}
		return
	}

	select {
	case lsn.data <- msg:
	default:
		glog.Errorf("ChanBroadcaster(%s): unable to send: msg=%+v, listener_id=%d", br.name, msg, lsn.id)
	}
}
