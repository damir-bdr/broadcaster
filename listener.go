package broadcast

type Listener interface {
	GetChan() <-chan interface{}
	Id() int
	Close()
}

type broadcastListener struct {
	c  <-chan interface{}
	id int
	br *ChanBroadcaster
}

func (l *broadcastListener) GetChan() <-chan interface{} {
	return l.c
}

func (l *broadcastListener) Id() int {
	return l.id
}

func (l *broadcastListener) Close() {
	l.br.removeListener(l.id)
}
