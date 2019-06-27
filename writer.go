package broadcast

type Writer interface {
	Write(v interface{}, syncReachedLsn bool)
	WriteForListener(id int, v interface{}, syncReachedLsn bool)
	WriteForListenerAndEveryoneElse(id int, v, velse interface{}, syncReachedLsn bool)
}
