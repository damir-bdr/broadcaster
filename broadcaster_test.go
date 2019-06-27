package broadcast

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func listen(t *testing.T, lsn Listener, startVal int, wg *sync.WaitGroup) {
	fmt.Println("listen start. lsnId: ", lsn.Id())
	i := startVal
	for v := range lsn.GetChan() {
		fmt.Println("listen chan Id: ", lsn.Id(), v)
		assert.Equal(t, i, v)
		i++
	}
	assert.Equal(t, startVal+10, i)
	wg.Done()
	fmt.Println("listen done. lsnId: ", lsn.Id())
}

func checkTimeoutTriggered(t *testing.T, lsn Listener, d time.Duration, wg *sync.WaitGroup) {
	fmt.Printf("checkTimeoutTriggered start. lsnId=%d duration=%v\n", lsn.Id(), d)

	select {
	case <-time.After(d):
		fmt.Println("Timeout is out!")
		wg.Done()
	case r, ok := <-lsn.GetChan():
		fmt.Printf("Received r: %v ok: %t", r, ok)
		t.Fail()
	}

	fmt.Printf("checkTimeoutTriggered done. lsnId=%d duration=%v\n", lsn.Id(), d)
}
func TestSeveralListeners(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")
	var wg sync.WaitGroup

	l1 := b.AddSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)

	l2 := b.AddSyncedListener()
	assert.NotEqual(t, l2.Id(), l1.Id())
	wg.Add(1)
	go listen(t, l2, 0, &wg)

	l3 := b.AddSyncedListener()
	assert.NotEqual(t, l3.Id(), l2.Id())
	wg.Add(1)
	go listen(t, l3, 0, &wg)

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, false)
	}

	l1.Close()
	l2.Close()
	l3.Close()

	wg.Wait()
}

func TestSyncListenerFnWriteFalse(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")
	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, true)
	}

	l1.Close()
	wg.Wait()
}

func TestSyncListenerFnWriteTrue(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")
	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go checkTimeoutTriggered(t, l1, 20*time.Millisecond, &wg)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, false)
	}

	wg.Wait()
	l1.Close()
}

func TestMsgForListener(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)

	l2 := b.AddSyncedListener()
	assert.NotEqual(t, l2.Id(), l1.Id())
	wg.Add(1)
	go checkTimeoutTriggered(t, l2, 20*time.Millisecond, &wg)

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.WriteForListener(l1.Id(), i, false)
	}

	l1.Close()
	wg.Wait()
}

func TestMsgForListenerWriteTrue(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.WriteForListener(l1.Id(), i, true)
	}

	l1.Close()
	wg.Wait()
}

func TestMsgForListenerWriteFalse(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go checkTimeoutTriggered(t, l1, 20*time.Millisecond, &wg)
	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.WriteForListener(l1.Id(), i, false)
	}

	wg.Wait()
	l1.Close()
}

func TestWriteForListenerAndEveryoneElse(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)

	l2 := b.AddSyncedListener()
	assert.NotEqual(t, l2.Id(), l1.Id())
	wg.Add(1)
	go listen(t, l2, 0, &wg)

	b.WriteForListenerAndEveryoneElse(l1.Id(), 0, 0, true)

	for i := 1; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, false)
	}

	l1.Close()
	l2.Close()

	wg.Wait()
}

func TestWriteForListenerAndEveryoneElseWriteFalse(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go checkTimeoutTriggered(t, l1, 20*time.Millisecond, &wg)

	l2 := b.AddSyncedListener()
	assert.NotEqual(t, l2.Id(), l1.Id())
	wg.Add(1)
	go listen(t, l2, 0, &wg)

	b.WriteForListenerAndEveryoneElse(l1.Id(), 0, 0, false)

	for i := 1; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, false)
	}

	l2.Close()
	wg.Wait()
	l1.Close()
}

func TestWriteForListenerAndEveryoneElse2(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddNotSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)

	l2 := b.AddNotSyncedListener()
	assert.NotEqual(t, l2.Id(), l1.Id())
	wg.Add(1)
	go checkTimeoutTriggered(t, l2, 50*time.Millisecond, &wg)

	b.WriteForListenerAndEveryoneElse(l1.Id(), 0, 0, true)

	for i := 1; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, false)
	}

	l1.Close()
	wg.Wait()
	l2.Close()
}

func TestBroadcastClose(t *testing.T) {
	ctx, cancelFn := context.WithCancel(context.Background())
	defer cancelFn()

	b := NewChanBroadcaster(ctx, 0, 1, "rpc")

	var wg sync.WaitGroup

	l1 := b.AddSyncedListener()
	wg.Add(1)
	go listen(t, l1, 0, &wg)

	for i := 0; i < 10; i++ {
		time.Sleep(time.Millisecond)
		b.Write(i, false)
	}

	b.Close()
	wg.Wait()
}
