package async

import "time"

// Future struct
type Future[T any] struct {
	result *T
	done   chan bool
}

// Async create async function
func Async[T any](f func() T) Future[T] {
	var result T
	done := make(chan bool, 1)

	go func() {
		defer close(done)
		result = f()
		done <- true
	}()

	return Future[T]{
		result: &result,
		done:   done,
	}
}

// Await await future
func (f Future[T]) Await() T {
	<-f.done
	return *f.result
}

// AwaitWithTimeOut await future with timeout
func (f Future[T]) AwaitWithTimeOut(timeout time.Duration) (*T, bool) {
	for {
		select {
		case <-time.After(timeout):
			return nil, false
		case <-f.done:
			return f.result, true
		}
	}
}
