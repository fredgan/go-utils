package nosql

import (
	"sync"

	sync2 "github.com/fredgan/go-utils/sync"
)

type Fn func() error

func Concurrency(fns []Fn, maxIdle int64) error {
	idleChan := make(chan bool, maxIdle)
	errChan := make(chan error, len(fns))

	defer func() {
		close(idleChan)
		close(errChan)
	}()

	wg := sync.WaitGroup{}

	var isExit sync2.AtomicBool
	isExit.Set(false)

	for _, fn := range fns {
		f := fn
		wg.Add(1)

		go func() {
			idleChan <- true
			defer func() {
				<-idleChan
				wg.Done()
			}()

			if isExit.Get() {
				return
			}

			err := f()
			if err != nil {
				isExit.Set(true)
				errChan <- err
			}
		}()
	}

	wg.Wait()

	if len(errChan) > 0 {
		return <-errChan
	}

	return nil
}
