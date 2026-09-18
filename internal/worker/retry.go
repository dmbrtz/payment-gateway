package worker

import "time"

func retry(operation func() error, attempts int, delay time.Duration, isRetryable func(error) bool) error {
	var err error
	for i := 0; i < attempts; i++ {
		err = operation()
		if err != nil {
			if i < attempts-1 {
				if isRetryable(err) {
					time.Sleep(delay)
					continue
				} else {
					return err
				}
			}
		} else {
			return nil
		}
	}
	return err
}
