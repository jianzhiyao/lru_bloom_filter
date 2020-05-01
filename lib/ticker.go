package lib

import (
	"log"
	"time"
)

func Every(duration time.Duration, work func(time.Time) bool) chan bool {

	ticker := time.NewTicker(duration)
	stop := make(chan bool, 1)
	go func() {
		defer log.Println("ticker stopped")
		for {
			select {
			case t := <-ticker.C:
				if !work(t) {
					stop <- true
				}
			case <-stop:
				return
			}
		}
	}()

	return stop
}
