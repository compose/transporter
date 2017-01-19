package clients

import (
	"sync"

	"github.com/compose/transporter/pkg/client"
	"github.com/compose/transporter/pkg/log"
)

// Closer takes care of receiving on the done channel and then properly cleaning up
// the session and WaitGroup.
func Closer(done chan struct{}, wg *sync.WaitGroup, s client.Session) {
	for {
		select {
		case <-done:
			log.Debugln("received done channel")
			s.Close()
			wg.Done()
			return
		}
	}
}
