package rethinkdb

import (
	"errors"
	"sync"

	"github.com/compose/transporter/client"
	"github.com/compose/transporter/log"
	"github.com/compose/transporter/message"
	"github.com/compose/transporter/message/ops"

	re "gopkg.in/gorethink/gorethink.v3"
)

var (
	_ client.Reader = &Reader{}
)

// Reader fulfills the client.Reader interface for use with both copying and tailing a RethinkDB
// database.
type Reader struct {
	tail bool
}

func newReader(tail bool) client.Reader {
	return &Reader{tail}
}

type iterationComplete struct {
	cursor *re.Cursor
	table  string
}

func (r *Reader) Read(filterFn client.NsFilterFunc) client.MessageChanFunc {
	return func(s client.Session, done chan struct{}) (chan message.Msg, error) {
		out := make(chan message.Msg)
		session := s.(*Session).session
		go func() {
			defer close(out)
			log.With("db", session.Database()).Infoln("starting Read func")
			tables, err := r.listTables(session, filterFn)
			if err != nil {
				log.With("db", session.Database()).Errorf("unable to list tables, %s", err)
				return
			}
			iterationComplete := r.iterateTable(session, tables, out, done)
			var wg sync.WaitGroup
			func() {
				for {
					select {
					case <-done:
						return
					case i, ok := <-iterationComplete:
						if !ok {
							return
						}
						log.With("db", session.Database()).With("table", i.table).Infoln("iterating complete")
						if i.cursor != nil {
							go func(wg *sync.WaitGroup, t string, c *re.Cursor) {
								wg.Add(1)
								defer wg.Done()
								errc := r.sendChanges(session.Database(), t, c, out, done)
								for err := range errc {
									log.With("db", session.Database()).With("table", t).Errorln(err)
									return
								}
							}(&wg, i.table, i.cursor)
						}
					}
				}
			}()
			log.With("db", session.Database()).Infoln("Read completed")
			// this will block if we're tailing
			wg.Wait()
			return
		}()
		return out, nil
	}
}

func (r *Reader) listTables(session *re.Session, filterFn func(name string) bool) (<-chan string, error) {
	out := make(chan string)
	tables, err := re.DB(session.Database()).TableList().Run(session)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(out)
		defer tables.Close()
		var table string
		for tables.Next(&table) {
			if filterFn(table) {
				log.With("db", session.Database()).With("table", table).Infoln("sending for iteration...")
				out <- table
			} else {
				log.With("db", session.Database()).With("table", table).Infoln("skipping iteration...")
			}
		}
		log.With("db", session.Database()).Infoln("done iterating tables")
	}()
	return out, nil
}

func (r *Reader) iterateTable(session *re.Session, in <-chan string, out chan<- message.Msg, done chan struct{}) <-chan iterationComplete {
	tableDone := make(chan iterationComplete)
	go func() {
		defer close(tableDone)
		for {
			select {
			case t, ok := <-in:
				if !ok {
					return
				}
				log.With("db", session.Database()).With("table", t).Infoln("iterating...")
				cursor, err := re.DB(session.Database()).Table(t).Run(session)
				if err != nil {
					return
				}
				defer cursor.Close()
				var ccursor *re.Cursor
				if r.tail {
					var err error
					ccursor, err = re.DB(session.Database()).Table(t).Changes(re.ChangesOpts{}).Run(session)
					if err != nil {
						return
					}
				}

				var result map[string]interface{}
				for cursor.Next(&result) {
					out <- message.From(ops.Insert, t, result)
					result = map[string]interface{}{}
				}

				if err := cursor.Err(); err != nil {
					return
				}
				tableDone <- iterationComplete{ccursor, t}
			case <-done:
				log.With("db", session.Database()).Infoln("iterating no more")
				return
			}
		}
	}()
	return tableDone
}

type rethinkDbChangeNotification struct {
	Error  string                 `gorethink:"error"`
	OldVal map[string]interface{} `gorethink:"old_val"`
	NewVal map[string]interface{} `gorethink:"new_val"`
}

func (r *Reader) sendChanges(db, table string, ccursor *re.Cursor, out chan<- message.Msg, done chan struct{}) chan error {
	errc := make(chan error)
	go func() {
		defer ccursor.Close()
		defer close(errc)
		changes := make(chan rethinkDbChangeNotification)
		ccursor.Listen(changes)
		log.With("db", db).With("table", table).Debugln("starting changes feed...")
		for {
			if err := ccursor.Err(); err != nil {
				errc <- err
				return
			}
			select {
			case <-done:
				log.With("db", db).With("table", table).Infoln("stopping changes...")
				return
			case change := <-changes:
				if done == nil {
					log.With("db", db).With("table", table).Infoln("stopping changes...")
					return
				}
				log.With("db", db).With("table", table).With("change", change).Debugln("received")

				if change.Error != "" {
					errc <- errors.New(change.Error)
				} else if change.OldVal != nil && change.NewVal != nil {
					out <- message.From(ops.Update, table, change.NewVal)
				} else if change.NewVal != nil {
					out <- message.From(ops.Insert, table, change.NewVal)
				} else if change.OldVal != nil {
					out <- message.From(ops.Delete, table, change.OldVal)
				}
			}
		}
	}()
	return errc
}
