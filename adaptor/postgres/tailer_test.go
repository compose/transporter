package postgres

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/client"
)

func addTestReplicationSlot(s *sql.DB) error {
	_, err := s.Exec(`
    SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
  `)
	return err
}

func dropTestReplicationSlot(s *sql.DB) error {
	_, err := s.Exec(`
    SELECT * FROM pg_drop_replication_slot('test_slot');
  `)
	return err
}

var (
	tailerTestData = &TestData{"tailer_test", "tailer_test_table", basicSchema, 10}
)

func TestTailer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Tailer in short mode")
	}
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", tailerTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to postgres, %s", err)
	}

	dropTestReplicationSlot(s.(*Session).pqSession)
	if err := addTestReplicationSlot(s.(*Session).pqSession); err != nil {
		t.Fatalf("unable to create replication slot, %s", err)
	}
	time.Sleep(1 * time.Second)

	r := newTailer("test_slot")
	readFunc := r.Read(map[string]client.MessageSet{}, func(table string) bool {
		if strings.HasPrefix(table, "information_schema.") || strings.HasPrefix(table, "pg_catalog.") {
			return false
		}
		return table == fmt.Sprintf("public.%s", tailerTestData.Table)
	})
	done := make(chan struct{})
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	checkCount("initial drain", tailerTestData.InsertCount, msgChan, t)

	for i := 10; i < 20; i++ {
		s.(*Session).pqSession.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (
      %d,            -- id
      '%s',          -- colvar VARCHAR(255),
      now() at time zone 'utc' -- coltimestamp TIMESTAMP,
    );`, tailerTestData.Table, i, randomHeros[i%len(randomHeros)]))
	}
	checkCount("tailed data", 10, msgChan, t)

	for i := 10; i < 20; i++ {
		s.(*Session).pqSession.Exec(fmt.Sprintf("UPDATE %s SET colvar = 'hello' WHERE id = %d;", tailerTestData.Table, i))
	}
	checkCount("updated data", 10, msgChan, t)

	for i := 10; i < 20; i++ {
		s.(*Session).pqSession.Exec(fmt.Sprintf(`DELETE FROM %v WHERE id = %d; `, tailerTestData.Table, i))
	}

	checkCount("deleted data", 10, msgChan, t)

	close(done)
}

func checkCount(desc string, expected int, msgChan <-chan client.MessageSet, t *testing.T) {
	var numMsgs int
	var wg sync.WaitGroup
	wg.Add(1)
	go func(wg *sync.WaitGroup) {
		for {
			select {
			case <-msgChan:
				numMsgs++
			case <-time.After(1 * time.Second):
				if numMsgs == expected {
					wg.Done()
					return
				}
			case <-time.After(20 * time.Second):
				wg.Done()
				return
			}
		}
	}(&wg)
	wg.Wait()
	if numMsgs != expected {
		t.Errorf("[%s] bad message count, expected %d, got %d\n", desc, expected, numMsgs)
	}
}
