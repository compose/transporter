package postgres

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/message"
)

func addTestReplicationSlot() error {
	_, err := defaultSession.pqSession.Exec(`
    SELECT * FROM pg_create_logical_replication_slot('test_slot', 'test_decoding');
  `)
	return err
}

func dropTestReplicationSlot() error {
	_, err := defaultSession.pqSession.Exec(`
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
	dropTestReplicationSlot()
	if err := addTestReplicationSlot(); err != nil {
		t.Fatalf("unable to create replication slot, %s", err)
	}
	time.Sleep(1 * time.Second)

	r := newTailer(tailerTestData.DB, "test_slot")
	readFunc := r.Read(func(table string) bool {
		if strings.HasPrefix(table, "information_schema.") || strings.HasPrefix(table, "pg_catalog.") {
			return false
		}
		return table == fmt.Sprintf("public.%s", tailerTestData.Table)
	})
	done := make(chan struct{})
	msgChan, err := readFunc(defaultSession, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	checkCount("initial drain", tailerTestData.InsertCount, msgChan, t)

	for i := 10; i < 20; i++ {
		defaultSession.pqSession.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (
      %d,            -- id
      '%s',          -- colvar VARCHAR(255),
      now() at time zone 'utc' -- coltimestamp TIMESTAMP,
    );`, tailerTestData.Table, i, randomHeros[i%len(randomHeros)]))
	}
	checkCount("tailed data", 10, msgChan, t)

	for i := 10; i < 20; i++ {
		defaultSession.pqSession.Exec(fmt.Sprintf("UPDATE %s SET colvar = 'hello' WHERE id = %d;", tailerTestData.Table, i))
	}
	checkCount("updated data", 10, msgChan, t)

	for i := 10; i < 20; i++ {
		defaultSession.pqSession.Exec(fmt.Sprintf(`DELETE FROM %v WHERE id = %d; `, tailerTestData.Table, i))
	}

	checkCount("deleted data", 10, msgChan, t)

	close(done)
}

func checkCount(desc string, expected int, msgChan <-chan message.Msg, t *testing.T) {
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
