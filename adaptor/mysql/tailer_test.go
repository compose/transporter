package mysql
import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/compose/transporter/client"
)

func checkBinLogReadable(s *sql.DB) error {
	var File string
	var Position int
	var _BinlogDoDB string
	var _BinlogIgnoreDB string
	var _ExecutedGtidSet string
	err := s.QueryRow(`SHOW MASTER STATUS;`).Scan(&File, &Position, &_BinlogDoDB, &_BinlogIgnoreDB, &_ExecutedGtidSet)
	return err
}

var (
	tailerTestData = &TestData{"tailer_test", "tailer_test_table", basicSchema, 10}
)

func TestTailer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping Tailer in short mode")
	}
	dsn := "mysql://root@localhost:3306?%s"
	c, err := NewClient(WithURI(fmt.Sprintf(dsn, tailerTestData.DB)))
	if err != nil {
		t.Fatalf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		t.Fatalf("unable to obtain session to mysql, %s", err)
	}

	if err := checkBinLogReadable(s.(*Session).mysqlSession); err != nil {
		t.Fatalf("unable to query binlog, %s", err)
	}
	time.Sleep(1 * time.Second)

	r := newTailer(dsn)
	readFunc := r.Read(map[string]client.MessageSet{}, func(table string) bool {
		if strings.HasPrefix(table, "information_schema.") ||
			strings.HasPrefix(table, "performance_schema.") ||
			strings.HasPrefix(table, "mysql.") ||
			strings.HasPrefix(table, "sys.") {
			return false
		}
		return table == fmt.Sprintf("%s.%s", tailerTestData.DB, tailerTestData.Table)
	})
	done := make(chan struct{})
	msgChan, err := readFunc(s, done)
	if err != nil {
		t.Fatalf("unexpected Read error, %s\n", err)
	}
	checkCount("initial drain", tailerTestData.InsertCount, msgChan, t)

	for i := 10; i < 20; i++ {
		s.(*Session).mysqlSession.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (
      %d,            -- id
      '%s',          -- colvar VARCHAR(255),
      now() at time zone 'utc' -- coltimestamp TIMESTAMP,
    );`, tailerTestData.Table, i, randomHeros[i%len(randomHeros)]))
	}
	checkCount("tailed data", 10, msgChan, t)

	for i := 10; i < 20; i++ {
		s.(*Session).mysqlSession.Exec(fmt.Sprintf("UPDATE %s SET colvar = 'hello' WHERE id = %d;", tailerTestData.Table, i))
	}
	checkCount("updated data", 10, msgChan, t)

	for i := 10; i < 20; i++ {
		s.(*Session).mysqlSession.Exec(fmt.Sprintf(`DELETE FROM %v WHERE id = %d; `, tailerTestData.Table, i))
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
