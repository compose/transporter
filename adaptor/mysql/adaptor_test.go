package mysql

import (
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/compose/transporter/log"
)

// Order cols per: https://dev.mysql.com/doc/refman/5.7/en/data-types.html
const (
	basicSchema   = "id INTEGER PRIMARY KEY, colvar VARCHAR(255), coltimestamp TIMESTAMP"
	complexSchema = `id INTEGER AUTO_INCREMENT,
	colinteger INTEGER,
	colsmallint SMALLINT,
	coltinyint TINYINT,
	colmediumint MEDIUMINT,
	colbigint BIGINT,
	coldecimal DECIMAL(8,8),
	colfloat FLOAT(23),
	coldoubleprecision DOUBLE PRECISION,
	colbit BIT(6),
	coldate DATE,
	coltime TIME,
	coltimestamp TIMESTAMP,
	colyear YEAR,
	colchar CHAR,
	colvar VARCHAR(255),
	colbinary BINARY(10),
	colblob BLOB,
	coltext TEXT,
	colpoint POINT,
	collinestring LINESTRING,
	colpolygon POLYGON,
	colgeometrycollection GEOMETRYCOLLECTION,
	PRIMARY KEY (id, colvar)`
)

var (
	defaultTestClient = &Client{
		uri: DefaultURI,
	}
	defaultSession *Session
	dbsToTest      = []*TestData{
		readerTestData,
		readerComplexTestData,
		tailerTestData,
		writerTestData,
		writerComplexTestData,
		writerUpdateTestData,
		writerDeleteTestData,
		writerComplexUpdateTestData,
		writerComplexDeleteTestData,
		writerComplexDeletePkTestData,
	}

	randomHeros = []string{"Superwoman", "Wonder Woman", "Batman", "Superman",
		"Thor", "Iron Man", "Spiderman", "Hulk", "Star-Lord", "Black Widow",
		"Ant\nMan"}

)

type TestData struct {
	DB          string
	Table       string
	Schema      string
	InsertCount int
}

func setup() {
	log.Infoln("setting up tests")
	rand.Seed(time.Now().Unix())

	s, err := defaultTestClient.Connect()
	if err != nil {
		log.Errorf("unable to initialize connection to mysql, %s", err)
		os.Exit(1)
	}
	defaultSession = s.(*Session)
	for _, testData := range dbsToTest {
		if _, err := defaultSession.mysqlSession.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testData.DB)); err != nil {
			log.Errorf("unable to drop database, could affect tests, %s", err)
		}
		if _, err := defaultSession.mysqlSession.Exec(fmt.Sprintf("CREATE DATABASE %s;", testData.DB)); err != nil {
			log.Errorf("unable to create database, could affect tests, %s", err)
		}
		setupData(testData)
	}
}

func setupData(data *TestData) {

	c, err := NewClient(WithURI(fmt.Sprintf("mysql://root@localhost:3306?%s", data.DB)))
	if err != nil {
		log.Errorf("unable to initialize connection to mysql, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		log.Errorf("unable to obtain session to mysql, %s", err)
	}
	mysqlSession := s.(*Session).mysqlSession

	if _, err := mysqlSession.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", data.Table)); err != nil {
		log.Errorf("unable to drop table, could affect tests, %s", err)
	}

	_, err = mysqlSession.Exec(fmt.Sprintf("CREATE TABLE %s ( %s );", data.Table, data.Schema))
	if err != nil {
		log.Errorf("unable to create table, could affect tests, %s", err)
	}

	// cp file to tmp for blob test
	cmd := exec.Command("cp", "logo-mysql-170x115.png" , "/tmp/logo-mysql-170x115.png")
	err = cmd.Run()
	if err != nil {
		log.Errorf("unable to copy blob image, could affect tests, %s", err)
	}
	for i := 0; i < data.InsertCount; i++ {
		if data.Schema == complexSchema {
			if _, err := mysqlSession.Exec(fmt.Sprintf(`
					 INSERT INTO %s VALUES (
							NULL,                                                                                 -- id
							%d,                                                                                   -- colinteger INTEGER,
							32767,                                                                                -- colsmallint SMALLINT,
							127,                                                                                  -- coltinyint TINYINT,
							8388607,                                                                              -- colmediumint MEDIUMINT,
							21474836471,                                                                          -- colbigint BIGINT,
							0.23509838,                                                                           -- coldecimal DECIMAL(8,8),
							0.314259892323,                                                                       -- colfloat FLOAT,
							0.314259892323,                                                                       -- coldoubleprecision DOUBLE PRECISION,
							b'101',                                                                               -- colbit BIT,
							'2021-12-10',                                                                         -- coldate DATE,
							'13:45:00',                                                                           -- coltime TIME,
							now(),                                                                                -- coltimestamp TIMESTAMP,
							'2021',                                                                               -- colyear YEAR,
							'a',                                                                                  -- colchar CHAR,
							'%s',                                                                                 -- colvar VARCHAR(255),
							0xDEADBEEF,                                                                           -- colbinary BINARY,
							LOAD_FILE('/tmp/logo-mysql-170x115.png'),                                             -- colblob BLOB,
							'this is extremely important',                                                        -- coltext TEXT,
							ST_GeomFromText('POINT (15 15)'),                                                     -- colpoint POINT,
							ST_GeomFromText('LINESTRING (0 0,1 1,2 2)'),                                          -- collinestring LINESTRING,
							ST_GeomFromText('POLYGON ((0 0,10 0,10 10,0 10,0 0),(5 5,7 5,7 7,5 7, 5 5))'),        -- colpolygon POLYGON,
							ST_GeomFromText('GEOMETRYCOLLECTION (POINT (1 1),LINESTRING (0 0,1 1,2 2,3 3,4 4))')  -- colgeometrycollection GEOMETRYCOLLECTION,
						);
			`, data.Table, i, randomHeros[i%len(randomHeros)])); err != nil {
				log.Errorf("unexpected Insert error, %s\n", err)
			}
		} else if data.Schema == basicSchema {
			if _, err := mysqlSession.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (
			  %d,            -- id
				'%s',        -- colvar VARCHAR(255),
				now()        -- coltimestamp TIMESTAMP,
			);`, data.Table, i, randomHeros[i%len(randomHeros)])); err != nil {
				log.Errorf("unexpected Insert error, %s\n", err)
			}
		}
	}
}

func TestMain(m *testing.M) {
	setup()
	code := m.Run()
	shutdown()
	os.Exit(code)
}

func shutdown() {
	log.Infoln("shutting down tests")
	defaultSession.mysqlSession.Close()
	log.Infoln("tests shutdown complete")
}
