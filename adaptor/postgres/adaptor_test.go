package postgres

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/compose/transporter/log"
)

const (
	basicSchema   = "id SERIAL PRIMARY KEY, colvar VARCHAR(255), coltimestamp TIMESTAMP"
	complexSchema = `id SERIAL,

  colvar VARCHAR(255),
  coltimestamp TIMESTAMP,

  colarrayint integer ARRAY[4],
  colarraystring varchar ARRAY[4],
  colbigint bigint,
  colbigserial bigserial,
  colbit bit,
  colboolean boolean,
  colbox box,
  colbytea bytea,
  colcharacter character,
  colcidr cidr,
  colcircle circle,
  coldate date,
  coldoubleprecision double precision,
  colenum mood,
  colinet inet,
  colinteger integer,
  colinterval interval,
  coljson json,
  colarrayjson json,
  coljsonb jsonb,
  colline line,
  collseg lseg,
  colmacaddr macaddr,
  colmoney money,
  colnumeric numeric(8,8),
  colpath path,
  colpg_lsn pg_lsn,
  colpoint point,
  colpolygon polygon,
  colreal real,
  colserial serial,
  colsmallint smallint,
  colsmallserial smallserial,
  coltext text,
  coltime time,
  coltsquery tsquery,
  coltsvector tsvector,
  coltxid_snapshot txid_snapshot,
  coluuid uuid,
  colxml xml,

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
		log.Errorf("unable to initialize connection to postgresql, %s", err)
		os.Exit(1)
	}
	defaultSession = s.(*Session)
	for _, testData := range dbsToTest {
		if _, err := defaultSession.pqSession.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s;", testData.DB)); err != nil {
			log.Errorf("unable to drop database, could affect tests, %s", err)
		}
		if _, err := defaultSession.pqSession.Exec(fmt.Sprintf("CREATE DATABASE %s;", testData.DB)); err != nil {
			log.Errorf("unable to create database, could affect tests, %s", err)
		}
		setupData(testData)
	}
}

func setupData(data *TestData) {
	c, err := NewClient(WithURI(fmt.Sprintf("postgres://127.0.0.1:5432/%s?sslmode=disable", data.DB)))
	if err != nil {
		log.Errorf("unable to initialize connection to postgres, %s", err)
	}
	defer c.Close()
	s, err := c.Connect()
	if err != nil {
		log.Errorf("unable to obtain session to postgres, %s", err)
	}
	pqSession := s.(*Session).pqSession
	if data.Schema == complexSchema {
		pqSession.Exec("CREATE TYPE mood AS ENUM('sad', 'ok', 'happy');")
	}

	if _, err := pqSession.Exec(fmt.Sprintf("DROP TABLE IF EXISTS %s;", data.Table)); err != nil {
		log.Errorf("unable to drop table, could affect tests, %s", err)
	}

	_, err = pqSession.Exec(fmt.Sprintf("CREATE TABLE %s ( %s );", data.Table, data.Schema))
	if err != nil {
		log.Errorf("unable to create table, could affect tests, %s", err)
	}

	for i := 0; i < data.InsertCount; i++ {
		if data.Schema == complexSchema {
			if _, err := pqSession.Exec(fmt.Sprintf(`
					 INSERT INTO %s VALUES (
							%d,                  -- id
							'%s',           -- colvar VARCHAR(255),
							now() at time zone 'utc', -- coltimestamp TIMESTAMP,

							'{1, 2, 3, 4}',           -- colarrayint ARRAY[4],
							'{"o,ne", "two", "three", "four"}' , -- colarraystring ARRAY[4],
							4000001240124,       -- colbigint bigint,
							DEFAULT,             -- colbigserial bigserial,
							B'1',                -- colbit bit,
							false,               -- colboolean boolean,
							'(10,10),(20,20)',   -- colbox box,
							E'\\xDEADBEEF',      -- colbytea bytea,
							'a',                 -- colcharacter character(1),
							'10.0.1.0/28',       -- colcidr cidr,
							'<(5, 10), 3>',      -- colcircle circle,
							now() at time zone 'utc', -- coldate date,
							0.314259892323,      -- coldoubleprecision double precision,
							'sad',               -- colenum mood,
							'10.0.1.0',          -- colinet inet,
							3,                   -- colinteger integer,
							DEFAULT,             -- autoset colinterval interval,
							'{"name": "batman"}',  -- coljson json,
							'[{"name": "batman"},{"name":"robin"}]',  -- colarrayjson json,
							'{"name": "alfred"}',  -- coljsonb jsonb,
							'{1, 1, 3}',         -- colline line,
							'[(10,10),(25,25)]', -- collseg lseg,
							'08:00:2b:01:02:03', -- colmacaddr macaddr,
							35.68,               -- colmoney money,
							0.23509838,   -- colnumeric numeric(8,8),
							'[(10,10),(20,20),(20,10),(15,15)]', -- colpath path,
							'0/3000000',         -- colpg_lsn pg_lsn,
							'(15,15)',           -- colpoint point,
							'((10,10),(11, 11),(11,0),(5,5))', -- colpolygon polygon,
							7,                   -- colreal real,
							DEFAULT,             -- colserial serial,
							3,                   -- colsmallint smallint,
							DEFAULT,             -- colsmallserial smallserial,
							'this is \n extremely important', -- coltext text,
							'13:45',             -- coltime time,
							'fat:ab & cat',      -- coltsquery tsquery,
							'a fat cat sat on a mat and ate a fat rat', -- coltsvector tsvector,
							null,
							'f0a0da24-4068-4be4-961d-7c295117ccca', -- coluuid uuid,
							'<person><name>Batman</name></person>' --    colxml xml,
						);
			`, data.Table, i, randomHeros[i%len(randomHeros)])); err != nil {
				log.Errorf("unexpected Insert error, %s\n", err)
			}
			// '[{"name": "batman"}, {"name": "robin"}]',  -- arraycoljson json,
		} else if data.Schema == basicSchema {
			if _, err := pqSession.Exec(fmt.Sprintf(`INSERT INTO %s VALUES (
			  %d,            -- id
				'%s',          -- colvar VARCHAR(255),
				now() at time zone 'utc' -- coltimestamp TIMESTAMP,
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
	defaultSession.pqSession.Close()
	log.Infoln("tests shutdown complete")
}
