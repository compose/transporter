package main

import (
		"log"
		"database/sql"
		//_ "github.com/lib/pq"
		_ "github.com/go-sql-driver/mysql"
	)

//var db *sql.DB

func main() {
	//db, _ := sql.Open("postgres", "postgres://127.0.0.1:5432?sslmode=disable")
	db, err := sql.Open("mysql", "/")
	if err != nil {
		panic(err.Error())
	}
	defer db.Close()
	db.SetMaxOpenConns(10)
	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}
}
