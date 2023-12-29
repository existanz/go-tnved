package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	fmt.Println("Start!")
	db, _ := sql.Open("sqlite3", "./output/tnved.db")
	createTables(db)
	fmt.Println("Finished!")
	db.Close()

}

func createTables(db *sql.DB) {
	create, _ := db.Prepare("CREATE TABLE IF NOT EXISTS tnved1 (RAZDEL CHARACTER(20), NAIM CHARACTER(4000), PRIM CHARACTER(4000), DATA DATE, DATA1 DATE)")
	create.Exec()
}
