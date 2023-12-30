package main

import (
	"database/sql"
	"fmt"

	_ "github.com/mattn/go-sqlite3"
)

type TCol struct {
	name   string
	dbType string
}

type Table struct {
	name   string
	desc   string
	fields []TCol
}

// tnved tables
var tables []Table = []Table{
	{
		name: "tnved1",
		desc: "Разделы",
		fields: []TCol{
			{name: "RAZDEL", dbType: "CHARACTER(2)"},
			{name: "NAIM", dbType: "CHARACTER(4000)"},
			{name: "PRIM", dbType: "CHARACTER(4000)"},
			{name: "DATA", dbType: "DATE"},
			{name: "DATA1", dbType: "DATE"},
		},
	},
	{
		name: "tnved2",
		desc: "Группы",
		fields: []TCol{
			{name: "RAZDEL", dbType: "CHARACTER(2)"},
			{name: "GRUPPA", dbType: "CHARACTER(2)"},
			{name: "NAIM", dbType: "CHARACTER(4000)"},
			{name: "PRIM", dbType: "CHARACTER(4000)"},
			{name: "DATA", dbType: "DATE"},
			{name: "DATA1", dbType: "DATE"},
		},
	},
	{
		name: "tnved3",
		desc: "Товарные позиции",
		fields: []TCol{
			{name: "GRUPPA", dbType: "CHARACTER(2)"},
			{name: "TOV_POZ", dbType: "CHARACTER(2)"},
			{name: "NAIM", dbType: "CHARACTER(4000)"},
			{name: "DATA", dbType: "DATE"},
			{name: "DATA1", dbType: "DATE"},
		},
	},
	{
		name: "tnved4",
		desc: "Товарные подпозиции",
		fields: []TCol{
			{name: "GRUPPA", dbType: "CHARACTER(2)"},
			{name: "TOV_POZ", dbType: "CHARACTER(2)"},
			{name: "SUB_POZ", dbType: "CHARACTER(6)"},
			{name: "KR_NAIM", dbType: "CHARACTER(200)"},
			{name: "DATA", dbType: "DATE"},
			{name: "DATA1", dbType: "DATE"},
		},
	},
}

func main() {
	fmt.Println("Start!")
	db, _ := sql.Open("sqlite3", "./output/tnved.db")
	defer db.Close()
	initTables(db, tables)
	fmt.Println("Finished!")

}

func initTables(db *sql.DB, tables []Table) {
	for _, table := range tables {
		createQuery := "CREATE TABLE IF NOT EXISTS " + table.name + " ("
		for _, field := range table.fields {
			createQuery += field.name + " " + field.dbType + ", "
		}
		createQuery = createQuery[:len(createQuery)-2] + ")"
		deleteQuery := "DELETE FROM " + table.name
		fmt.Println("Create table " + table.name + " - " + table.desc)
		create, _ := db.Prepare(createQuery)
		create.Exec()
		create.Close()
		delete, _ := db.Prepare(deleteQuery)
		delete.Exec()
		delete.Close()
		fmt.Println("Table " + table.name + " successfuly initialized")
	}
}

func unpack(s []string, vars ...*string) {
	for i := range vars {
		*vars[i] = s[i]
	}
}
