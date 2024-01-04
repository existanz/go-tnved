package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strings"

	"go-tnved/pkg/utils"

	_ "github.com/mattn/go-sqlite3"
)

const (
	importDir  = "import"
	outputDir  = "output"
	dbFileName = "tnved.db"
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
	db, _ := sql.Open("sqlite3", utils.PathAdd(outputDir, dbFileName))
	initTables(db, tables)
	defer db.Close()

	dir, _ := os.Open(importDir)
	defer dir.Close()
	files, _ := dir.ReadDir(0)
	for _, file := range files {
		fileName := file.Name()
		nextFile, _ := os.Open(utils.PathAdd(importDir, fileName))
		scanner := bufio.NewScanner(nextFile)
		scanner.Scan()
		var version, date string
		utils.Unpack(strings.Split(scanner.Text(), "|"), &version, &date)
		fmt.Printf("File %s, version: %s, date: %s start conversion! \n", fileName, version, date)
		tableName := strings.ToLower(fileName[:len(fileName)-4])
		i := slices.IndexFunc(tables, func(t Table) bool {
			return t.name == tableName
		})
		insert := insertToTable(db, tables[i])
		for scanner.Scan() {
			line := utils.DecodeCP866(scanner.Text())
			rows := utils.StringsToAny(strings.Split(line, "|"))
			insert.Exec(rows...)
		}
		defer insert.Close()
		fmt.Printf("Table %s is filled from file %s \n", tableName, fileName)
	}
	fmt.Println("Finished!")

}

func insertToTable(db *sql.DB, table Table) *sql.Stmt {
	insertQuery := "INSERT INTO " + table.name + " ("
	values := ") VALUES ("
	for _, field := range table.fields {
		insertQuery += field.name + ", "
		values += "?, "
	}
	insertQuery = insertQuery[:len(insertQuery)-2] + values[:len(values)-2] + ")"
	stmt, _ := db.Prepare(insertQuery)
	return stmt
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
