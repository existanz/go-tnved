package main

import (
	"bufio"
	"bytes"
	"database/sql"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/text/encoding/charmap"
	"golang.org/x/text/transform"
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
	db, _ := sql.Open("sqlite3", pathAdd(outputDir, dbFileName))
	initTables(db, tables)
	defer db.Close()

	dir, _ := os.Open(importDir)
	defer dir.Close()
	files, _ := dir.ReadDir(0)
	for _, file := range files {
		fileName := file.Name()
		nextFile, _ := os.Open(pathAdd(importDir, fileName))
		scanner := bufio.NewScanner(nextFile)
		scanner.Scan()
		var version, date string
		unpack(strings.Split(scanner.Text(), "|"), &version, &date)
		fmt.Printf("File %s, version: %s, date: %s start conversion! \n", fileName, version, date)
		tableName := strings.ToLower(fileName[:len(fileName)-4])
		i := slices.IndexFunc(tables, func(t Table) bool {
			return t.name == tableName
		})
		insert := insertToTable(db, tables[i])
		for scanner.Scan() {
			line := DecodeCP866(scanner.Text())
			rows := stringsToAny(strings.Split(line, "|"))
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

func unpack(s []string, vars ...*string) {
	for i := range vars {
		*vars[i] = s[i]
	}
}

func pathAdd(paths ...string) string {
	var path string = "."
	for _, point := range paths {
		path += "/" + point
	}
	return path
}

func DecodeCP866(s string) string {
	reader := transform.NewReader(bytes.NewReader([]byte(s)), charmap.CodePage866.NewDecoder())
	res, _ := io.ReadAll(reader)
	return string(res)
}

func stringsToAny(strings []string) []any {
	res := make([]any, len(strings))
	for i, str := range strings {
		res[i] = str
	}
	return res[:len(res)-1]
}
