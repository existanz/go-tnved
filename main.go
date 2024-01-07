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
	defer db.Close()
	var command string
	fmt.Scan(&command)
	if command == "convert" {

		initTables(db, tables)

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
	} else if command == "export" {
		createJson(db)
	}
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

type razdelRow struct {
	razdel string  `json:"razdel"`
	naim   string  `json:"naim"`
	data   string  `json:"data"`
	groups []group `json:"groups"`
}

type group struct {
	gruppa string  `json:"gruppa"`
	naim   string  `json:"naim"`
	data   string  `json:"data"`
	tovars []tovar `json:"tovars"`
}

type tovar struct {
	tovPoz string `json:"tovpoz"`
	naim   string `json:"naim"`
	data   string `json:"date"`
	subs   []sub  `json:"subs"`
}

type sub struct {
	subPoz string `json:"subpoz"`
	naim   string `json:"naim"`
	data   string `json:"date"`
}

func createJson(db *sql.DB) {
	var rows []razdelRow
	selectRazdel, _ := db.Query("SELECT RAZDEL, NAIM, DATA FROM tnved1 WHERE DATA1 = ''")
	for selectRazdel.Next() {
		var row razdelRow
		_ = selectRazdel.Scan(&row.razdel, &row.naim, &row.data)
		rows = append(rows, row)
	}
	defer selectRazdel.Close()
	for i, r := range rows {
		selectGroup, _ := db.Query("SELECT GRUPPA, NAIM, DATA FROM tnved2 WHERE DATA1 = '' AND RAZDEL = ?", r.razdel)
		for selectGroup.Next() {
			var row group
			_ = selectGroup.Scan(&row.gruppa, &row.naim, &row.data)
			rows[i].groups = append(rows[i].groups, row)
		}
	}
	for i, r := range rows {
		for j, g := range r.groups {
			selectTovar, _ := db.Query("SELECT TOV_POZ, NAIM, DATA FROM tnved3 WHERE DATA1 = '' AND GRUPPA = ?", g.gruppa)
			for selectTovar.Next() {
				var row tovar
				_ = selectTovar.Scan(&row.tovPoz, &row.naim, &row.data)
				rows[i].groups[j].tovars = append(rows[i].groups[j].tovars, row)
			}
		}
	}

	for i, r := range rows {
		for j, g := range r.groups {
			for k, t := range g.tovars {
				selectSub, err := db.Query("SELECT SUB_POZ, KR_NAIM, DATA FROM tnved4 WHERE DATA1 = '' AND GRUPPA = ? AND TOV_POZ = ?", g.gruppa, t.tovPoz)
				if err != nil {
					fmt.Println(err.Error())
				}
				for selectSub.Next() {
					var row sub
					_ = selectSub.Scan(&row.subPoz, &row.naim, &row.data)
					rows[i].groups[j].tovars[k].subs = append(rows[i].groups[j].tovars[k].subs, row)
				}
			}
		}
	}

	fmt.Println(rows[0])
	defer selectRazdel.Close()
}
