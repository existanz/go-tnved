package main

import (
	"bufio"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"slices"
	"strings"

	"go-tnved/pkg/utils"

	"github.com/existanz/gomenu"

	_ "github.com/mattn/go-sqlite3"
)

const (
	importDir    = "import"
	outputDir    = "output"
	dbFileName   = "tnved.db"
	jsonFileName = "tnved.json"
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

var menuItems []*gomenu.MenuItem = []*gomenu.MenuItem{
	{Label: "Load to DB", ID: "load"},
	{Label: "Export to json", ID: "export"},
}

func main() {
	m := gomenu.NewMenu("Choose action")
	m.Items = menuItems
	db, _ := sql.Open("sqlite3", utils.PathAdd(outputDir, dbFileName))
	defer db.Close()
	command := m.Load()
	if command == "load" {
		loadToDB(db)
	} else if command == "export" {
		tnved := selectTNVED(db)
		saveToJSON(tnved)
	}
}

func loadToDB(db *sql.DB) {
	initTables(db, tables)

	dir, _ := os.Open(importDir)
	defer dir.Close()
	files, _ := dir.ReadDir(0)
	for _, file := range files {
		fileName := file.Name()
		processFile(db, fileName)
	}
	fmt.Println("Finished!")
}

func processFile(db *sql.DB, fileName string) {
	nextFile, _ := os.Open(utils.PathAdd(importDir, fileName))
	defer nextFile.Close()
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
	Razdel string  `json:"chapter"`
	Naim   string  `json:"naim"`
	Data   string  `json:"date"`
	Groups []group `json:"groups"`
}

type group struct {
	Gruppa string  `json:"code"`
	Naim   string  `json:"naim"`
	Data   string  `json:"date"`
	Tovars []tovar `json:"goods"`
}

type tovar struct {
	TovPoz string `json:"code"`
	Naim   string `json:"naim"`
	Data   string `json:"date"`
	Subs   []sub  `json:"subs"`
}

type sub struct {
	SubPoz string `json:"code"`
	Naim   string `json:"naim"`
	Data   string `json:"date"`
}

func selectTNVED(db *sql.DB) []razdelRow {
	var rows []razdelRow
	selectRazdel, _ := db.Query("SELECT RAZDEL, NAIM, DATA FROM tnved1 WHERE DATA1 = ''")
	for selectRazdel.Next() {
		var row razdelRow
		_ = selectRazdel.Scan(&row.Razdel, &row.Naim, &row.Data)
		rows = append(rows, row)
	}
	defer selectRazdel.Close()
	for i, r := range rows {
		selectGroup, _ := db.Query("SELECT GRUPPA, NAIM, DATA FROM tnved2 WHERE DATA1 = '' AND RAZDEL = ?", r.Razdel)
		defer selectGroup.Close()
		for selectGroup.Next() {
			var row group
			_ = selectGroup.Scan(&row.Gruppa, &row.Naim, &row.Data)
			rows[i].Groups = append(rows[i].Groups, row)
		}
	}
	for i, r := range rows {
		for j, g := range r.Groups {
			selectTovar, _ := db.Query("SELECT GRUPPA || TOV_POZ, NAIM, DATA FROM tnved3 WHERE DATA1 = '' AND GRUPPA = ?", g.Gruppa)
			defer selectTovar.Close()
			for selectTovar.Next() {
				var row tovar
				_ = selectTovar.Scan(&row.TovPoz, &row.Naim, &row.Data)
				rows[i].Groups[j].Tovars = append(rows[i].Groups[j].Tovars, row)
			}
		}
	}

	for i, r := range rows {
		for j, g := range r.Groups {
			for k, t := range g.Tovars {
				selectSub, _ := db.Query("SELECT GRUPPA || TOV_POZ || SUB_POZ, KR_NAIM, DATA FROM tnved4 WHERE DATA1 = '' AND GRUPPA = ? AND TOV_POZ = ?", g.Gruppa, t.TovPoz[2:])
				defer selectSub.Close()
				for selectSub.Next() {
					var row sub
					_ = selectSub.Scan(&row.SubPoz, &row.Naim, &row.Data)
					rows[i].Groups[j].Tovars[k].Subs = append(rows[i].Groups[j].Tovars[k].Subs, row)
				}
			}
		}
	}
	return rows
}

func saveToJSON(tnved []razdelRow) {
	println("formed, start to export")
	file, err := json.MarshalIndent(tnved, "", "  ")
	if err != nil {
		fmt.Println(err.Error())
	}
	err = os.WriteFile(utils.PathAdd(outputDir, jsonFileName), file, 0644)
	if err != nil {
		fmt.Println(err.Error())
	}

	println("file exported")
}
