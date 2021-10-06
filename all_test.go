package memsql

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/aryann/difflib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/runner-mei/errors"
	"github.com/runner-mei/memsql/memcore"
	"github.com/runner-mei/memsql/vm"
	"golang.org/x/tools/txtar"
)

type TestTable struct {
	Name    string                   `json:"table"`
	Tags    map[string]string        `json:"tags"`
	Records []map[string]interface{} `json:"records"`
}

type TestSelect struct {
	Name       string   `json:"name"`
	SQL        string   `json:"sql"`
	RowSort    bool     `json:"row_sort"`
	ColumnSort bool     `json:"column_sort"`
	Results    []string `json:"results"`
}

type TestCase struct {
	Name          string `json:"name"`
	RuntimeValues map[string][]map[string]interface{}
	Tables        []TestTable  `json:"tables"`
	Selects       []TestSelect `json:"selects"`
}

type TestApp struct {
	filename    string
	driver      string
	conn        *sql.DB
	s           memcore.Storage
	runtimeRead ReadFunc
}

func (app *TestApp) Close() error {
	err := app.conn.Close()
	if err == nil {
		if app.filename != "" {
			err = os.Remove(app.filename)
		}
	}
	return err
}

func (app *TestApp) ReadWith(runtimeValues map[string][]map[string]interface{}) {
	app.runtimeRead = ReadValues(runtimeValues)
}

func (app *TestApp) Add(t *testing.T, table *TestTable) error {
	innerTable, err := memcore.ToTable(table.Records)
	if err != nil {
		t.Error(err)
		return err
	}

	if strings.HasPrefix(table.Name, "db.") {
		tableName := strings.TrimPrefix(table.Name, "db.")
		create := "Create TABLE " + tableName + "("
		for idx, column := range innerTable.Columns {
			if idx != 0 {
				create = create + "\r\n"
			}
			isLast := false
			if idx == len(innerTable.Columns)-1 {
				isLast = true
			}

			create = create + " " + column.Name + "\t\t"
			if len(innerTable.Records) == 0 {
				create = create + " varchar(10)"
				if !isLast {
					create = create + ","
				}
				continue
			}
			create = create + " " + innerTable.Records[0][idx].ToSQLTypeLiteral()
			if !isLast {
				create = create + ","
			}
		}
		create = create + ")"

		if app.driver == "sqlite3" {
			create = strings.Replace(create, "BOOLEAN", "INTEGER", -1)
		}

		_, err = app.conn.Exec(create)
		if err != nil {
			t.Error(err)
			return err
		}

		for _, record := range innerTable.Records {
			insert := "INSERT INTO " + tableName + "("
			values := "VALUES("

			for idx := range record {

				literal := record[idx].ToSQLLiteral()

				if app.driver == "sqlite3" {
					if literal == "true" {
						literal = "1"
					} else if literal == "false" {
						literal = "0"
					}
				}

				if idx == 0 {
					insert = insert + innerTable.Columns[idx].Name
					values = values + literal
				} else {
					insert = insert + "," + innerTable.Columns[idx].Name
					values = values + "," + literal
				}
			}

			insert = insert + ")"
			values = values + ")"
			_, err = app.conn.Exec(insert + " " + values)
			if err != nil {
				t.Log(insert + " " + values)
				t.Error(err)
				return err
			}
		}
		return nil
	}

	tableName := table.Name
	index := strings.Index(tableName, "$")
	if index > 0 {
		tableName = tableName[:index]
	}
	app.s.Set(tableName, memcore.MapToTags(table.Tags), time.Now(), innerTable, nil)
	return nil
}

func (app *TestApp) Execute(t *testing.T, ctx *Context, sqlstmt string) (RecordSet, error) {
	if ctx == nil {
		ctx = &Context{}
	}
	if ctx.Ctx == nil {
		ctx.Ctx = context.Background()
	}
	if ctx.Storage == nil {

		if app.runtimeRead != nil {
			ctx.Storage = NewHookStorage(app.s, app.runtimeRead)
		} else {
			ctx.Storage = WrapStorage(app.s)
		}
	}
	if ctx.Foreign == nil {
		ctx.Foreign = NewDbForeign(app.driver, app.conn)
	}
	return Execute(ctx, sqlstmt)
}

func RecordToLine(t *testing.T, record Record, sort bool) string {
	if sort {
		record = memcore.SortByColumnName(record)
	}
	var sb strings.Builder
	record.ToLine(&sb, ",")
	return sb.String()
}

func RecordToLines(t *testing.T, results RecordSet, columnSort bool) []string {
	var lines = make([]string, 0, len(results))
	for idx := range results {
		lines = append(lines, RecordToLine(t, results[idx], columnSort))
	}
	return lines
}

func assertResults(t *testing.T, rowSort, columnSort bool, records RecordSet, excepted []string) {
	actual := RecordToLines(t, records, columnSort)
	if rowSort {
		sort.Strings(actual)
		sort.Strings(excepted)
	}
	results := difflib.Diff(excepted, actual)

	isOk := true
	for _, result := range results {
		if result.Delta != difflib.Common {
			isOk = false
			break
		}
	}

	if !isOk {
		for _, result := range results {
			t.Error(result)
		}
	}
}

func newTestApp(t *testing.T) *TestApp {
	filename := ""
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatal(err)
	}

	return &TestApp{
		filename: filename,
		s:        memcore.NewStorage(),
		driver:   "sqlite3",
		conn:     db,
	}
}

func readValue(s string) Value {
	return vm.ReadValueFromString(strings.TrimSpace(s))
}

func readTable(data []byte) (TestTable, error) {
	var tags = map[string]string{}

	data = bytes.TrimSpace(data)
	if bytes.HasPrefix(data, []byte("tags:")) {
		data = bytes.TrimPrefix(data, []byte("tags:"))

		pos := bytes.Index(data, []byte("\n"))
		if pos < 0 {
			pos = len(data)
		}

		err := json.Unmarshal(bytes.TrimSpace(data[:pos]), &tags)
		if err != nil {
			return TestTable{}, err
		}

		data = data[pos:]
		data = bytes.TrimSpace(data)
	}

	if len(data) == 0 {
		return TestTable{
			Tags: tags,
		}, nil
	}

	var records []map[string]interface{}
	var headers []string
	r := csv.NewReader(bytes.NewReader(data))
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return TestTable{}, err
		}
		if len(record) == 0 {
			continue
		}

		if len(headers) == 0 {
			headers = record
			continue
		}

		var values = map[string]interface{}{}
		for idx := range record {
			values[headers[idx]] = readValue(record[idx])
		}
		records = append(records, values)
	}

	return TestTable{
		Tags:    tags,
		Records: records,
	}, nil
}

func readText(txt []byte) (TestCase, error) {
	ar := txtar.Parse(bytes.Replace(txt, []byte("\r\n"), []byte("\n"), -1))

	// if err != nil {
	// 	return TestCase{}, err
	// }

	var tables []TestTable
	var stmts = map[string]string{}
	var rowSorts = map[string]bool{}
	var columnSorts = map[string]bool{}
	var results = map[string][]string{}
	var runtimeValues = map[string][]map[string]interface{}{}

	for _, file := range ar.Files {
		switch {
		case strings.HasSuffix(file.Name, ".sql"):
			name := strings.TrimSuffix(file.Name, ".sql")
			stmts[name] = string(bytes.TrimSpace(file.Data))
		case strings.HasSuffix(file.Name, ".result"):
			name := strings.TrimSuffix(file.Name, ".result")
			name = strings.TrimSuffix(name, ".sort")
			name = strings.TrimSuffix(name, ".row_sort")
			name = strings.TrimSuffix(name, ".column_sort")
			name = strings.TrimSuffix(name, ".row_sort")
			name = strings.TrimSuffix(name, ".column_sort")
			
			lines := bytes.Split(file.Data, []byte("\n"))

			var ss = make([]string, 0, len(lines))
			for idx := range lines {
				bs := bytes.TrimSuffix(lines[idx], []byte("\r"))
				ss = append(ss, string(bs))
			}

			for idx := len(ss) - 1; ; idx-- {
				if idx < 0 {
					ss = ss[:0]
					break
				}
				if ss[idx] != "" {
					ss = ss[:idx+1]
					break
				}
			}

			results[name] = ss

			if strings.Contains(file.Name, ".column_sort.") {
				columnSorts[name] = true
			}
			if strings.Contains(file.Name, ".row_sort.") {
				rowSorts[name] = true
			}
			if strings.Contains(file.Name, ".sort.") {
				columnSorts[name] = true
				rowSorts[name] = true
			}
		default:
			tableName := file.Name
			if strings.HasPrefix(tableName, "table_") {
				tableName = strings.TrimPrefix(tableName, "table_")
				tableName = strings.TrimSuffix(tableName, ".txt")
			}

			table, err := readTable(file.Data)
			if err != nil {
				return TestCase{}, err
			}
			table.Name = tableName

			if !strings.HasPrefix(tableName, "runtime_") {
				tables = append(tables, table)
				break
			}
			tableName = strings.TrimPrefix(tableName, "runtime_")

			runtimeValues[tableName+"-"+memcore.KeyValues(memcore.MapToTags(table.Tags)).ToKey()] = table.Records
		}
	}

	if len(stmts) == 0 {
		var sb strings.Builder
		for idx, file := range ar.Files {
			if idx != 0 {
				sb.WriteString(",")
			}
			sb.WriteString(file.Name)
		}
		return TestCase{}, errors.New("Parse:" + sb.String())
	}

	testCase := TestCase{
		Tables:        tables,
		RuntimeValues: runtimeValues,
		// Selects []TestSelect `json:"selects"`
	}
	for key, stmt := range stmts {
		sel := TestSelect{
			Name:    key,
			SQL:     stmt,
			Results: results[key],
		}
		if value, ok := rowSorts[key]; ok {
			sel.RowSort = value
		}
		if value, ok := columnSorts[key]; ok {
			sel.ColumnSort = value
		}
		testCase.Selects = append(testCase.Selects, sel)
	}
	return testCase, nil
}

func TestSize(t *testing.T) {
	if unsafe.Sizeof(vm.Value{}) != 64 {
		t.Error("size")
	}
}

func TestAll(t *testing.T) {
	var allTests = []TestCase{}
	list, err := ioutil.ReadDir("./tests")
	if err != nil {
		t.Error(err)
		return
	}

	for _, file := range list {
		bs, err := ioutil.ReadFile(filepath.Join("./tests", file.Name()))
		if err != nil {
			t.Error(err)
			return
		}

		tc, err := readText(bs)
		if err != nil {
			t.Error(file.Name(), err)
			return
		}
		tc.Name = file.Name()
		allTests = append(allTests, tc)
	}

	runTests(t, allTests)
}

func runTests(t *testing.T, allTests []TestCase) {
	for _, test := range allTests {
		t.Run(test.Name, func(t *testing.T) {
			app := newTestApp(t)
			defer func() {
				err := app.Close()
				if err != nil {
					t.Error(err)
				}
			}()
			for _, table := range test.Tables {
				app.Add(t, &table)
			}

			if len(test.RuntimeValues) > 0 {
				app.ReadWith(test.RuntimeValues)
			}

			for _, stmt := range test.Selects {
				t.Run(stmt.Name, func(t *testing.T) {
					t.Log(stmt.SQL)

					ctx := &Context{}
					results, err := app.Execute(t, ctx, stmt.SQL)
					if err != nil {
						t.Log(ctx.Debuger.String())
						t.Error(err)
						return
					}
					assertResults(t, stmt.RowSort, stmt.ColumnSort, results, stmt.Results)
					if t.Failed() {
						t.Log(ctx.Debuger.String())
					}
				})
			}
		})
	}
}
