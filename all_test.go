package memsql

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"io"
	"io/ioutil"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aryann/difflib"
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
	Name    string   `json:"name"`
	SQL     string   `json:"sql"`
	Sort    bool     `json:"sort"`
	Results []string `json:"results"`
}

type TestCase struct {
	Name    string       `json:"name"`
	Tables  []TestTable  `json:"tables"`
	Selects []TestSelect `json:"selects"`
}

type TestApp struct {
	s Storage
}

func (app *TestApp) Add(t *testing.T, table *TestTable) error {
	innerTable, err := memcore.ToTable(table.Records)
	if err != nil {
		t.Error(err)
		return err
	}

	app.s.Set(table.Name, memcore.MapToTags(table.Tags), innerTable)
	return nil
}

func (app *TestApp) Execute(t *testing.T, sqlstmt string) (RecordSet, error) {
	return Execute(&Context{
		Ctx:     context.Background(),
		Storage: app.s,
	}, sqlstmt)
}

func RecordToLine(t *testing.T, record Record, sort bool) string {
	if sort {
		record = memcore.SortByColumnName(record)
	}
	var sb strings.Builder
	record.ToLine(&sb, ",")
	return sb.String()
}

func RecordToLines(t *testing.T, results RecordSet, sort bool) []string {
	var lines = make([]string, 0, len(results))
	for idx := range results {
		lines = append(lines, RecordToLine(t, results[idx], sort))
	}
	return lines
}

func assertResults(t *testing.T, records RecordSet, sort bool, excepted []string) {
	actual := RecordToLines(t, records, sort)
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
	return &TestApp{
		s: memcore.NewStorage(),
	}
}

func readValue(s string) Value {
	return vm.ReadValueFromString(s)
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

		data = data[:pos]
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
	ar := txtar.Parse(txt)
	// if err != nil {
	// 	return TestCase{}, err
	// }

	var tables []TestTable
	var stmts = map[string]string{}
	var sorts = map[string]bool{}
	var results = map[string][]string{}

	for _, file := range ar.Files {
		switch {
		case strings.HasSuffix(file.Name, ".sql"):
			name := strings.TrimSuffix(file.Name, ".sql")
			stmts[name] = string(bytes.TrimSpace(file.Data))
		case strings.HasSuffix(file.Name, ".result"):
			name := strings.TrimSuffix(file.Name, ".result")
			name = strings.TrimSuffix(name, ".sort")
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

			if strings.Contains(file.Name, ".sort.") {
				sorts[name] = true
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
			tables = append(tables, table)
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
		Tables: tables,
		// Selects []TestSelect `json:"selects"`
	}
	for key, stmt := range stmts {
		sel := TestSelect{
			Name:    key,
			SQL:     stmt,
			Sort:    false,
			Results: results[key],
		}
		if value, ok := sorts[key]; ok {
			sel.Sort = value
		}
		testCase.Selects = append(testCase.Selects, sel)
	}
	return testCase, nil
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
			t.Error(err)
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
			for _, table := range test.Tables {
				app.Add(t, &table)
			}

			for _, stmt := range test.Selects {
				t.Run(stmt.Name, func(t *testing.T) {
					t.Log(stmt.SQL)

					results, err := app.Execute(t, stmt.SQL)
					if err != nil {
						t.Error(err)
						return
					}
					assertResults(t, results, stmt.Sort, stmt.Results)
				})
			}
		})
	}
}
