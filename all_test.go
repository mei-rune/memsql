package memsql

import (
	"context"
	"strings"
	"testing"

	"github.com/aryann/difflib"
	"github.com/runner-mei/memsql/memcore"
)

type TestTable struct {
	Table   string                   `json:"table"`
	Tags    map[string]string        `json:"tags"`
	Records []map[string]interface{} `json:"records"`
}

type TestSelect struct {
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

	app.s.Set(table.Table, memcore.MapToTags(table.Tags), innerTable)
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
	for _, result := range results {
		t.Error(result)
	}
}

func newTestApp(t *testing.T) *TestApp {
	return &TestApp{
		s: memcore.NewStorage(),
	}
}

func TestAll(t *testing.T) {
	var allTests = []TestCase{}
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
				results, err := app.Execute(t, stmt.SQL)
				if err != nil {
					t.Error("[select]", stmt.SQL)
					t.Error(err)
					continue
				}
				assertResults(t, results, stmt.Sort, stmt.Results)
			}
		})
	}
}
