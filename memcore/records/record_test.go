package records

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/runner-mei/memsql/vm"
)

func assertEqual(t *testing.T, want, got interface{}, msg ...string) {
	if reflect.DeepEqual(want, got) {
		return
	}
	t.Helper()

	t.Error(strings.Join(msg, ""), "want", want, "got", got)
}

func TestRecord(t *testing.T) {
	r1 := &Record{
		Tags: KeyValues{KeyValue{Key: "a", Value: "1"}},
		Columns: []Column{
			Column{TableName: "abc", TableAs: "t1", Name: "c1"},
			Column{TableName: "abc", TableAs: "t1", Name: "c2"},
			Column{TableName: "abc", TableAs: "t1", Name: "c3_valuenotexists"},
		},
		Values: []Value{
			vm.StringToValue("v1"),
			vm.StringToValue("v2"),
		},
	}

	taga, ok := r1.Get("a")
	if !ok {
		t.Error("tag a isnot found")
	} else {
		assertEqual(t, vm.ValueString, taga.Type)
		assertEqual(t, "1", taga.StrValue())
	}

	c1, ok := r1.Get("c1")
	if !ok {
		t.Error("c1 a isnot found")
	} else {
		assertEqual(t, vm.ValueString, c1.Type)
		assertEqual(t, "v1", c1.StrValue())
	}

	c3, ok := r1.Get("c3_valuenotexists")
	if !ok {
		t.Error("c3_valuenotexists a isnot found")
	} else {
		if !c3.IsNil() {
			t.Error("want nil got", c3.String())
		}
	}

	_, ok = r1.Get("notexists")
	if ok {
		t.Error("notexists a is found")
	}

	for _, qualifier := range []string{"abc", "t1"} {
		taga, ok := r1.GetByQualifierName(qualifier, "a")
		if !ok {
			t.Error("tag a isnot found")
		} else {
			assertEqual(t, vm.ValueString, taga.Type)
			assertEqual(t, "1", taga.StrValue())
		}

		c1, ok := r1.GetByQualifierName(qualifier, "c1")
		if !ok {
			t.Error("c1 a isnot found")
		} else {
			assertEqual(t, vm.ValueString, c1.Type)
			assertEqual(t, "v1", c1.StrValue())
		}

		c3, ok := r1.GetByQualifierName(qualifier, "c3_valuenotexists")
		if !ok {
			t.Error("c3_valuenotexists a isnot found")
		} else {
			if !c3.IsNil() {
				t.Error("want nil got", c3.String())
			}
		}

		_, ok = r1.GetByQualifierName(qualifier, "notexists")
		if ok {
			t.Error("notexists a is found")
		}
	}

	// taga, ok = r1.GetByQualifierName("qnotexists", "a")
	// if ok {
	// 	t.Error("qnotexists a is found")
	// }

	c1, ok = r1.GetByQualifierName("qnotexists", "c1")
	if ok {
		t.Error("qnotexists a is found")
	}

	c3, ok = r1.GetByQualifierName("qnotexists", "c3_valuenotexists")
	if ok {
		t.Error("qnotexists a is found")
	}

	_, ok = r1.GetByQualifierName("qnotexists", "notexists")
	if ok {
		t.Error("notexists a is found")
	}
}

func TestMergeRecord(t *testing.T) {
	r1 := &Record{
		Tags: KeyValues{KeyValue{Key: "taga", Value: "tag1"}},
		Columns: []Column{
			Column{TableName: "abc", TableAs: "t1", Name: "c1"},
			Column{TableName: "abc", TableAs: "t1", Name: "c2"},
			Column{TableName: "abc", TableAs: "t1", Name: "c3_valuenotexists"},
		},
		Values: []Value{
			vm.StringToValue("v1"),
			vm.StringToValue("v2"),
		},
	}

	r2 := &Record{
		Tags: KeyValues{KeyValue{Key: "tagb", Value: "tag2"}},
		Columns: []Column{
			Column{TableName: "abd", TableAs: "t2", Name: "d1"},
			Column{TableName: "abd", TableAs: "t2", Name: "d2"},
			Column{TableName: "abd", TableAs: "t2", Name: "d3_valuenotexists"},
		},
		Values: []Value{
			vm.StringToValue("v21"),
			vm.StringToValue("v22"),
		},
	}

	result := MergeRecord("", *r1, "", *r2)
	excepted := Record{
		Columns: []Column{
			Column{TableName: "abc", TableAs: "t1", Name: "taga"},
			Column{TableName: "abc", TableAs: "t1", Name: "c1"},
			Column{TableName: "abc", TableAs: "t1", Name: "c2"},
			Column{TableName: "abc", TableAs: "t1", Name: "c3_valuenotexists"},

			Column{TableName: "abd", TableAs: "t2", Name: "tagb"},
			Column{TableName: "abd", TableAs: "t2", Name: "d1"},
			Column{TableName: "abd", TableAs: "t2", Name: "d2"},
			Column{TableName: "abd", TableAs: "t2", Name: "d3_valuenotexists"},
		},
		Values: []Value{
			vm.StringToValue("tag1"),
			vm.StringToValue("v1"),
			vm.StringToValue("v2"),
			vm.Null(),
			vm.StringToValue("tag2"),
			vm.StringToValue("v21"),
			vm.StringToValue("v22"),
			vm.Null(),
		},
	}

	opts := cmp.Options{
		cmpopts.EquateApproxTime(1 * time.Second),
	}
	if !cmp.Equal(result, excepted, opts) {

		txt := cmp.Diff(result, excepted, opts)
		t.Error(txt)
	}

	result = MergeRecord("as1", *r1, "as2", *r2)
	excepted = Record{
		Columns: []Column{
			Column{TableName: "abc", TableAs: "as1", Name: "taga"},
			Column{TableName: "abc", TableAs: "as1", Name: "c1"},
			Column{TableName: "abc", TableAs: "as1", Name: "c2"},
			Column{TableName: "abc", TableAs: "as1", Name: "c3_valuenotexists"},

			Column{TableName: "abd", TableAs: "as2", Name: "tagb"},
			Column{TableName: "abd", TableAs: "as2", Name: "d1"},
			Column{TableName: "abd", TableAs: "as2", Name: "d2"},
			Column{TableName: "abd", TableAs: "as2", Name: "d3_valuenotexists"},
		},
		Values: []Value{
			vm.StringToValue("tag1"),
			vm.StringToValue("v1"),
			vm.StringToValue("v2"),
			vm.Null(),
			vm.StringToValue("tag2"),
			vm.StringToValue("v21"),
			vm.StringToValue("v22"),
			vm.Null(),
		},
	}

	if !cmp.Equal(result, excepted, opts) {
		txt := cmp.Diff(result, excepted, opts)
		t.Error(txt)
	}
}
