package main

import (
	"fmt"
	"testing"

	"github.com/vkuznet/dbs2go/utils"
)

// TestInList
func TestInList(t *testing.T) {
	vals := []string{"1", "2", "3"}
	res := utils.InList("1", vals)
	if res == false {
		t.Error("Fail TestInList")
	}
	res = utils.InList("5", vals)
	if res == true {
		t.Error("Fail TestInList")
	}
}

// TestRecordSize
func TestRecordSize(t *testing.T) {
	rec := make(map[string]int)
	rec["a"] = 1
	rec["b"] = 2
	size, err := utils.RecordSize(rec)
	if err != nil {
		t.Error("Fail in RecordSize", err)
	}
	fmt.Println("record", rec, "size", size)
}

// TestReplaceBinds
func TestReplaceBinds(t *testing.T) {
	str := `
	INSERT INTO {{.Owner}}.FILES
    (file_id,logical_file_name,is_file_valid)
    VALUES
    (:file_id,:logical_file_name,:is_file_valid)
	 `
	nstr := utils.ReplaceBinds(str)
	expect := `
	INSERT INTO {{.Owner}}.FILES
    (file_id,logical_file_name,is_file_valid)
    VALUES
    (?,?,?)
	 `
	if nstr != expect {
		t.Error("unable to replace binds")
	}
}
