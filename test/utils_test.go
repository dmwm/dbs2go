package main

import (
	"fmt"
	"io"
	"testing"

	"github.com/dmwm/dbs2go/utils"
)

// TestUtilsInList
func TestUtilsInList(t *testing.T) {
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

// TestUtilsSet
func TestUtilsSet(t *testing.T) {
	vals := []int64{1, 2, 3, 1}
	res := utils.Set(vals)
	if !utils.Equal(res, utils.Set([]int64{3, 2, 1})) {
		t.Error("Fail TestUtilsSet")
	}
	arr := []int64{4, 5, 6}
	if utils.Equal(res, arr) {
		t.Error("Fail of Equal in TestUtilsSet")
	}
}

// TestUtilsRecordSize
func TestUtilsRecordSize(t *testing.T) {
	rec := make(map[string]int)
	rec["a"] = 1
	rec["b"] = 2
	size, err := utils.RecordSize(rec)
	if err != nil {
		t.Error("Fail in RecordSize", err)
	}
	fmt.Println("record", rec, "size", size)
}

// TestUtilsReplaceBinds
func TestUtilsReplaceBinds(t *testing.T) {
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

// TestUtilsInsert
func TestUtilsInsert(t *testing.T) {
	var arr []interface{}
	arr = append(arr, 1)
	arr = append(arr, 2)
	arr = utils.Insert(arr, 0)
	for i, v := range arr {
		if i != v {
			t.Error("invalid insert", arr)
		}
	}
}

// TestUtilsUpdateOrderedDict
func TestUtilsUpdateOrderedDict(t *testing.T) {
	omap := make(map[int][]string)
	omap[1] = []string{"a", "b"}
	fmt.Printf("input omap %+v\n", omap)
	nmap := make(map[int][]string)
	nmap[1] = []string{"c"}
	nmap[2] = []string{"x"}
	fmt.Printf("input nmap %+v\n", nmap)
	out := utils.UpdateOrderedDict(omap, nmap)
	fmt.Printf("output map %+v\n", out)
	if list, ok := out[1]; ok {
		if len(list) != 3 {
			t.Error("wrong number of entries")
		}
	} else {
		t.Error("no entries for index 1")
	}
	if list, ok := out[2]; ok {
		if len(list) != 1 {
			t.Error("wrong number of entries")
		}
	} else {
		t.Error("no entries for index 2")
	}
}

// TestUtilsPipe tests pipe logic used in dbs/migrate.go
func TestUtilsPipe(t *testing.T) {
	pr, pw := io.Pipe()
	defer pr.Close()
	msg := "test"

	go func() {
		defer pw.Close()
		size, err := pw.Write([]byte(msg))
		if err != nil {
			t.Error(err.Error())
		}
		fmt.Printf("wrote '%s' size %d\n", msg, size)
	}()

	data, err := io.ReadAll(pr)
	if err != nil {
		t.Error(err.Error())
	}
	fmt.Printf("read '%s'\n", string(data))
	if string(data) != msg {
		t.Errorf("written data %s, read data %s", msg, string(data))
	}
}
