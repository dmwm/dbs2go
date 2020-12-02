package main

import (
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
