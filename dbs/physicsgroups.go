package dbs

import (
	"net/http"
)

// PhysicsGroups DBS API
func (API) PhysicsGroups(params Record, w http.ResponseWriter) (int64, error) {
	var args []interface{}
	var conds []string

	// parse dataset argument
	conds, args = AddParam("physics_group_name", "pg.PHYSICS_GROUP_NAME", params, conds, args)

	// get SQL statement from static area
	stm := getSQL("physicsgroups")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(w, stm, args...)
}

// InsertPhysicsGroups DBS API
func (API) InsertPhysicsGroups(values Record) error {
	return InsertValues("insert_physics_groups", values)
}
