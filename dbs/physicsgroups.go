package dbs

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/vkuznet/dbs2go/utils"
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

// PhysicsGroups
type PhysicsGroups struct {
	PHYSICS_GROUP_ID   int64  `json:"physics_group_id"`
	PHYSICS_GROUP_NAME string `json:"physics_group_name"`
}

// Insert implementation of PhysicsGroups
func (r *PhysicsGroups) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PHYSICS_GROUP_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertId(tx, "PHYSICS_GROUPS", "physics_group_id")
			r.PHYSICS_GROUP_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_FT")
			r.PHYSICS_GROUP_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// get SQL statement from static area
	stm := getSQL("insert_physicsgroups")
	if DBOWNER == "sqlite" {
		stm = getSQL("insert_physicsgroups_sqlite")
	}
	if utils.VERBOSE > 0 {
		log.Printf("Insert PhysicsGroups\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PHYSICS_GROUP_ID, r.PHYSICS_GROUP_NAME)
	return err
}

// Validate implementation of PhysicsGroups
func (r *PhysicsGroups) Validate() error {
	if r.PHYSICS_GROUP_NAME == "" {
		return errors.New("missing physics_group_name")
	}
	return nil
}

// Decode implementation for PhysicsGroups
func (r *PhysicsGroups) Decode(reader io.Reader) (int64, error) {
	// init record with given data record
	data, err := ioutil.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return 0, err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return 0, err
	}
	size := int64(len(data))
	return size, nil
}
