package dbs

import (
	"database/sql"
	"encoding/json"
	"io"
	"log"

	"github.com/vkuznet/dbs2go/utils"
)

// PhysicsGroups DBS API
func (a API) PhysicsGroups() error {
	var args []interface{}
	var conds []string

	// parse dataset argument
	conds, args = AddParam("physics_group_name", "pg.PHYSICS_GROUP_NAME", a.Params, conds, args)

	// get SQL statement from static area
	stm := getSQL("physicsgroups")
	stm = WhereClause(stm, conds)

	// use generic query API to fetch the results from DB
	return executeAll(a.Writer, a.Separator, stm, args...)
}

// PhysicsGroups
type PhysicsGroups struct {
	PHYSICS_GROUP_ID   int64  `json:"physics_group_id"`
	PHYSICS_GROUP_NAME string `json:"physics_group_name" validate:"required"`
}

// Insert implementation of PhysicsGroups
func (r *PhysicsGroups) Insert(tx *sql.Tx) error {
	var tid int64
	var err error
	if r.PHYSICS_GROUP_ID == 0 {
		if DBOWNER == "sqlite" {
			tid, err = LastInsertID(tx, "PHYSICS_GROUPS", "physics_group_id")
			r.PHYSICS_GROUP_ID = tid + 1
		} else {
			tid, err = IncrementSequence(tx, "SEQ_PG")
			r.PHYSICS_GROUP_ID = tid
		}
		if err != nil {
			return err
		}
	}
	// set defaults and validate the record
	r.SetDefaults()
	err = r.Validate()
	if err != nil {
		log.Println("unable to validate record", err)
		return err
	}
	// get SQL statement from static area
	stm := getSQL("insert_physics_groups")
	if utils.VERBOSE > 0 {
		log.Printf("Insert PhysicsGroups\n%s\n%+v", stm, r)
	}
	_, err = tx.Exec(stm, r.PHYSICS_GROUP_ID, r.PHYSICS_GROUP_NAME)
	return err
}

// Validate implementation of PhysicsGroups
func (r *PhysicsGroups) Validate() error {
	if err := RecordValidator.Struct(*r); err != nil {
		return DecodeValidatorError(r, err)
	}
	return nil
}

// SetDefaults implements set defaults for PhysicsGroups
func (r *PhysicsGroups) SetDefaults() {
}

// Decode implementation for PhysicsGroups
func (r *PhysicsGroups) Decode(reader io.Reader) error {
	// init record with given data record
	data, err := io.ReadAll(reader)
	if err != nil {
		log.Println("fail to read data", err)
		return err
	}
	err = json.Unmarshal(data, &r)

	//     decoder := json.NewDecoder(r)
	//     err := decoder.Decode(&rec)
	if err != nil {
		log.Println("fail to decode data", err)
		return err
	}
	return nil
}

// InsertPhysicsGroups DBS API
func (a API) InsertPhysicsGroups() error {
	return insertRecord(&PhysicsGroups{}, a.Reader)
}
