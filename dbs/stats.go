package dbs

import (
	"database/sql"
	"log"

	"github.com/dmwm/dbs2go/utils"
)

// SchemaInfo represents schema details
type SchemaInfo struct {
	Owner   string
	Size    float64
	Indexes []SchemaIndex
}

// SchemaIndex represents schema index details
type SchemaIndex struct {
	Owner string
	Index string
	Size  float64
}

// TableInfo represents individual table info
type TableInfo struct {
	Owner   string
	Table   string
	Rows    float64
	Size    float64
	Indexes []TableIndex
}

// TableIndex represents individual table index sizes
type TableIndex struct {
	Owner string
	Table string
	Index string
	Size  float64
}

// DBInfo represents entire database information
type DBInfo struct {
	FullSize  float64
	IndexSize float64
	Schemas   []SchemaInfo
	Tables    []TableInfo
}

// DBStats returns database stats
func DBStats() (DBInfo, error) {
	var dbInfo DBInfo

	tmpl := make(Record)
	tmpl["Owner"] = "CMS_DBS3%"

	tx, err := DB.Begin()
	if err != nil {
		log.Println("unable to get DB transaction", err)
		return dbInfo, Error(err, TransactionErrorCode, "", "dbs.stats.DBStats")
	}
	defer tx.Rollback()

	dbInfo.FullSize, err = fullSize(tx, tmpl)
	if err != nil {
		log.Println("unable to get full size info", err)
	}
	dbInfo.IndexSize, err = indexSize(tx, tmpl)
	if err != nil {
		log.Println("unable to get index size info", err)
	}
	dbInfo.Schemas, err = schemasSize(tx, tmpl)
	if err != nil {
		log.Println("unable to get schemas size info", err)
	}
	dbInfo.Tables, err = tablesSize(tx, tmpl)
	if err != nil {
		log.Println("unable to get tables size info", err)
	}

	err = tx.Commit()
	if err != nil {
		log.Println("unable to commit transaction", err)
		return dbInfo, Error(err, CommitErrorCode, "", "dbs.stats.DBStats")
	}

	return dbInfo, nil
}

// helper function to get full database size
func fullSize(tx *sql.Tx, tmpl Record) (float64, error) {
	stm, err := LoadTemplateSQL("stats_db_size", tmpl)
	if err != nil {
		return 0, Error(err, LoadErrorCode, "", "dbs.stats.fullSize")
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		log.Printf("### SQL statement ###\n%s\n\n", stm)
	}
	rows, err := tx.Query(stm)
	if err != nil {
		log.Printf("unable to execute query %s, error %v", stm, err)
		return 0, Error(err, QueryErrorCode, "", "dbs.stats.fullSize")
	}
	var totalSize float64
	for rows.Next() {
		var size float64
		if err := rows.Scan(&size); err != nil {
			log.Printf("unable to scan size row, error %v", err)
			return 0, Error(err, RowsScanErrorCode, "", "dbs.stats.fullSize")
		}
		totalSize += size
	}
	return totalSize, nil
}

// helper function to get index size of database
func indexSize(tx *sql.Tx, tmpl Record) (float64, error) {
	stm, err := LoadTemplateSQL("stats_db_indexes", tmpl)
	if err != nil {
		return 0, Error(err, LoadErrorCode, "", "dbs.stats.indexSize")
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		log.Printf("### SQL statement ###\n%s\n\n", stm)
	}
	rows, err := tx.Query(stm)
	if err != nil {
		log.Printf("unable to execute query %s, error %v", stm, err)
		return 0, Error(err, QueryErrorCode, "", "dbs.stats.indexSize")
	}
	var totalSize float64
	for rows.Next() {
		var size float64
		if err := rows.Scan(&size); err != nil {
			log.Printf("unable to scan size row, error %v", err)
			return 0, Error(err, RowsScanErrorCode, "", "dbs.stats.indexSize")
		}
		totalSize += size
	}
	return totalSize, nil
}

// helper function to get schemas information from a database
func schemasSize(tx *sql.Tx, tmpl Record) ([]SchemaInfo, error) {
	var schemas []SchemaInfo
	var schemaIndexes []SchemaIndex
	stm, err := LoadTemplateSQL("stats_schemas_indexes", tmpl)
	if err != nil {
		return schemas, Error(err, LoadErrorCode, "", "dbs.stats.schemaSize")
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		log.Printf("### SQL statement ###\n%s\n\n", stm)
	}
	rows, err := tx.Query(stm)
	if err != nil {
		log.Printf("unable to execute query %s, error %v", stm, err)
		return schemas, Error(err, QueryErrorCode, "", "dbs.stats.schemaSize")
	}
	for rows.Next() {
		var owner string
		var size float64
		if err := rows.Scan(&owner, &size); err != nil {
			log.Printf("unable to scan size row, error %v", err)
			return schemas, Error(err, RowsScanErrorCode, "", "dbs.stats.schemaSize")
		}
		schema := SchemaIndex{Owner: owner, Size: size}
		schemaIndexes = append(schemaIndexes, schema)
	}

	stm, err = LoadTemplateSQL("stats_schemas_size", tmpl)
	if err != nil {
		return schemas, Error(err, LoadErrorCode, "", "dbs.stats.schemaSize")
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		log.Printf("### SQL statement ###\n%s\n\n", stm)
	}
	rows, err = tx.Query(stm)
	if err != nil {
		log.Printf("unable to execute query %s, error %v", stm, err)
		return schemas, Error(err, QueryErrorCode, "", "dbs.stats.schemaSize")
	}
	for rows.Next() {
		var owner string
		var size float64
		if err := rows.Scan(&owner, &size); err != nil {
			log.Printf("unable to scan size row, error %v", err)
			return schemas, Error(err, RowsScanErrorCode, "", "dbs.stats.schemaSize")
		}
		schema := SchemaInfo{Owner: owner, Size: size}
		for _, s := range schemaIndexes {
			if s.Owner == owner {
				schema.Indexes = append(schema.Indexes, s)
			}
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}
func tablesSize(tx *sql.Tx, tmpl Record) ([]TableInfo, error) {
	var tableIndexes []TableIndex
	var tables []TableInfo
	stm, err := LoadTemplateSQL("stats_tables_indexes", tmpl)
	if err != nil {
		return tables, Error(err, LoadErrorCode, "", "dbs.stats.tablesSize")
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		log.Printf("### SQL statement ###\n%s\n\n", stm)
	}
	rows, err := tx.Query(stm)
	if err != nil {
		log.Printf("unable to execute query %s, error %v", stm, err)
		return tables, Error(err, QueryErrorCode, "", "dbs.stats.tablesSize")
	}
	for rows.Next() {
		var owner string
		var table string
		var index string
		var size float64
		if err := rows.Scan(&owner, &table, &index, &size); err != nil {
			log.Printf("unable to scan size row, error %v", err)
			return tables, Error(err, RowsScanErrorCode, "", "dbs.stats.tablesSize")
		}
		t := TableIndex{Owner: owner, Table: table, Index: index, Size: size}
		tableIndexes = append(tableIndexes, t)
	}
	stm, err = LoadTemplateSQL("stats_tables_size", tmpl)
	if err != nil {
		return tables, Error(err, LoadErrorCode, "", "dbs.stats.tablesSize")
	}
	stm = CleanStatement(stm)
	if utils.VERBOSE > 1 {
		log.Printf("### SQL statement ###\n%s\n\n", stm)
	}
	rows, err = tx.Query(stm)
	if err != nil {
		log.Printf("unable to execute query %s, error %v", stm, err)
		return tables, Error(err, QueryErrorCode, "", "dbs.stats.tablesSize")
	}
	for rows.Next() {
		var owner string
		var table string
		var nrows float64
		var size float64
		if err := rows.Scan(&owner, &table, &nrows, &size); err != nil {
			log.Printf("unable to scan size row, error %v", err)
			return tables, Error(err, RowsScanErrorCode, "", "dbs.stats.tablesSize")
		}
		tinfo := TableInfo{Owner: owner, Table: table, Rows: nrows, Size: size}
		for _, t := range tableIndexes {
			if t.Owner == owner && t.Table == table {
				tinfo.Indexes = append(tinfo.Indexes, t)
			}
		}
		tables = append(tables, tinfo)
	}
	return tables, nil
}
