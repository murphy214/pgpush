package postgis

import (
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	"github.com/paulmach/go.geojson"
	"reflect"
	"strings"
)

type ColumnType string

// int types
var SmallInt ColumnType = "smallint" // int
var Integer ColumnType = "integer"   // int
var BigInt ColumnType = "bigint"     // int

// float types
var Decimal ColumnType = "decimal" // float
var Numeric ColumnType = "numeric" // float
var Real ColumnType = "real"       // float
//var Double ColumnType = "double"   // float

// serial types
var SmallSerial ColumnType = "smallserial" // int
var Serial ColumnType = "serial"           // int
var BigSerial ColumnType = "bigserial"     // int

// character types
var VarChar ColumnType = "varchar" // string
var Char ColumnType = "Char"       // string
var Text ColumnType = "text"       // string

// Binary Data Types
var Bytea ColumnType = "bytea" // string

// timestamps & timezones
var TimestampWithoutTimezone ColumnType = "timestamp without timezone" // int
var TimestampWithTimezone ColumnType = "timestamp with timezone"       // int
var Date ColumnType = "date"                                           // int
var TimeWithoutTimezone ColumnType = "time without timezone"           // int
var TimeWithTimezone ColumnType = "time with timezone"                 // int
var Interval ColumnType = "interval"                                   // int

// boolean type
var Boolean ColumnType = "boolean" // bool

// geometry data types
var Geometry ColumnType = "geometry" // geometry

var TypeMap = map[ColumnType]string{
	SmallInt: "int",
	Integer:  "int",
	BigInt:   "int",

	Decimal: "float",
	Numeric: "float",
	Real:    "float",
	//Double:  "float",

	SmallSerial: "int",
	Serial:      "int",
	BigSerial:   "int",

	VarChar: "string",
	Char:    "string",
	Text:    "string",

	Bytea: "string",

	TimestampWithoutTimezone: "int",
	TimestampWithTimezone:    "int",
	Date:                "int",
	TimeWithoutTimezone: "int",
	TimeWithTimezone:    "int",
	Interval:            "int",

	Boolean: "bool",

	Geometry: "geometry",
}

// column data type
type Column struct {
	Name string
	Type ColumnType
}

// table structrue
type Table struct {
	TableName            string
	InsertStmt           string
	CreateStmt           string
	CurrentInsertStmt    string
	InsertValue          string
	CurrentInterfaceList []interface{}
	Count                int
	ColumnMap            map[string]string
	Tx                   *pgx.Tx
	Columns              []Column
	Inserts              int
	Conn                 *pgx.ConnPool
}

// Creates a table structure to map to.
func CreateTable(tablename string, columns []Column, config pgx.ConnPoolConfig) (*Table, error) {
	columnmap := map[string]string{}
	createlist, insertlist := []string{}, []string{}
	for _, column := range columns {

		mytype := TypeMap[column.Type]
		val := fmt.Sprintf("%s %s", column.Name, string(column.Type))
		if mytype != "geometry" {
			columnmap[column.Name] = mytype
			insertlist = append(insertlist, "$%d")
		} else if mytype == "geometry" {
			insertval := "ST_GeomFromWKB($%d,4326)"
			insertlist = append(insertlist, insertval)
		}
		createlist = append(createlist, val)
	}
	createval := strings.Join(createlist, ",")
	createstmt := fmt.Sprintf("CREATE TABLE %s (%s);", tablename, createval)
	insertval := strings.Join(insertlist, ",")
	insertstmt := fmt.Sprintf("INSERT INTO %s VALUES ", tablename)
	insertvalupdate := fmt.Sprintf("(%s), ", insertval)

	p, err := pgx.NewConnPool(config)
	if err != nil {
		return &Table{}, err
	}

	// exectuing create table stmt
	_, err = p.Exec(createstmt)
	if err != nil {
		fmt.Println(err)
	}

	tx, err := p.Begin()
	if err != nil {
		return &Table{}, err
	}

	return &Table{
		TableName:         tablename,
		InsertStmt:        insertstmt,
		CreateStmt:        createstmt,
		ColumnMap:         columnmap,
		Tx:                tx,
		Columns:           columns,
		Inserts:           0,
		Conn:              p,
		InsertValue:       insertvalupdate,
		CurrentInsertStmt: insertstmt,
	}, nil

}

// writes a value and returns the bytes of such value
// does not implement write sint currently
func ParseValue(value interface{}) (interface{}, string) {
	vv := reflect.ValueOf(value)
	kd := vv.Kind()
	var typeval string
	var myval interface{}
	// switching for each type
	switch kd {
	case reflect.String:
		typeval = "string"
		myval = vv.String()
	case reflect.Float32:
		typeval = "float"
		myval = float64(vv.Float())
	case reflect.Float64:
		typeval = "float"
		myval = float64(vv.Float())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		typeval = "int"
		myval = int(vv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		typeval = "int"
		myval = int(vv.Uint())
	case reflect.Bool:
		typeval = "bool"
		myval = vv.Bool()
	}

	return myval, typeval
}

// adds a feature to the postgis table
func (table *Table) AddFeature(feature *geojson.Feature) error {
	newlist := []interface{}{}
	newlist2 := []interface{}{}
	for pos, i := range table.Columns {
		newlist2 = append(newlist2, table.Count*len(table.Columns)+pos+1)

		if string(i.Name) == "geometry" {
			geomb, err := EncodeGeometryWKB(feature.Geometry)
			if err != nil {
				return err
			}

			newlist = append(newlist, geomb)
		} else {
			val, boolval := feature.Properties[i.Name]
			if !boolval {
				newlist = append(newlist, " ")
			} else {
				newlist = append(newlist, fmt.Sprint(val))
			}
		}
	}
	table.CurrentInterfaceList = append(table.CurrentInterfaceList, newlist...)
	table.CurrentInsertStmt += fmt.Sprintf(table.InsertValue, newlist2...)
	table.Count++

	//
	if table.Count != 5000 {

	} else {

		table.CurrentInsertStmt = table.CurrentInsertStmt[0 : len(table.CurrentInsertStmt)-2]
		_, err := table.Tx.Exec(table.CurrentInsertStmt, table.CurrentInterfaceList...)

		if err != nil {
			fmt.Println(err)
		}
		table.Count = 0
		table.CurrentInsertStmt = table.InsertStmt
		table.CurrentInterfaceList = []interface{}{}
	}

	return nil
}

// commits the given features and refreshes the transaction
func (table *Table) Commit() error {
	if table.Count > 0 {
		table.CurrentInsertStmt = table.CurrentInsertStmt[0 : len(table.CurrentInsertStmt)-2]
		_, err := table.Tx.Exec(table.CurrentInsertStmt, table.CurrentInterfaceList...)
		if err != nil {
			fmt.Println(err)
		}
		table.Count = 0
		table.CurrentInsertStmt = table.InsertStmt
		table.CurrentInterfaceList = []interface{}{}
	}
	err := table.Tx.Commit()
	if err != nil {
		return err
	}
	tx, err := table.Conn.Begin()
	table.Tx = tx
	return err
}

// reads a given table from schema so features can be added in postgis
func ReadTable(tablename string, config pgx.ConnPoolConfig) *Table {
	p, err := pgx.NewConnPool(config)
	if err != nil {
		return &Table{}
	}
	tx, err := p.Begin()
	if err != nil {
		return &Table{}
	}

	result, err := tx.Query("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = 'new';")
	if err != nil {
		fmt.Println(err)
	}
	columns := []Column{}
	raw := map[string]string{}
	for result.Next() {
		var key, typeval string
		err = result.Scan(&key, &typeval)
		if err != nil {
			fmt.Println(err)
		}
		raw[key] = typeval
		columns = append(columns, Column{Name: key})
	}

	insertlist := []string{}
	for _, column := range columns {
		if column.Name != "geometry" {
			insertlist = append(insertlist, "$%d")
		} else if column.Name == "geometry" {
			insertval := "ST_GeomFromWKB($%d,4326)"
			insertlist = append(insertlist, insertval)
		}
	}

	insertval := strings.Join(insertlist, ",")
	insertstmt := fmt.Sprintf("INSERT INTO %s VALUES ", tablename)
	insertvalupdate := fmt.Sprintf("(%s), ", insertval)

	return &Table{
		TableName:         tablename,
		InsertStmt:        insertstmt,
		Tx:                tx,
		Columns:           columns,
		Inserts:           0,
		Conn:              p,
		InsertValue:       insertvalupdate,
		CurrentInsertStmt: insertstmt,
	}
}
