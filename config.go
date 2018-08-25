package postgis

import (
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	"github.com/paulmach/go.geojson"
	"reflect"
	"strconv"
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
var Double ColumnType = "double"   // float

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
	Double:  "float",

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
	TableName  string
	InsertStmt string
	CreateStmt string
	ColumnMap  map[string]string
	Tx         *pgx.Tx
	Columns    []Column
	Inserts    int
	Conn       *pgx.ConnPool
}

// Creates a table structure to map to.
func CreateTable(tablename string, columns []Column, config pgx.ConnPoolConfig) (*Table, error) {
	columnmap := map[string]string{}
	createlist, insertlist := []string{}, []string{}
	for i, column := range columns {

		mytype := TypeMap[column.Type]
		val := fmt.Sprintf("%s %s", column.Name, string(column.Type))
		if mytype != "geometry" {
			columnmap[column.Name] = mytype
			insertlist = append(insertlist, "$"+strconv.Itoa(i+1))
		} else if mytype == "geometry" {
			insertval := fmt.Sprintf("ST_GeomFromWKB(%s,4326)", "$"+strconv.Itoa(i+1))
			insertlist = append(insertlist, insertval)
		}
		createlist = append(createlist, val)
	}
	createval := strings.Join(createlist, ", ")
	createstmt := fmt.Sprintf("CREATE TABLE %s (%s);", tablename, createval)
	insertval := strings.Join(insertlist, ", ")
	insertstmt := fmt.Sprintf("INSERT INTO %s VALUES (%s);", tablename, insertval)

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
		TableName:  tablename,
		InsertStmt: insertstmt,
		CreateStmt: createstmt,
		ColumnMap:  columnmap,
		Tx:         tx,
		Columns:    columns,
		Inserts:    0,
		Conn:       p,
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
	for _, i := range table.Columns {

		if string(i.Type) == "geometry" {
			geomb, err := EncodeGeometryWKB(feature.Geometry)
			if err != nil {
				return err
			}

			newlist = append(newlist, geomb)
		} else {
			val, boolval := feature.Properties[i.Name]

			if boolval {
				newval, typeval := ParseValue(val)
				if typeval != string(i.Type) {
					if typeval == "int" && string(i.Type) == "string" {
						myval := newval.(int)
						addval := strconv.Itoa(myval)
						newlist = append(newlist, addval)
					} else if typeval == "int" && string(i.Type) == "float" {
						myval := newval.(int)
						addval := float64(myval)
						newlist = append(newlist, addval)
					} else if typeval == "float" && string(i.Type) == "int" {
						myval := newval.(float64)
						addval := float64(myval)
						newlist = append(newlist, addval)
					} else if typeval == "float" && string(i.Type) == "string" {
						myval := newval.(float64)
						addval := fmt.Sprintf("%f", myval)
						newlist = append(newlist, addval)
					} else if typeval == "string" && string(i.Type) == "int" {
						myval := newval.(string)
						addval, err := strconv.ParseInt(myval, 10, 64)
						var addval2 int
						if err != nil {
							addval2 = 0
						} else {
							addval2 = int(addval)
						}
						newlist = append(newlist, addval2)
					} else if typeval == "string" && string(i.Type) == "float" {
						myval := newval.(string)
						addval, err := strconv.ParseFloat(myval, 64)
						var addval2 float64
						if err != nil {
							addval2 = 0.0
						} else {
							addval2 = float64(addval)
						}
						newlist = append(newlist, addval2)
					} else {
						newlist = append(newlist, nil)
					}

				} else {
					newlist = append(newlist, newval)
				}
			} else {
				newlist = append(newlist, nil)
			}
		}
	}
	_, err := table.Tx.Exec(table.InsertStmt, newlist...)
	if err == nil {
		table.Inserts++
	}
	return err
}

// commits the given features and refreshes the transaction
func (table *Table) Commit() error {
	err := table.Tx.Commit()
	if err != nil {
		return err
	}
	tx, err := table.Conn.Begin()
	table.Tx = tx
	return err
}
