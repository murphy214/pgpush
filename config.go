package pgpush

import (
	"errors"
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	"github.com/paulmach/go.geojson"
	"reflect"
	"strconv"
	"strings"
)

var DefaultIncrement = 1000
var DefaultSRID = 4326
var WebMercatorSRID = 3857

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

// hstore column type
var HStore ColumnType = "hstore_tags"

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

	HStore: "hstore",
}

// column data type
type Column struct {
	Name       string
	Type       ColumnType
	TargetSRID int
	GivenSRID  int
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
	HStoreFormatString   string
	HStoreColumns        []string
	Conn                 *pgx.ConnPool
}

// Creates a table structure to map to.
func CreateTable(tablename string, columns []Column, config pgx.ConnPoolConfig) (*Table, error) {
	columnmap := map[string]string{}
	createlist, insertlist := []string{}, []string{}
	var hstore_bool bool
	for _, column := range columns {
		mytype := TypeMap[column.Type]
		if mytype == "hstore" {
			hstore_bool = true
		}
	}
	hstore_lines := []string{}
	hstore_columns := []string{}
	//pos := 0
	new_columns := []Column{}
	column_names := []string{}
	for _, column := range columns {
		var hstore_val bool
		mytype := TypeMap[column.Type]
		val := fmt.Sprintf("%s %s", column.Name, string(column.Type))
		if hstore_bool && mytype == "string" {
			columnmap[column.Name] = mytype
			hstore_lines = append(hstore_lines, fmt.Sprintf(`"%s" `, column.Name)+`=> "%s",`)
			hstore_columns = append(hstore_columns, column.Name)
			hstore_val = true
		} else if strings.Contains(mytype, "hstore") {
			insertlist = append(insertlist, "$%d")
			//pos++
			column_names = append(column_names, column.Name)
			new_columns = append(new_columns, column)
			val = fmt.Sprintf("%s %s", column.Name, "hstore")

		} else if mytype != "geometry" {
			columnmap[column.Name] = mytype
			insertlist = append(insertlist, "$%d")
			//pos++
			new_columns = append(new_columns, column)
			column_names = append(column_names, column.Name)
		} else if mytype == "geometry" {
			if column.GivenSRID == 0 {
				column.GivenSRID = DefaultSRID
			}
			if column.TargetSRID == 0 {
				column.TargetSRID = DefaultSRID
			}
			var insertval string
			if column.GivenSRID != column.TargetSRID {
				insertval = "ST_Transform(" + "ST_GeomFromWKB($%d," + strconv.Itoa(column.GivenSRID) + ")" + fmt.Sprintf(",%d)", column.TargetSRID)
			} else {
				insertval = "ST_GeomFromWKB($%d," + strconv.Itoa(column.GivenSRID) + ")"
			}
			//insertval := "ST_GeomFromWKB($%d," + strconv.Itoa(column.GivenSRID) + ")"
			insertlist = append(insertlist, insertval)
			//pos++
			new_columns = append(new_columns, column)
			column_names = append(column_names, column.Name)
		}
		if !hstore_val {
			createlist = append(createlist, val)
		}
	}

	hstore_string := ""
	if hstore_bool {
		val := strings.Join(hstore_lines, "\n")
		val = val[:len(val)-1]
		hstore_string = val
		columns = new_columns
	}
	column_string := strings.Join(column_names, ", ")
	createval := strings.Join(createlist, ",")
	createstmt := fmt.Sprintf("CREATE TABLE %s (%s);", tablename, createval)
	insertval := strings.Join(insertlist, ",")
	insertstmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES ", tablename, column_string)
	insertvalupdate := fmt.Sprintf("(%s), ", insertval)

	p, err := pgx.NewConnPool(config)
	if err != nil {
		return &Table{}, err
	}

	// creating extension for postgis
	_, _ = p.Exec("create extension postgis;")

	// creating extension for hstore
	_, _ = p.Exec("create extension hstore;")

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
		TableName:          tablename,
		InsertStmt:         insertstmt,
		CreateStmt:         createstmt,
		ColumnMap:          columnmap,
		Tx:                 tx,
		Columns:            columns,
		Inserts:            0,
		Conn:               p,
		InsertValue:        insertvalupdate,
		CurrentInsertStmt:  insertstmt,
		HStoreColumns:      hstore_columns,
		HStoreFormatString: hstore_string,
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

func ValidPolygonFeature(feature *geojson.Feature) bool {
	// simpl
	totalbool := false
	totalbool = len(feature.Geometry.Polygon) == 0
	if feature.Geometry.Type == "Polygon" {
		for _, i := range feature.Geometry.Polygon {
			boolval := len(i) < 4
			if boolval {
				totalbool = true
			}
		}
	} else {
		for _, i := range feature.Geometry.MultiPolygon {
			for _, ii := range i {
				boolval := len(ii) < 4
				if boolval {
					totalbool = true
				}
			}
		}
	}
	return !totalbool
}

// adds a feature to the postgis table
func (table *Table) AddFeature(feature *geojson.Feature) error {
	if strings.Contains(string(feature.Geometry.Type), "Polygon") {
		totalbool := ValidPolygonFeature(feature)
		if !totalbool {
			return errors.New("Polygon Geometry Invalid.")
		}
	}

	newlist := []interface{}{}
	newlist2 := []interface{}{}
	for pos, i := range table.Columns {
		newlist2 = append(newlist2, table.Count*len(table.Columns)+pos+1)
		if string(i.Type) == "hstore_tags" {
			hstore_vals := []interface{}{}
			for _, name := range table.HStoreColumns {
				hstore_vals = append(hstore_vals, fmt.Sprint(feature.Properties[name]))
			}
			newval := fmt.Sprintf(table.HStoreFormatString, hstore_vals...)
			newlist = append(newlist, newval)

		} else if string(i.Name) == "geometry" {
			geomb, err := EncodeGeometryWKB(feature.Geometry)
			if err != nil {
				return err
			}

			newlist = append(newlist, geomb)
		} else {
			val, boolval := feature.Properties[i.Name]
			//fmt.Println(i, val, boolval)x
			if !boolval {
				newlist = append(newlist, nil)
			} else {
				newlist = append(newlist, fmt.Sprint(val))
			}

		}
	}
	table.CurrentInterfaceList = append(table.CurrentInterfaceList, newlist...)
	table.CurrentInsertStmt += fmt.Sprintf(table.InsertValue, newlist2...)
	table.Count++

	//
	if table.Count != DefaultIncrement {

	} else {
		//var oldtable *Table
		//*oldtable = *table
		//go func(oldtable *pgpush.Table) {
		table.CurrentInsertStmt = table.CurrentInsertStmt[0 : len(table.CurrentInsertStmt)-2]
		table.Tx.Exec(table.CurrentInsertStmt, table.CurrentInterfaceList...)
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
