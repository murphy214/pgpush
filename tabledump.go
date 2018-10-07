package pgpush

import (
	"fmt"
	"github.com/jackc/pgx"
	_ "github.com/lib/pq"
	g "github.com/murphy214/geobuf"
	"github.com/paulmach/go.geojson"
	"os"
	"strings"
)

var SQLUser = "postgres"
var SQLPassword = ""

// this function takes a table database and sql
// and returns the fields that arent geometry
// and the geometry field
func ColumnsGeometryKey(tablename string, database string, db *pgx.ConnPool) ([]string, string) {
	// getting the appropriate columns to build a query
	// we really just need all the but geometry
	result, err := db.Query(fmt.Sprintf("select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '%s';", tablename))
	if err != nil {
		fmt.Println(err)
	}

	columns := []string{}
	var geometrykey string
	var key, typeval string
	for result.Next() {
		// scanning the key and the typeval
		err = result.Scan(&key, &typeval)
		if err != nil {
			fmt.Println(err)
		}

		// collecting the columns
		if typeval == "USER-DEFINED" && strings.Contains(key, "geom") {
			geometrykey = key
		} else {
			columns = append(columns, key)
		}
	}
	return columns, geometrykey
}

// this function gets the srid for a given geometry type
func GetSRID(geometrykey string, tablename string, db *pgx.ConnPool) int {
	sqlstring := fmt.Sprintf(`select ST_SRID(%s) from %s limit 1;`, geometrykey, tablename)
	var srid int64

	err := db.QueryRow(sqlstring).Scan(&srid)
	if err != nil {
		fmt.Println(err)
	}
	return int(srid)
}

// reads all the feautres from an sql table and returns them as a geobuf file
func ReadTableSQL(tablename string, database string, outfilename string) (*g.Reader, error) {
	config := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "localhost",
			Port:     5432,
			Database: database,
			User:     SQLPassword,
		},
		MaxConnections: 1,
	}

	// setting sql password if applicable
	if SQLPassword != "" {
		config.ConnConfig.Password = SQLPassword
	}

	// creating the connection
	p, err := pgx.NewConnPool(config)
	if err != nil {
		fmt.Println(err)
	}

	// getting the columns and geometry key
	columns, geometrykey := ColumnsGeometryKey(tablename, database, p)

	// getting srid number
	srid := GetSRID(geometrykey, tablename, p)

	// adding the st_transform component if needed
	if srid != 4326 {
		geometrykey = fmt.Sprintf("ST_Transform(%s,4326)", geometrykey)
	}

	// creating values needed to form query string
	geometrystring := fmt.Sprintf("ST_AsGeoJSON(%s)", geometrykey)
	valstring := strings.Join(columns, ",")
	querystring := fmt.Sprintf("select %s,%s from %s", valstring, geometrystring, tablename)

	// creating geobuf writer
	buf := g.WriterFileNew(outfilename)

	// creating query and performing query operation
	rows, err := p.Query(querystring)
	if err != nil {
		fmt.Println(err)
	}

	// getting geometry field and trimming keys
	geometrypos := len(columns)
	var geometry geojson.Geometry

	// iterating through rows of results and writing to geobuf file
	for rows.Next() {
		vals, err := rows.Values()
		if err != nil {
			fmt.Println(err)
		}

		// creating the properties map
		tempmap := map[string]interface{}{}
		for pos, key := range columns {
			tempmap[key] = vals[pos]
		}

		// getting geometry
		geometry.Scan(vals[geometrypos])

		// assemblign feature
		feature := &geojson.Feature{Geometry: &geometry, Properties: tempmap}

		// if osm_id is in the properties map add that as an id
		val, boolval := tempmap["osm_id"]
		if boolval {
			valint, boolval2 := val.(int)
			if boolval2 {
				feature.ID = valint
			}
		}

		// adding the feature to geobuf
		buf.WriteFeature(feature)
	}
	return buf.Reader(), err
}

// creates a geojson from a given sql table
func TableToGeoJSON(tablename string, database string, outfilename string) error {
	var geojsonbool bool
	var geobuf_filename, geojsonfilename string
	if strings.HasSuffix(outfilename, "geojson") {
		geojsonbool = true
		geojsonfilename = outfilename
		geobuf_filename = strings.Split(outfilename, ".")[0] + ".geobuf"
	} else {
		geobuf_filename = outfilename
	}

	// creating geobuf
	buf, err := ReadTableSQL(tablename, database, geobuf_filename)
	if err != nil {
		return err
	}

	// converting geobuf to geojson if needed
	if geojsonbool {
		// creating geojson file
		g.ConvertGeobuf(buf.Filename, geojsonfilename)
		os.Remove(buf.Filename)
		fmt.Printf("Output geojson file created %s.\n", geojsonfilename)
	} else {
		fmt.Printf("Output geojson file created %s.\n", geobuf_filename)
	}

	return nil
}
