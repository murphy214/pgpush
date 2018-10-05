### What is it? 

Shoving geospatial data into a table is rather annoying for me usually, and is usually kind of hacky, this library provides an abstraction for creating tables with a certain schema and shoving inserting geojson features into said table. Currently it provides support for things like hstore and handles the logic for getting out those string fields as well. The column list currently assumes a one-to-one mapping of field keys in a geojson feature to fields in the desired table. 

### TO-DO

This project currently has a lot of things it needs to do or address that have not been touched yet...

* **easy** - Support for different geometry projections 
* **medium** - Performance related to concurrent inserts (currently batch executes n row inserts per 1 transaction) 
* **hard** - Data field validation to make sure feauture properties interface values agree with the typing in the schema.
* **medium** - Address Index creation and id columns
* **medium** - A more robust read schema implementation


### Usage 

Below shows a small usage example for the insertion API. 

```golang
package main

import (
	"./postgis_driver"
	"fmt"
	"github.com/jackc/pgx"
	"github.com/paulmach/go.geojson"
)

func main() {
	// database name
	dbname := "testing"

	// pool configuration
	poolconfig := pgx.ConnPoolConfig{
		ConnConfig: pgx.ConnConfig{
			Host:     "localhost",
			Port:     5432,
			Database: dbname,
			User:     "postgres",
		},
		MaxConnections: 10,
	}

	// columns configuration
	// if any string field is given and an hstore column exists
	// each string column will be placed in the hstore field
	columns := []postgis.Column{
		{Name: "strfield1", Type: postgis.VarChar},
		{Name: "strfield2", Type: postgis.VarChar},
		{Name: "strfield3", Type: postgis.VarChar},
		{Name: "hstore_tags", Type: postgis.HStore},
		{Name: "geometry", Type: postgis.Geometry},
	}

	// creaing geojson feature
	mymap := map[string]interface{}{"strfield1": "field1", "strfield2": "field2", "strfield3": "field3"}
	feature := geojson.NewPointFeature([]float64{-90.0, 40.0})
	feature.Properties = mymap

	// creating table
	table, err := postgis.CreateTable("new_table", columns, poolconfig)
	if err != nil {
		fmt.Println(err)
	}

	// adding feature to table
	err = table.AddFeature(feature)
	if err != nil {
		fmt.Println(err)
	}

	// commiting changes
	err = table.Commit()
	if err != nil {
		fmt.Println(err)
	}
}

```

