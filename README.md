### What is it? 

Shoving geospatial data into a table is rather annoying for me usually, and is usually kind of hacky, this library provides an abstraction for creating tables with a certain schema and shoving inserting geojson features into said table. Currently it provides support for things like hstore and handles the logic for getting out those string fields as well. The column list currently assumes a one-to-one mapping of field keys in a geojson feature to fields in the desired table. 



