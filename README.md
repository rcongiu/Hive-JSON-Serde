JsonSerde - a read/write SerDe for JSON Data
================================================

AUTHOR: Roberto Congiu <rcongiu@yahoo.com>

Serialization/Deserialization module for Apache Hadoop Hive

This module allows hive to read and write in JSON format (see http://json.org for more info).

Features:
* Read data stored in JSON format
* Convert data to JSON format when INSERT INTO table
* arrays and maps are supported
* nested data structures are also supported. 
* modular to support multiple versions of CDH

BINARIES
----------
github used to allow uploading of binaries, but not anymore.
Many people have been asking me for binaries in private by email
so I decided to upload binaries here:

http://www.congiu.net/hive-json-serde/

so you don't need to compile your own. There are versions for
CDH4 and CDH5.


COMPILE
---------

Use maven to compile the serde.
The project uses maven profiles to support multiple 
version of hive/CDH. 
To build for CDH4:

```
mvn -Pcdh4 clean package
```

To build for CDH5:
```
mvn -Pcdh5 clean package
```

the serde will be in 
```
json-serde/target/json-serde-VERSION-jar-with-dependencies.jar
```


```bash
$ mvn package

# If you want to compile the serde against a different
# version of the cloudera libs, use -D:
$ mvn -Dcdh.version=0.9.0-cdh3u4c-SNAPSHOT package
```



Hive 0.14.0 and 1.0.0
-----------

Compile with
```
mvn -Pcdh5 -Dcdh5.hive.version=1.0.0 clean package
```


EXAMPLES
------------

Example scripts with simple sample data are in src/test/scripts. Here some excerpts:

### Query with complex fields like arrays

```sql
CREATE TABLE json_test1 (
	one boolean,
	three array<string>,
	two double,
	four string )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'data.txt' OVERWRITE INTO TABLE  json_test1 ;
hive> select three[1] from json_test1;

gold
yellow
```

If you have complex json it can become tedious to create the table 
by hand. I recommend [hive-json-schema)(https://github.com/quux00/hive-json-schema) to build your schema from the data.


### Nested structures

You can also define nested structures:

```sql
add jar ../../../target/json-serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

CREATE TABLE json_nested_test (
	country string,
	languages array<string>,
	religions map<string,array<int>>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

-- data : {"country":"Switzerland","languages":["German","French",
-- "Italian"],"religions":{"catholic":[10,20],"protestant":[40,50]}}

LOAD DATA LOCAL INPATH 'nesteddata.txt' OVERWRITE INTO TABLE  json_nested_test ;

select * from json_nested_test;  -- result: Switzerland	["German","French","Italian"]	{"catholic":[10,20],"protestant":[40,50]}
select languages[0] from json_nested_test; -- result: German
select religions['catholic'][0] from json_nested_test; -- result: 10
```

### SUPPORT FOR ARRAYS
You could have JSON arrays, in that case the SerDe would still work, 
and it will expect data in the JSON arrays ordered just like the hive
columns, like you'd see in the regular text/csv serdes.
For instance, if you do
```sql
CREATE TABLE people ( name string, age int)
```
your data should look like
```javascript
["John", 26 ]
["Mary", 23 ]
```
Arrays can still be nested, so you could have
```sql
CREATE TABLE complex_array ( 
	name string, address struct<street:string,city:string>) ...
-- data:
["John", { street:"10 green street", city:"Paris" } .. ]
```


### MALFORMED DATA

The default behavior on malformed data is throwing an exception. 
For example, for malformed json like 
{"country":"Italy","languages" "Italian","religions":{"catholic":"90"}}

you get:
Failed with exception java.io.IOException:org.apache.hadoop.hive.serde2.SerDeException: Row is not a valid JSON Object - JSONException: Expected a ':' after a key at 32 [character 33 line 1]

this may not be desirable if you have a few bad lines you wish to ignore. If so you can do:
```sql
ALTER TABLE json_table SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");
```

it will not make the query fail, and the above record will be returned as
NULL	null	null


### UNIONTYPE support (PLEASE READ IF YOU USE IT)

A Uniontype is a field that can contain different types, like in C.
Hive usually stores a 'tag' that is basically the index of the datatype,
for instance, if you create a uniontype<int,string,float> , tag would be
0 for int, 1 for string, 2 for float (see https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-UnionTypes).

Now, JSON data does not store anything like that, so the serde will try and
look what it can do.. that is, check, in order, if the data is compatible
with any of the given types. So, THE ORDER MATTERS. Let's say you define
a field f as UNIONTYPE<int,string> and your js has
```{json}
{ "f": "123" }  // parsed as int, since int precedes string in definitions,
                // and "123" can be parsed to a number
{ "f": "asv" }  // parsed as string
```
That is, a number in a string. This will return a tag of 0 and an int rather
than a string.
It's worth noticing that complex Union types may not be that efficient, since
the SerDe may try to parse the same data in several ways; however, several
people asked me to implement this feature to cope with bad JSON, so..I did.




### MAPPING HIVE KEYWORDS

Sometimes it may happen that JSON data has attributes named like reserved words in hive.
For instance, you may have a JSON attribute named 'timestamp', which is a reserved word 
in hive, and hive will fail when issuing a CREATE TABLE.
This SerDe can map hive columns over attributes named differently, using SerDe properties.

For instance:
CREATE TABLE mytable (
	myfield string,
        ts string ) ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES ( "mapping.ts" = "timestamp" )
STORED AS TEXTFILE;

Notice the "mapping.ts", that means: take the column 'ts' and read into it the 
JSON attribute named "timestamp"


### ARCHITECTURE

For the JSON encoding/decoding, I am using a modified version of Douglas Crockfords JSON library:
https://github.com/douglascrockford/JSON-java
which is included in the distribution. I had to make some minor changes to it, for this reason
I included it in my distribution and moved it to another package (since it's included in hive!)

The SerDe builds a series of wrappers around JSONObject. Since serialization and deserialization
are executed for every (and possibly billions) record we want to minimize object creation, so
instead of serializing/deserializing to an ArrayList, I kept the JSONObject and built a cached
objectinspector around it. So when deserializing, hive gets a JSONObject, and a JSONStructObjectInspector
to read from it. Hive has Structs, Maps, Arrays and primitives while JSON has Objects, Arrays and primitives.
Hive Maps and Structs are both implemented as object, which are less restrictive than hive maps: 
a JSON Object could be a mix of keys and values of different types, while hive expects you to declare the 
type of map (example: map<string,string>). The user is responsible for having the JSON data structure 
match hive table declaration.

More detailed explanation on my blog:
http://www.congiu.com/articles/json_serde

### Notes

#### Timestamp support
note that timestamp support will use the systems default timezone
to convert timestamps.


### CONTRIBUTING

I am using gitflow for the release cycle.


### THANKS
 
Thanks to Douglas Crockford for the liberal license for his JSON library, and thanks to 
my employer OpenX and my boss Michael Lum for letting me open source the code.



Versions:
* 1.0: initial release
* 1.1: fixed some string issues
* 1.1.1 (2012/07/03): fixed Map Adapter (get and put would call themselves...ooops)
* 1.1.2 (2012/07/26): Fixed issue with columns that are not mapped into JSON, reported by Michael Phung
* 1.1.4 (2012/10/04): Fixed issue #13, problem with floats, Reported by Chuck Connell
* 1.1.6 (2013/07/10): Fixed issue #28, error after 'alter table add columns'
* 1.1.7 (2013/09/30): Fixed issue #25, timestamp support, fix parametrized build,
		    Fixed issue #31 (static member shouldn't be static)
* 1.1.8 (2014/01/22): Rewritten handling of numbers, so their parsing from string is delayed to 
                      deserialization time. Fixes #39, #45, #34, #29, #26, #22, #13
* 1.1.9.1 (2014/02/02) fixed some bugs
* 1.1.9.2 (2014/02/25)	fixed issue with { field = null }  #50,
		      	support for array records,
		      	fixed handling of null in arrays #54,
		      	refactored Timestamp Handling
* 1.2     (2014/06)     Refactored to multimodule for CDH5 compatibility
* 1.3     (2014/09/08)  fixed #80, #82, #84, #85
* 1.4     ???? Added UNIONTYPE support (#53), made CDH5 default 




