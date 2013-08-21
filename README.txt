JsonSerde - a read/write SerDe for JSON Data
AUTHOR: Roberto Congiu <rcongiu@yahoo.com>

Serialization/Deserialization module for Apache Hadoop Hive

This module allows hive to read and write in JSON format (see http://json.org for more info).

Features:
* Read data stored in JSON format
* Convert data to JSON format when INSERT INTO table
* arrays and maps are supported
* nested data structures are also supported. 

COMPILE

Use maven to compile the serde.

$ mvn package

If you want to compile the serde against a different version of the cloudera libs,
use -D:
mvn -Dcdh.version=0.9.0-cdh3u4c-SNAPSHOT package


EXAMPLES

Example scripts with simple sample data are in src/test/scripts. Here some excerpts:

* Query with complex fields like arrays

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


* Nested structures

You can also define nested structures:
add jar ../../../target/json-serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

CREATE TABLE json_nested_test (
	country string,
	languages array<string>,
	religions map<string,array<int>>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

-- data : {"country":"Switzerland","languages":["German","French","Italian"],"religions":{"catholic":[10,20],"protestant":[40,50]}}
LOAD DATA LOCAL INPATH 'nesteddata.txt' OVERWRITE INTO TABLE  json_nested_test ;

select * from json_nested_test;  -- result: Switzerland	["German","French","Italian"]	{"catholic":[10,20],"protestant":[40,50]}
select languages[0] from json_nested_test; -- result: German
select religions['catholic'][0] from json_nested_test; -- result: 10

* MALFORMED DATA

The default behavior on malformed data is throwing an exception. 
For example, for malformed json like 
{"country":"Italy","languages" "Italian","religions":{"catholic":"90"}}

you get:
Failed with exception java.io.IOException:org.apache.hadoop.hive.serde2.SerDeException: Row is not a valid JSON Object - JSONException: Expected a ':' after a key at 32 [character 33 line 1]

this may not be desirable if you have a few bad lines you wish to ignore. If so you can do:
ALTER TABLE json_table SET SERDEPROPERTIES ( "ignore.malformed.json" = "true");

it will not make the query fail, and the above record will be returned as
NULL	null	null

* MAPPING HIVE KEYWORDS

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

-Another workaround (Tested on HIVE 0.9.0) is to add grave accent (`) around the reserved word for example:
......`schema`:struct<version:int>......

And query this way: SELECT json.`schema`.version from <Table>;


# ARCHITECTURE

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

# CONTRIBUTING

I am using gitflow for the release cycle.


* THANKS
 
Thanks to Douglas Crockford for the liberal license for his JSON library, and thanks to 
my employer OpenX and my boss Michael Lum for letting me open source the code.



Versions:
1.0: initial release
1.1: fixed some string issues
1.1.1 (2012/07/03): fixed Map Adapter (get and put would call themselves...ooops)
1.1.2 (2012/07/26): Fixed issue with columns that are not mapped into JSON, reported by Michael Phung
1.1.4 (2012/10/04): Fixed issue #13, problem with floats, Reported by Chuck Connell
1.1.6 (2013/07/10): Fixed issue #28, error after 'alter table add columns'



