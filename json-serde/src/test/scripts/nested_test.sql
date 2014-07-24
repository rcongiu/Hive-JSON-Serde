add jar ../../../target/json-serde-1.1.3-jar-with-dependencies.jar;

DROP TABLE json_nested_test;
CREATE TABLE json_nested_test (
	country string,
	languages array<string>,
	religions map<string,array<int>>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'nesteddata.txt' OVERWRITE INTO TABLE  json_nested_test ;

select * from json_nested_test;
select languages[0] from json_nested_test;
select religions['catholic'][0] from json_nested_test;
