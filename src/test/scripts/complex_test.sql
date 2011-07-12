add jar ../../../target/json-serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

DROP TABLE json_complex_test;
CREATE TABLE json_complex_test (
	country string,
	languages array<string>,
	religions map<string,string>)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'complexdata.txt' OVERWRITE INTO TABLE  json_complex_test ;

select * from json_complex_test;
select languages[0] from json_complex_test;
select religions['catholic'] from json_complex_test;
