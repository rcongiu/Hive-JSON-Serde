add jar ../../../target/json-serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

DROP TABLE json_test1;
CREATE TABLE json_test1 (
	one boolean,
	three array<string>,
	two double,
	four string )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;
