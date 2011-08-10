add jar ../../../target/json-serde-1.1-jar-with-dependencies.jar;

DROP TABLE   simple_table_json;
CREATE TABLE simple_table_json (
	name string,
	country string,
	company string,
	amount int )
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
STORED AS TEXTFILE;

INSERT OVERWRITE TABLE simple_table_json
	SELECT name, country,company,amount
	FROM simple_table;
