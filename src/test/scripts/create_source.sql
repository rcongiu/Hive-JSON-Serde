add jar ../../../target/json-serde-1.1-jar-with-dependencies.jar;

DROP TABLE   simple_table;
CREATE TABLE simple_table (
	name string,
	country string,
	company string,
	amount int )
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE;

LOAD DATA LOCAL INPATH 'text_data.txt' OVERWRITE INTO TABLE  simple_table ;
