add jar ../../../target/json-serde-1.0-SNAPSHOT-jar-with-dependencies.jar;

LOAD DATA LOCAL INPATH 'data.txt' OVERWRITE INTO TABLE  json_test1 ;
