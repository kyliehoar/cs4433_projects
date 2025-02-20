-- Load the dataset from the HDFS
DATA = LOAD 'hdfs://localhost:9000/project2/pages.csv'
       USING PigStorage(',')
       AS (id:chararray, name:chararray, nationality:chararray, age:int, hobby:chararray);

-- Filter records where nationality is 'Canada'
CANADIANS = FILTER DATA BY nationality == 'Canada';

-- Extract only the name and hobby
RESULT = FOREACH CANADIANS GENERATE name, hobby;

-- Store the result to HDFS in the project2/output directory
STORE RESULT INTO 'hdfs://localhost:9000/project2/output' USING PigStorage(',');
