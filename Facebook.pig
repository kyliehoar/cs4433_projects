-- Load the dataset from HDFS
DATA = LOAD 'hdfs://localhost:9000/project2/pages.csv'
       USING PigStorage(',')
       AS (id:chararray, name:chararray, nationality:chararray, age:int, hobby:chararray);

-- Filter records to exclude the header row and ensure nationality is not empty
FILTERED_DATA = FILTER DATA BY nationality IS NOT NULL AND nationality != '' AND id != 'PersonID';

-- Group by nationality
GROUPED_DATA = GROUP FILTERED_DATA BY nationality;

-- Count the occurrences of each nationality
COUNTED_DATA = FOREACH GROUPED_DATA GENERATE group AS nationality, COUNT(FILTERED_DATA) AS count;

-- Store the result to HDFS (task_c output)
STORE COUNTED_DATA INTO 'hdfs://localhost:9000/project2/output_taskc' USING PigStorage(',');
