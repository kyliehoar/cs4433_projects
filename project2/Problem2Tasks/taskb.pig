-- Load the pages metadata from HDFS
PAGES = LOAD 'hdfs://localhost:9000/project2/pages.csv'
        USING PigStorage(',')
        AS (personID:chararray, name:chararray, nationality:chararray, age:int, hobby:chararray);

-- Load the access log data from HDFS (access_logs.csv containing pageID and visitCount)
ACCESS_LOG = LOAD 'hdfs://localhost:9000/project2/access_logs.csv'
             USING PigStorage(',')
             AS (pageID:chararray, visitCount:int);

-- Join the access log data with the pages metadata on personID (from pages) and pageID (from access log)
JOINED_DATA = JOIN ACCESS_LOG BY pageID, PAGES BY personID;

-- Sort the joined data by visitCount in descending order to get the top visited pages
SORTED_DATA = ORDER JOINED_DATA BY visitCount DESC;

-- Limit the results to the top 10 pages (if you need the top 10 pages)
TOP_10_PAGES = LIMIT SORTED_DATA 10;

-- Extract the id, name, and nationality from the top 10 pages
RESULT = FOREACH TOP_10_PAGES GENERATE pageID AS id, name, nationality;

-- Store the output in the desired directory (output_taskb)
STORE RESULT INTO 'hdfs://localhost:9000/project2/output_taskb' USING PigStorage(',');
