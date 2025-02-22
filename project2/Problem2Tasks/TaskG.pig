access_logs = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/access_logs.csv' USING PigStorage(',') AS (AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:chararray);

pages = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv' USING PigStorage(',') AS (PersonID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

person_access = JOIN pages by PersonID LEFT OUTER, access_logs by ByWho;

flattened_data = FOREACH person_access GENERATE PersonID, Name, ToDate(AccessTime, 'yyyy-MM-dd HH:mm:ss') AS date, CurrentTime() as now;

daysBetween = FOREACH flattened_data GENERATE PersonID, Name, DaysBetween(now, date) AS days_between;

connected = FILTER daysBetween by days_between < 14;

connected_ids = DISTINCT (FOREACH connected GENERATE PersonID, Name);

all_ids = DISTINCT (FOREACH flattened_data GENERATE PersonID, Name);

ids_disconnected = JOIN all_ids by PersonID LEFT OUTER, connected_ids by PersonID;

disconnected = FILTER ids_disconnected BY connected_ids::pages::PersonID IS NULL;

result = FOREACH disconnected GENERATE all_ids::pages::PersonID, all_ids::pages::Name;

STORE result INTO '/Users/kyliehoar/Downloads/CS4433/project2/pig/outputTaskG' USING PigStorage(',');
