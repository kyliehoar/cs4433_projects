
access_logs = LOAD '/Users/eloisasalcedo-marx/IdeaProjects/DS503Project0/access_logs.csv' USING PigStorage(',')
    AS (AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:chararray);


access_data = FOREACH access_logs GENERATE ByWho AS UserID, WhatPage AS PageID;


grouped_accesses = GROUP access_data BY UserID;
total_accesses = FOREACH grouped_accesses GENERATE
    group AS UserID,
    COUNT(access_data) AS TotalAccesses;


distinct_pages = DISTINCT access_data;  -- Remove duplicate (UserID, PageID) pairs
grouped_pages = GROUP distinct_pages BY UserID;
page_counts = FOREACH grouped_pages GENERATE
    group AS UserID,
    COUNT(distinct_pages) AS UniquePagesAccessed;


final_result = JOIN total_accesses BY UserID LEFT OUTER, page_counts BY UserID;


final_output = FOREACH final_result GENERATE
    total_accesses::UserID AS UserID,
    total_accesses::TotalAccesses AS TotalAccesses,
    (page_counts::UniquePagesAccessed IS NULL ? 0 : page_counts::UniquePagesAccessed) AS UniquePagesAccessed;


STORE final_result INTO 'outputfavoritespig' USING PigStorage(',');