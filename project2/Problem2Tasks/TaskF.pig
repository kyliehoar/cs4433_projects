friends = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/friends.csv' USING PigStorage(',') AS (FriendRel:int, PersonID:int, MyFriend:int, DateOfFriendship:chararray, Desc:chararray);

access_logs = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/access_logs.csv' USING PigStorage(',') AS (AccessID:int, ByWho:int, WhatPage:int, TypeOfAccess:chararray, AccessTime:chararray);

pages = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv' USING PigStorage(',') AS (PersonID_pages:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

friend_access = JOIN friends by PersonID LEFT OUTER, access_logs by ByWho;

fake_friends = FILTER friend_access by MyFriend != WhatPage;

fake_friends_clean = FOREACH fake_friends GENERATE friends::PersonID AS PersonID;

distinct_fakes = DISTINCT fake_friends_clean;

names_fake_friends = JOIN distinct_fakes by PersonID, pages by PersonID_pages;

result = FOREACH names_fake_friends GENERATE distinct_fakes::PersonID AS PersonID, pages::Name AS Name;

STORE result INTO '/Users/kyliehoar/Downloads/CS4433/project2/pig/outputTaskF' USING PigStorage(',');

