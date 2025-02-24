
friends = LOAD '/Users/eloisasalcedo-marx/IdeaProjects/DS503Project0/friends.csv' USING PigStorage(',')
    AS (FriendRel:int, PersonID:int, MyFriend:int, DateOfFriendship:chararray, Desc:chararray);

pages = LOAD '/Users/eloisasalcedo-marx/IdeaProjects/DS503Project0/pages.csv' USING PigStorage(',')
    AS (PersonID_pages:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

friend_names = JOIN friends BY MyFriend LEFT OUTER, pages BY PersonID_pages;

friend_cleaned = FOREACH friend_names GENERATE
    friends::MyFriend AS PersonID,
    pages::Name AS Name;

grouped_friends = GROUP friend_cleaned BY PersonID;
friend_counts = FOREACH grouped_friends GENERATE
    group AS PersonID,
    COUNT(friend_cleaned) AS FriendCount;

result = JOIN friend_counts BY PersonID LEFT OUTER, pages BY PersonID_pages;

final_result = FOREACH result GENERATE
    (pages::Name IS NULL ? 'Unknown' : pages::Name) AS Name,
    friend_counts::FriendCount AS FriendCount;

STORE final_result INTO 'output' USING PigStorage(',');
