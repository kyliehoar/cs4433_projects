friends = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/friends.csv' USING PigStorage(',') AS (FriendRel:int, PersonID:int, MyFriend:int, DateOfFriendship:chararray, Desc:chararray);

pages = LOAD '/Users/kyliehoar/IdeaProjects/cs4433_project1/pages.csv' USING PigStorage(',') AS (PersonID:int, Name:chararray, Nationality:chararray, CountryCode:int, Hobby:chararray);

friends_grouped = GROUP friends by MyFriend;

friend_counts = FOREACH friends_grouped GENERATE group AS PersonID, COUNT(friends) AS num_friends;

total_friendships = FOREACH (GROUP friend_counts ALL) GENERATE SUM(friend_counts.num_friends) AS total_friends;

total_users = FOREACH (GROUP pages ALL) GENERATE COUNT(pages) AS total_users;

crossed = CROSS total_friendships, total_users;

average_friendships = FOREACH crossed GENERATE total_friends/total_users AS avg_friends;

ids_and_names = JOIN pages by PersonID, friend_counts BY PersonID;

popular_people = FILTER ids_and_names BY friend_counts::num_friends > average_friendships.avg_friends;

result = FOREACH popular_people GENERATE pages::PersonID, pages::Name;

STORE result INTO '/Users/kyliehoar/Downloads/CS4433/project2/pig/outputTaskH' USING PigStorage(',');
