-- happiest country
select c.name, sum(t.sentiment) as total_sentiment
from tweets_data.tweets t
         inner join tweets_data.locations l on t.location_id = l.id
         inner join tweets_data.countries c on l.country_id = c.id
group by c.name
order by total_sentiment desc
limit 1;
-- result:
-- United States|23


-- happiest location
select l.name, sum(t.sentiment) as total_sentiment
from tweets_data.tweets t
         inner join tweets_data.locations l on t.location_id = l.id
group by l.name
order by total_sentiment desc
limit 1;
-- result:
-- Polska|9


-- happiest user tweets
select u.name, t.text
from tweets_data.tweets t
         inner join tweets_data.users u on t.user_id = u.id
where u.name = (select name
                from (select u.name, sum(t.sentiment) as total_sentiment
                      from tweets_data.tweets t
                               inner join tweets_data.users u on t.user_id = u.id
                      group by u.name
                      order by total_sentiment desc
                      limit 1) as happiest_user_name);
-- result:

-- BIRTHDAY GIRL ✨|Hi @ShawnMendes 😘
-- Today is my birthday🎈
-- Can you follow me? This would be the best a birthday gift ever. 🎁
-- I love you soo much! 💕
-- x12,098

-- BIRTHDAY GIRL ✨|Hi @ShawnMendes 😘
-- Today is my birthday🎈
-- Can you follow me? This would be the best a birthday gift ever. 🎁
-- I love you soo much! 💕
-- x12,134