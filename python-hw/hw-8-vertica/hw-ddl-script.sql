CREATE schema IF NOT EXISTS tweets_data;

--
DROP TABLE IF EXISTS tweets_data.countries;
CREATE TABLE IF NOT EXISTS tweets_data.countries
(
    ID   NUMERIC(6, 0) NOT NULL,
    NAME VARCHAR(64)   NOT NULL,
    CODE VARCHAR(64)   NOT NULL,
    PRIMARY KEY (ID) ENABLED
)
    ORDER BY NAME
    UNSEGMENTED ALL NODES;
COPY tweets_data.countries FROM '/home/dbadmin/data/countries.csv' PARSER fcsvparser() abort on error no commit;
-- SELECT * FROM tweets_data.countries;

--
DROP TABLE IF EXISTS tweets_data.locations;
CREATE TABLE IF NOT EXISTS tweets_data.locations
(
    ID         NUMERIC(6, 0) NOT NULL,
    NAME       VARCHAR(128)  NOT NULL,
    COUNTRY_ID NUMERIC(6, 0) NOT NULL,
    PRIMARY KEY (ID) ENABLED
)
    ORDER BY NAME
    UNSEGMENTED ALL NODES;
COPY tweets_data.locations FROM '/home/dbadmin/data/locations.csv' PARSER fcsvparser() abort on error no commit;
-- SELECT * FROM tweets_data.locations;

--
DROP TABLE IF EXISTS tweets_data.users;
CREATE TABLE IF NOT EXISTS tweets_data.users
(
    ID   NUMERIC(6, 0) NOT NULL,
    NAME VARCHAR(128)  NOT NULL,
    PRIMARY KEY (ID) ENABLED
)
    ORDER BY NAME
    UNSEGMENTED ALL NODES;
COPY tweets_data.users FROM '/home/dbadmin/data/users.csv' PARSER fcsvparser() abort on error no commit;
-- SELECT * FROM tweets_data.users;

--
DROP TABLE IF EXISTS tweets_data.tweets;
CREATE TABLE IF NOT EXISTS tweets_data.tweets
(
    ID          NUMERIC(6, 0) NOT NULL,
    TEXT        LONG VARCHAR  NOT NULL,
    SENTIMENT   INTEGER       NOT NULL,
    LANG        VARCHAR       NOT NULL,
    CREATED_AT  DATETIME      NOT NULL,
    LOCATION_ID INTEGER,
    USER_ID     INTEGER       NOT NULL,
    PRIMARY KEY (ID) ENABLED
)
    UNSEGMENTED ALL NODES;
COPY tweets_data.tweets FROM '/home/dbadmin/data/tweets.csv' PARSER fcsvparser() abort on error no commit;
-- SELECT * FROM tweets_data.tweets;
