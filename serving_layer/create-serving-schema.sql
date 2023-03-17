CREATE SCHEMA IF NOT EXISTS SERVING_LAYER;

/* 
==========================================================================================================
Create Dimension table business 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS SERVING_LAYER.dimBusiness (
    business_id                       TEXT   PRIMARY KEY,
    name                              TEXT,
    stars                             NUMERIC(3,2),
    review_count                      INT,
    is_open                           BOOLEAN,
	categories 			              TEXT,
	location_address                  TEXT,
    location_city                     TEXT,
    location_state                    TEXT,
    location_postal_code              INT,
    location_latitude                 FLOAT,
    location_longitude                FLOAT, 
	checkin_date                      TIMESTAMP
);


/* 
==========================================================================================================
Create Dimension table Date
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS SERVING_LAYER.dimDate (
    timestamp           TIMESTAMP    PRIMARY KEY,
    date                DATE,
    day                 INT,
    month               INT,
    year                INT,
	week                INT,
	quarter             INT
);

/* 
==========================================================================================================
Create Dimension table User 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS SERVING_LAYER.dimUser (
    user_id             TEXT      PRIMARY KEY,
    name                TEXT,
    review_count        INT,
    yelping_since       TIMESTAMP,
    useful              INT,
    funny               INT,
    cool                INT,
    elite               TEXT,
    friends             TEXT,
    fans                INT,
    average_stars       NUMERIC(3,2),
    compliment_hot      INT,
    compliment_more     INT,
    compliment_profile  INT,
    compliment_cute     INT,
    compliment_list     INT,
    compliment_note     INT,
    compliment_plain    INT,
    compliment_cool     INT,
    compliment_funny    INT,
    compliment_writer   INT,
    compliment_photos   INT
);


/*
==========================================================================================================
Create Dimension table Weather (Consolidating Temperature and Precipitation) 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS SERVING_LAYER.dimWeather (
    date                        DATE    PRIMARY KEY,
    temp_min                    FLOAT,
    temp_max                    FLOAT,
    temp_normal_min             FLOAT,
    temp_normal_max             FLOAT,
    precipitation               FLOAT,
    precipitation_normal        FLOAT
);


/* 
==========================================================================================================
Create Fact table Review 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS SERVING_LAYER.factReview (
    review_id           TEXT   PRIMARY KEY,
    user_id             TEXT,
    business_id         TEXT,
    stars               NUMERIC(3,2),
    useful              BOOLEAN,
    funny               BOOLEAN,
    cool                BOOLEAN,
    text                TEXT,
    timestamp           TIMESTAMP,
	date                DATE,
	CONSTRAINT FK_FACT_REVIEW_DIMBUSINESS_ID              FOREIGN KEY(business_id)    REFERENCES  SERVING_LAYER.dimBusiness(business_id),
    CONSTRAINT FK_FACT_REVIEW_DIMUSER_ID                  FOREIGN KEY(user_id)        REFERENCES  SERVING_LAYER.dimUser(user_id),
    CONSTRAINT FK_FACT_REVIEW_TIMESTAMP_DIMDATE_TIMESTAMP FOREIGN KEY(timestamp)      REFERENCES  SERVING_LAYER.dimDate(timestamp),
	CONSTRAINT FK_FACT_REVIEW_DATE_DIMWEATHER_DATE        FOREIGN KEY(date)           REFERENCES  SERVING_LAYER.dimWeather(date)
);