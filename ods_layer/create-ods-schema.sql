DROP SCHEMA ODS_LAYER CASCADE;
CREATE SCHEMA IF NOT EXISTS ODS_LAYER;

/* Table location */
CREATE TABLE IF NOT EXISTS ODS_LAYER.location (
    location_id     SERIAL      PRIMARY KEY,
    address         TEXT,
    city            TEXT,
    state           TEXT,
    postal_code     TEXT,
    latitude        FLOAT,
    longitude       FLOAT
);

/* 
==========================================================================================================
Create Business table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.business (
    business_id         TEXT   PRIMARY KEY,
    name                TEXT,
    location_id         INT,
    stars               NUMERIC(3,2),
    review_count        INT,
    is_open             INT,
    categories          TEXT,
    CONSTRAINT FK_BUSINESS_LOCATION_ID FOREIGN KEY(location_id) REFERENCES ODS_LAYER.location(location_id)
);


/* 
==========================================================================================================
Create Business Attributes table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.business_attributes (
    business_id                 TEXT     PRIMARY KEY,
    NoiseLevel                  TEXT,
    BikeParking                 TEXT,
    RestaurantsAttire           TEXT,
    BusinessAcceptsCreditCards  TEXT,
    BusinessParking             TEXT,
    RestaurantsReservations     TEXT,
    GoodForKids                 TEXT,
    RestaurantsTakeOut          TEXT,
    Caters                      TEXT,
    WiFi                        TEXT,
    RestaurantsDelivery         TEXT,
    HasTV                       TEXT,
    RestaurantsPriceRange2      TEXT,
    Alcohol                     TEXT,
    Music                       TEXT,
    BusinessAcceptsBitcoin      TEXT,
    GoodForDancing              TEXT,
    DogsAllowed                 TEXT,
    BestNights                  TEXT,
    RestaurantsGoodForGroups    TEXT,
    OutdoorSeating              TEXT,
    HappyHour                   TEXT,
    RestaurantsTableService     TEXT,
    GoodForMeal                 TEXT,
    WheelchairAccessible        TEXT,
    Ambience                    TEXT,
    CoatCheck                   TEXT,
    DriveThru                   TEXT,
    Smoking                     TEXT,
    BYOB                        TEXT,
    Corkage                     TEXT,
    CONSTRAINT FK_BUSINESS_ATTRIBUTE_BUSINESS_ID FOREIGN KEY(business_id) REFERENCES ODS_LAYER.business(business_id)
);

/* 
==========================================================================================================
Create Business Hours table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.business_hours (
    business_id TEXT PRIMARY KEY,
    monday      TEXT,
    tuesday     TEXT,
    wednesday   TEXT,
    thursday    TEXT,
    friday      TEXT,
    saturday    TEXT,
    sunday      TEXT,
    CONSTRAINT FK_BUSINESS_HOURS_BUSINESS_ID FOREIGN KEY(business_id) REFERENCES ODS_LAYER.business(business_id)
);


/* 
==========================================================================================================
Create Date Time table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.date_time (
    timestamp   TIMESTAMP PRIMARY KEY,
    date        DATE,
    day         INT,
    month       INT,
    year        INT,
    week        INT,
    quarter     INT
);

/* 
==========================================================================================================
Create User table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.user (
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
    compliment_photos   INT,
    CONSTRAINT FK_USER_DATE_TIME_ID FOREIGN KEY(yelping_since) REFERENCES ODS_LAYER.date_time(timestamp)
);

/* 
==========================================================================================================
Create Tip table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.tip (
    tip_id              SERIAL  PRIMARY KEY,
    user_id             TEXT,
    business_id         TEXT,
    text                TEXT,
    timestamp           TIMESTAMP,
    compliment_count    INT,
    CONSTRAINT FK_TIP_BUSINESS_ID  FOREIGN KEY(business_id)    REFERENCES  ODS_LAYER.business(business_id),
    CONSTRAINT FK_TIP_USER_ID      FOREIGN KEY(user_id)        REFERENCES  ODS_LAYER.user(user_id),
    CONSTRAINT FK_TIP_DATE_TIME_ID FOREIGN KEY(timestamp)      REFERENCES  ODS_LAYER.date_time(timestamp)
);

/* 
==========================================================================================================
Create Review table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.review (
    review_id           TEXT   PRIMARY KEY,
    user_id             TEXT,
    business_id         TEXT,
    stars               NUMERIC(3,2),
    useful              BOOLEAN,
    funny               BOOLEAN,
    cool                BOOLEAN,
    text                TEXT,
    timestamp           TIMESTAMP,
	CONSTRAINT FK_REVIEW_BUSINESS_ID FOREIGN KEY(business_id)    REFERENCES  ODS_LAYER.business(business_id),
    CONSTRAINT FK_REVIEW_USER_ID FOREIGN KEY(user_id)            REFERENCES  ODS_LAYER.user(user_id),
    CONSTRAINT FK_REVIEW_DATE_TIME_ID FOREIGN KEY(timestamp)     REFERENCES  ODS_LAYER.date_time(timestamp)
);

/* 
==========================================================================================================
Create Chekin table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.checkin (
    checkin_id          SERIAL     PRIMARY KEY  ,
    business_id         TEXT,
    timestamp           TIMESTAMP,
    CONSTRAINT FK_CHECKIN_BUSINESS_ID FOREIGN KEY(business_id)    REFERENCES  ODS_LAYER.business(business_id),
    CONSTRAINT FK_CHECKIN_DATE_TIME_ID FOREIGN KEY(timestamp) REFERENCES  ODS_LAYER.date_time(timestamp)
);

/* 
==========================================================================================================
Create Temperature table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.temperature (
    temperature_id              SERIAL     PRIMARY KEY  ,
    date                        DATE,
    temp_min                    FLOAT,
    temp_max                    FLOAT,
    temp_normal_min             FLOAT,
    temp_normal_max             FLOAT
    -- CONSTRAINT FK_TEMP_DATE_TIME_ID FOREIGN KEY(date) REFERENCES  ODS_LAYER.date_time(date)
);

/* 
==========================================================================================================
Create Precipitation table 
==========================================================================================================
*/
CREATE TABLE IF NOT EXISTS ODS_LAYER.precipitation (
    precipitation_id            SERIAL     PRIMARY KEY  ,
    date                        DATE,
    precipitation               FLOAT,
    precipitation_normal        FLOAT
    -- CONSTRAINT FK_PREC_DATE_TIME_ID FOREIGN KEY(date) REFERENCES  ODS_LAYER.date_time(date)
);