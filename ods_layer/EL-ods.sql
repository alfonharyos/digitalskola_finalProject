TRUNCATE ODS_LAYER.temperature;
TRUNCATE ODS_LAYER.precipitation;
TRUNCATE ODS_LAYER.checkin;
TRUNCATE ODS_LAYER.review;
TRUNCATE ODS_LAYER.location CASCADE;
TRUNCATE ODS_LAYER.business_attributes CASCADE;
TRUNCATE ODS_LAYER.business CASCADE;
TRUNCATE ODS_LAYER.business_hours;
TRUNCATE ODS_LAYER.date_time CASCADE;
TRUNCATE ODS_LAYER.user CASCADE;
TRUNCATE ODS_LAYER.tip CASCADE;




/* 
==========================================================================================================
Insert Business location 
==========================================================================================================
*/
INSERT INTO ODS_LAYER.location (
    address, 
    city, 
    state, 
    postal_code,
    latitude, 
    longitude
)
SELECT DISTINCT ON (YB.state, YB.postal_code, YB.latitude, YB.longitude) 
    YB.address, 
    YB.city, 
    YB.state, 
    YB.postal_code, 
    YB.latitude, 
    YB.longitude
FROM RAW_LAYER.yelp_business AS YB
ORDER BY YB.state, YB.postal_code, YB.latitude, YB.longitude, YB.city, YB.address;


/* 
==========================================================================================================
Insert ODS_LAYER Business 
==========================================================================================================
*/

-- INSERT INTO ODS_LAYER.business (business_id, name, location_id, stars, review_count, is_open, categories)
-- SELECT  YB.business_id,
--         YB.name,
--         OL.location_id,
--         YB.stars,
--         YB.review_count,
--         YB.is_open,
--         YB.categories
-- FROM RAW_LAYER.yelp_business AS YB
-- LEFT JOIN ODS_LAYER.location AS OL
-- ON YB.address = OL.address AND
-- YB.city = OL.city AND
-- YB.state = OL.state AND
-- YB.postal_code = OL.postal_code
-- WHERE YB.business_id NOT IN (SELECT OB.business_id FROM ODS_LAYER.business AS OB)
-- ON CONFLICT (business_id) DO UPDATE
--     SET name = excluded.name,
--         location_id = excluded.location_id,
--         stars = excluded.stars,
--         review_count = excluded.review_count,
--         is_open = excluded.is_open,
--         categories = excluded.categories;

/* 
==========================================================================================================
Insert Business Attributes 
==========================================================================================================
*/
INSERT INTO ODS_LAYER.business_attributes (
    business_id, 
    NoiseLevel, 
    BikeParking, 
    RestaurantsAttire, 
    BusinessAcceptsCreditCards, 
    BusinessParking, 
    RestaurantsReservations, 
    GoodForKids, 
    RestaurantsTakeOut,
    Caters, 
    WiFi, 
    RestaurantsDelivery, 
    HasTV, 
    RestaurantsPriceRange2, 
    Alcohol, 
    Music, 
    BusinessAcceptsBitcoin, 
    GoodForDancing, 
    DogsAllowed, 
    BestNights, 
    RestaurantsGoodForGroups, 
    OutdoorSeating, 
    HappyHour, 
    RestaurantsTableService, 
    GoodForMeal, 
    WheelchairAccessible, 
    Ambience, 
    CoatCheck, 
    DriveThru, 
    Smoking, 
    BYOB, 
    Corkage)
SELECT YB.business_id, 
    attributes->>'NoiseLevel', 
    attributes->>'BikeParking', 
    attributes->>'RestaurantsAttire', 
    attributes->>'BusinessAcceptsCreditCards', 
    attributes->>'BusinessParking', 
    attributes->>'RestaurantsReservations', 
    attributes->>'GoodForKids', 
    attributes->>'RestaurantsTakeOut', 
    attributes->>'Caters', 
    attributes->>'WiFi', 
    attributes->>'RestaurantsDelivery', 
    attributes->>'HasTV', 
    attributes->>'RestaurantsPriceRange2', 
    attributes->>'Alcohol', 
    attributes->>'Music', 
    attributes->>'BusinessAcceptsBitcoin', 
    attributes->>'GoodForDancing', 
    attributes->>'DogsAllowed', 
    attributes->>'BestNights', 
    attributes->>'RestaurantsGoodForGroups', 
    attributes->>'OutdoorSeating', 
    attributes->>'HappyHour', 
    attributes->>'RestaurantsTableService', 
    attributes->>'GoodForMeal', 
    attributes->>'WheelchairAccessible', 
    attributes->>'Ambience', 
    attributes->>'CoatCheck', 
    attributes->>'DriveThru', 
    attributes->>'Smoking', 
    attributes->>'BYOB', 
    attributes->>'Corkage'
FROM RAW_LAYER.yelp_business AS YB;


/* 
==========================================================================================================
Insert Business hours 
==========================================================================================================
*/
INSERT INTO ODS_LAYER.business_hours (
    business_id, 
    monday, 
    tuesday, 
    wednesday, 
    thursday, 
    friday, 
    saturday, 
    sunday)
SELECT YB.business_id, 
    hours->>'Monday', 
    hours->>'Tuesday', 
    hours->>'Wednesday', 
    hours->>'Thursday', 
    hours->>'Friday', 
    hours->>'Saturday', 
    hours->>'Sunday'
FROM RAW_LAYER.yelp_business AS YB;


/*
==========================================================================================================
Loading timestamps - yelping_since - from user table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, date, day, month, year, week, quarter)
SELECT YU.yelping_since,
       DATE(YU.yelping_since),
       DAY(YU.yelping_since),
       MONTH(YU.yelping_since),
       YEAR(YU.yelping_since),
	   WEEK(YU.yelping_since),
	   QUARTER(YU.yelping_since)
FROM RAW_LAYER.yelp_user AS YU
WHERE YU.yelping_since NOT IN (SELECT timestamp FROM ODS_LAYER.date_time);


/*
==========================================================================================================
Loading user data from RAW to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.user (user_id, name, review_count, yelping_since, useful, funny, cool, elite, friends,
                  fans, average_stars, compliment_hot, compliment_more, compliment_profile, compliment_cute,
                  compliment_list, compliment_note, compliment_plain, compliment_cool, compliment_funny,
                  compliment_writer, compliment_photos)
       
SELECT YU.user_id, YU.name, YU.review_count, YU.yelping_since, YU.useful, YU.funny, YU.cool, YU.elite, YU.friends,
       YU.fans, YU.average_stars, YU.compliment_hot, YU.compliment_more, YU.compliment_profile, YU.compliment_cute,
       YU.compliment_list, YU.compliment_note, YU.compliment_plain, YU.compliment_cool, YU.compliment_funny,
       YU.compliment_writer, YU.compliment_photos
FROM RAW_LAYER.yelp_user AS YU
WHERE YU.user_id NOT IN (SELECT user_id FROM ODS_LAYER.user);


/*
==========================================================================================================
Loading timestamps - yelping_tip - from user table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, date, day, month, year, week, quarter)
SELECT YT.timestamp,
       DATE(YT.timestamp),
       DAY(YT.timestamp),
       MONTH(YT.timestamp),
       YEAR(YT.timestamp),
	   WEEK(YT.timestamp),
	   QUARTER(YT.timestamp)
FROM RAW_LAYER.yelp_tip AS YT
WHERE YT.timestamp NOT IN (SELECT timestamp FROM ODS_LAYER.date_time);

/*
==========================================================================================================
Loading tips from staging.yelping_tip table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.tip (user_id, business_id, text, timestamp, compliment_count)
SELECT YT.user_id, YT.business_id, YT.text, YT.timestamp, YT.compliment_count
FROM RAW_LAYER.yelp_tip AS YT;

/*
==========================================================================================================
Loading checkin data from staging.yelping_checkin table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.checkin (business_id, timestamp)
SELECT YC.business_id, 
       CAST(F.value AS DATETIME)AS DATE
FROM   RAW_LAYER.yelp_checkin AS YC, 
       LATERAL Flatten(input => split(YC.DATE,',')) AS F;

/*
==========================================================================================================
Loading timestamps - checkin - from checkin table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, date, day, month, year, week, quarter)
SELECT OC.timestamp,
       DATE(OC.timestamp),
       DAY(OC.timestamp),
       MONTH(OC.timestamp),
       YEAR(OC.timestamp),
	   WEEK(OC.timestamp),
	   QUARTER(OC.timestamp)
FROM ODS_LAYER.checkin AS OC
WHERE OC.timestamp NOT IN (SELECT timestamp FROM ODS_LAYER.date_time);


/*
==========================================================================================================
Loading timestamps - yelping_review - from user table to date_time table
==========================================================================================================
*/
INSERT INTO ODS_LAYER.date_time (timestamp, date, day, month, year, week, quarter)
SELECT YR.timestamp,
       DATE(YR.timestamp),
       DAY(YR.timestamp),
       MONTH(YR.timestamp),
       YEAR(YR.timestamp),
	   WEEK(YR.timestamp),
	   QUARTER(YR.timestamp)
FROM RAW_LAYER.yelp_review AS YR
WHERE YR.timestamp NOT IN (SELECT timestamp FROM ODS_LAYER.date_time);


/*
==========================================================================================================
Loading review data from staging.yelping_review table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.review (review_id, user_id, business_id, stars, useful, funny, cool, text, timestamp)
SELECT  YR.review_id, YR.user_id, YR.business_id, YR.stars, 
		YR.useful, YR.funny, YR.cool, YR.text, YR.timestamp
FROM RAW_LAYER.yelp_review AS YR
WHERE YR.review_id NOT IN (SELECT ODS_LAYER.review_id FROM ODS_LAYER.review);

/*
==========================================================================================================
Loading temperature data from staging.weather_temperature table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.temperature (date, temp_min, temp_max, temp_normal_min, temp_normal_max)
SELECT TO_DATE(RT.date, 'YYYYMMDD'), RT.min, RT.max, RT.normal_min, RT.normal_max
FROM RAW_LAYER.weather_temperature AS RT;

/*
==========================================================================================================
Loading precipitation data from staging.weather_precipitation table to ods
==========================================================================================================
*/
INSERT INTO ODS_LAYER.precipitation (date, precipitation, precipitation_normal)
SELECT TO_DATE(RP.date, 'YYYYMMDD'), RP.precipitation, RP.precipitation_normal
FROM RAW_LAYER.weather_precipitation AS RP;