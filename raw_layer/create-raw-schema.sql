CREATE SCHEMA IF NOT EXISTS RAW_LAYER;

CREATE TABLE IF NOT EXISTS RAW_LAYER.yelp_business (
    business_id TEXT,
    name TEXT,
    address TEXT,
    city TEXT,
    state TEXT,
    postal_code TEXT,
    latitude FLOAT,
    longitude FLOAT,
    stars NUMERIC(3,2),
    review_count INT,
    is_open INT,
    attributes JSON,
    categories TEXT,
    hours JSON
);

CREATE TABLE IF NOT EXISTS RAW_LAYER.yelp_checkin (
    business_id TEXT,
    date TEXT
);

CREATE TABLE IF NOT EXISTS RAW_LAYER.yelp_review (
    review_id TEXT,
    user_id TEXT,
    business_id TEXT,
    stars NUMERIC(3,2),
    useful INT,
    funny INT,
    cool INT,
    text TEXT,
    timestamp TIMESTAMP
);

CREATE TABLE IF NOT EXISTS RAW_LAYER.yelp_tip (
    user_id TEXT,
    business_id TEXT,
    text TEXT,
    timestamp TIMESTAMP,
    compliment_count INT
);

CREATE TABLE IF NOT EXISTS RAW_LAYER.yelp_user (
    user_id TEXT,
    name TEXT,
    review_count INT,
    yelping_since TIMESTAMP,
    useful INT,
    funny INT,
    cool INT,
    elite TEXT,
    friends TEXT,
    fans INT,
    average_stars NUMERIC(3,2),
    compliment_hot INT,
    compliment_more INT,
    compliment_profile INT,
    compliment_cute INT,
    compliment_list INT,
    compliment_note INT,
    compliment_plain INT,
    compliment_cool INT,
    compliment_funny INT,
    compliment_writer INT,
    compliment_photos INT
);

CREATE TABLE IF NOT EXISTS RAW_LAYER.weather_precipitation (
	date DATE,
	precipitation FLOAT,
	precipitation_normal FLOAT
	
);

CREATE TABLE IF NOT EXISTS RAW_LAYER.weather_temperature (
	date DATE,
	min FLOAT,
	max FLOAT,
	normal_min FLOAT,
	normal_max FLOAT
);