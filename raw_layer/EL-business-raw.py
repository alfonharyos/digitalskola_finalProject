from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, Numeric, JSON
import json

# Create a database engine
engine = create_engine('postgresql://postgres:12345678@localhost/final_project')

# define table object
metadata = MetaData()
yelp_business = Table('yelp_business', metadata,
    Column('business_id', String),
    Column('name', String),
    Column('address', String),
    Column('city', String),
    Column('state', String),
    Column('postal_code', String),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('stars', Numeric(3,2)),
    Column('review_count', Integer),
    Column('is_open', Integer),
    Column('attributes', JSON),
    Column('categories', String),
    Column('hours', JSON),
    schema='raw_layer'
)

# load data from file
with open('data/yelp/yelp_academic_dataset_business.json', 'r', encoding='utf-8') as f:
    values_list = [json.loads(line) for line in f]

# split data into chunks of 10000 rows
chunk_size = 10000
chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]

# insert data in batches
for chunk in chunks:
    engine.execute(yelp_business.insert().values(chunk))