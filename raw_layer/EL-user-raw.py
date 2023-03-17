from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime, Numeric
import json

# Create a database engine
engine = create_engine('postgresql://postgres:12345678@localhost/final_project')

# define table object
metadata = MetaData()
yelp_user = Table('yelp_user', metadata,
    Column('user_id', String),
    Column('name', String),
    Column('review_count', Integer),
    Column('yelping_since', DateTime),
    Column('useful', Integer),
    Column('funny', Integer),
    Column('cool', Integer),
    Column('fans', Integer),
    Column('elite', String),
    Column('friends', String),
    Column('average_stars', Numeric(3,2)),
    Column('compliment_hot', Integer),
    Column('compliment_more', Integer),
    Column('compliment_profile', Integer),
    Column('compliment_cute', Integer),
    Column('compliment_list', Integer),
    Column('compliment_note', Integer),
    Column('compliment_plain', Integer),
    Column('compliment_cool', Integer),
    Column('compliment_funny', Integer),
    Column('compliment_writer', Integer),
    Column('compliment_photos', Integer),
    schema='raw_layer'
)

# load data from file
with open('data/yelp/yelp_academic_dataset_user.json', 'r', encoding='utf-8') as f:
    values_list = [json.loads(line.replace('date', 'timestamp')) for line in f]

# split data into chunks of 1000 rows
chunk_size = 1000
chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]

# insert data in batches
for chunk in chunks:
    engine.execute(yelp_user.insert().values(chunk))