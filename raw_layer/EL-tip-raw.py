from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, DateTime
import json

# Create a database engine
engine = create_engine('postgresql://postgres:12345678@localhost/final_project')

# define table object
metadata = MetaData()
yelp_tip = Table('yelp_tip', metadata,
    Column('user_id', String),
    Column('business_id', String),
    Column('text', String),
    Column('timestamp', DateTime),
    Column('compliment_count', Integer),
    schema='raw_layer'
)

# load data from file
with open('data/yelp/yelp_academic_dataset_tip.json', 'r', encoding='utf-8') as f:
    values_list = [json.loads(line.replace('date', 'timestamp')) for line in f]

# split data into chunks of 10000 rows
chunk_size = 10000
chunks = [values_list[i:i+chunk_size] for i in range(0, len(values_list), chunk_size)]

# insert data in batches
for chunk in chunks:
    engine.execute(yelp_tip.insert().values(chunk))