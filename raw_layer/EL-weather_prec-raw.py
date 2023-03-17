from sqlalchemy import create_engine, Table, Column, Float, MetaData, DATE
from datetime import datetime
import csv

# Create a database engine
engine = create_engine('postgresql://postgres:12345678@localhost/final_project')

# define table object
metadata = MetaData()
weather_precipitation = Table('weather_precipitation', metadata,
    Column('date', DATE),
	Column('precipitation', Float),
	Column('precipitation_normal', Float),
    schema='raw_layer'
)

# load data from file
with open('data/USW00023169-LAS_VEGAS_MCCARRAN_INTL_AP-precipitation-inch.csv', 'r', encoding='utf-8') as f:
    next(f)  # skip the header row
    values_list = [row for row in csv.reader(f)]

# convert date column to string format of 'YYYYMMDD'
for row in values_list:
    row[0] = datetime.strptime(row[0], '%Y%m%d').strftime('%Y-%m-%d')
    row[1] = 0.1 if row[1] == 'T' else row[1] if row[1] else None

# insert data in batches
for values in values_list:
    engine.execute(weather_precipitation.insert().values(values))