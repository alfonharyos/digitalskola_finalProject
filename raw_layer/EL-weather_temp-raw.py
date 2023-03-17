from sqlalchemy import create_engine, Table, Column, Float, MetaData, DATE
from datetime import datetime
import csv

# Create a database engine
engine = create_engine('postgresql://postgres:12345678@localhost/final_project')

# define table object
metadata = MetaData()
weather_temperature = Table('weather_temperature', metadata,
    Column('date', DATE),
	Column('min', Float),
	Column('max', Float),
	Column('normal_min', Float),
	Column('normal_max', Float),
	Column('precipitation_normal', Float),
    schema='raw_layer'
)

# load data from file
with open('data/USW00023169-temperature-degreeF.csv', 'r', encoding='utf-8') as f:
    next(f)  # skip the header row
    values_list = [row for row in csv.reader(f)]

# convert date column to string format of 'YYYYMMDD'
for row in values_list:
    row[0] = datetime.strptime(row[0], '%Y%m%d').strftime('%Y-%m-%d')
    row[1] = None if row[1] == '' else row[1]
    row[2] = None if row[2] == '' else row[2]
    row[3] = None if row[3] == '' else row[3]
    row[4] = None if row[4] == '' else row[4]

# insert data in batches
for values in values_list:
    engine.execute(weather_temperature.insert().values(values))