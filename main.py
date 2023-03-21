import csv
import sqlite3
from sqlalchemy import create_engine

conn = sqlite3.connect('database.db')

cursor = conn.cursor()

create_measure = """
   CREATE TABLE IF NOT EXISTS measure (
      station VARCHAR(11),
      date VARCHAR(10) NOT NULL,
      precip float NOT NULL,
      tobs int NOT NULL
   );
   """   

create_stations = """
   CREATE TABLE IF NOT EXISTS stations (
      station VARCHAR(11),
      latitude float NOT NULL,
      lognitude float NOT NULL,
      elevation float NOT NULL,
      name text NOT NULL,
      country text NOT NULL,
      state text NOT NULL
   );
   """

cursor.execute(create_measure)
clean_measure_data = open('clean_measure.csv')
measure_content = csv.reader(clean_measure_data)
insert_clean_measure_data = "INSERT INTO measure (station, date, precip, tobs) VALUES (?, ?, ?, ?)"
cursor.executemany(insert_clean_measure_data, measure_content)
conn.commit()

cursor.execute(create_stations)
clean_stations_data = open('clean_stations.csv')
stations_content = csv.reader(clean_stations_data)
insert_clean_stations_data = "INSERT OR REPLACE INTO stations (station, latitude, lognitude, elevation, name, country, state) VALUES (?, ?, ?, ?, ?, ?, ?)"
cursor.executemany(insert_clean_stations_data, stations_content)
conn.commit()

query = 'SELECT * FROM stations LIMIT 5'

engine = create_engine('sqlite:///database.db')
print(engine.driver)
print(engine.table_names())
print(engine.execute(query))
results = engine.execute(query)
for r in results:
   print(r)

conn.close()