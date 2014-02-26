import pandas as pd
import pandas.io.sql as psql
import sqlite3 as lite
import json

file_name = "first_floor_small"
con = lite.connect("/home/nipun/Desktop/funf/" + file_name + ".db")
sql = "select timestamp, value from data where probe='edu.mit.media.funf.probe.builtin.ProximityProbe'"

df = psql.frame_query(sql, con)
df.index = pd.to_datetime(df.timestamp, unit='s')
df = df.drop("timestamp", 1)

# Converting data from json to dict
ser = df.value.apply(json.loads)


# Storing the name and rssi value
df['ssid'] = ser.apply(lambda x: x['SSID'])
df['level'] = ser.apply(lambda x: x['level'])
df['frequency'] = ser.apply(lambda x: x['frequency'])


# Drop the value now as not required and store CSV
df = df.drop("value", 1)
df.to_csv("location_" + file_name + ".csv")
