import pandas as pd
import pandas.io.sql as psql
import sqlite3 as lite
import json

con = lite.connect("/home/nipun/Desktop/funf/first_floor_small.db")
sql = "select timestamp, probe, value from data"

df = psql.frame_query(sql, con)
#Filter bluetooth content
df = df[df.probe == 'edu.mit.media.funf.probe.builtin.BluetoothProbe']
df.index = pd.to_datetime(df.timestamp, unit='s')
df = df.drop("timestamp", 1)
df = df.drop("probe",1)

# Converting data from json to dict
ser = df.value.apply(json.loads)

# Creating a dict MAC->Name
mac_to_name = {}
for x in ser:
    if 'android.bluetooth.device.extra.NAME' in x.keys():
        mac = x['android.bluetooth.device.extra.DEVICE']['mAddress']
        if mac not in mac_to_name.keys():
            mac_to_name[mac]= x['android.bluetooth.device.extra.NAME']

#Storing the name and rssi value
df['name'] = ser.apply(lambda x: mac_to_name[x['android.bluetooth.device.extra.DEVICE']['mAddress']])
df['rssi'] = ser.apply(lambda x: x['android.bluetooth.device.extra.RSSI'])


#Drop the value now as not required and store CSV
df = df.drop("value",1)
df.to_csv("first_floor_small.csv")


