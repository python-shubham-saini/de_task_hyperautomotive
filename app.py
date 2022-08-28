import json
import pandas as pd
import mysql.connector as conn
from pathlib import Path
import datetime

# creating function to convert timestamp to Datatime formate


def tsToDate(timestamp):
    if timestamp == '000000':
        return "0000-00-00"
    else:
        value = datetime.datetime.fromtimestamp(float(timestamp))
        return f"{value:'%Y-%m-%d %H:%M:%S'}"


def dm(x):
    degrees = int(x) // 100
    minutes = x - 100*degrees
    return degrees, minutes


def decimal_degrees(degrees, minutes):
    return degrees + minutes/60


def convertIntoInt(a):
    if type(a) != int:
        return 00
    else:
        return a


# Creating connection with my sql
mydb = conn.connect(host="localhost", user='root', passwd='root')
cursor = mydb.cursor()
# creating data base
cursor.execute('create database hyperautomotive')
cursor.execute("use hyperautomotive")


# Reading Data path
device_regn_path = "./device_regn.csv"
file_list = list(Path("./dsm-api").glob("*.json"))

df = pd.read_csv(device_regn_path)
device_id = df['device_id']
device_private_key = df['private_key']
# print(device_id)


def InsertJsonData():
    # Reading all device_id from device_regn.csv
    for i in range(len(device_id)):
        cursor.execute(
            f"create table {device_id[i]}(private_Key VARCHAR(8), ping_type TINYINT(2) UNSIGNED, rtc TIMESTAMP(6), ses_tim INT(5), lat FLOAT(9), longi FLOAT(10), data_size TINYINT(2) UNSIGNED, data VARCHAR(50), ping_time TIMESTAMP)")

        # Reading all Data from all json files
        for j in range(len(file_list)):
            with open(file_list[j]) as f:
                data = json.load(f)

            for k in range(len(data)):
                text = data[k]['text']
                ts = float(data[k]['ts'])
                text = text.replace("\"\"", "\":\"")

                if "{" in text:
                    text = text.replace(";", ",")
                    x = text.index("'\"")
                    text = str(text[x+2:len(text)-3])
                    textData = json.loads(text)
                    # print('j->', (j), k)
                    # print('textData', textData)

                    if device_private_key[i] == textData['PVT_KEY']:
                        # inserting Data according into tables
                        cursor.execute(
                            f"insert into {device_id[i]} values ('{textData['PVT_KEY']}', {int(textData['PING_TYPE'])},{tsToDate(textData['RTC'])} , {int(textData['Ses_time'])}, {float(decimal_degrees(*dm(float(textData['LAT']))))}, {float(decimal_degrees(*dm(float(textData['LONGI']))))}, 00, {textData['DATA']}, {tsToDate(ts)})")
                        mydb.commit()
                        print("Data Inserted")

                elif "b'" in text:
                    text = text.replace(";", "0")
                    textData = str(text[2:-1])
                    newData = textData.split(",")
                    if device_private_key[i] == newData[0]:
                        # print('file No.->', (j), k)
                        # print('newData ', newData)

                        # inserting Data according into tables
                        cursor.execute(
                            f"insert into {device_id[i]} values ('{newData[0]}', {int(convertIntoInt(newData[1]))},{tsToDate(newData[2])} , {int(newData[3])}, {float(decimal_degrees(*dm(float(newData[4]))))}, {float(decimal_degrees(*dm(float(newData[5]))))}, {newData[6]}, '{newData[7]}', {tsToDate(ts)})")
                        mydb.commit()
                        print("Data Inserted")


InsertJsonData()
