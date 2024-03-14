import requests
import time
from datetime import datetime
import mysql.connector

hwrcurl = 'http://localhost:8008/hotWaterRCon'
hwfwurl = 'http://localhost:8008/hotWaterFWon'
hwdcurl = 'http://localhost:8008/hotWaterDCon'
hwidurl = 'http://localhost:8008/hotWaterIDon'

headers = {'Authorization': 'VKOnNhH2SebMU6S'}

def HwRCON():
    print("Recirculation Charge")
    upsres = requests.get(hwrcurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        print(data)
        HwRCON()
    else:
        print(data)

    time.sleep(50)

def HwFWON():
    print("Freshwater charge")
    upsres = requests.get(hwfwurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        print(data)
        HwFWON()
    else:
        print(data)

    time.sleep(50)

def HwDCON():
    print("Discharge")
    upsres = requests.get(hwdcurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        print(data)
        HwDCON()
    else:
        print(data)

    time.sleep(50)

def HwIDON():
    print("Idle")
    upsres = requests.get(hwidurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        print(data)
        HwIDON()
    else:
        print(data)

    time.sleep(50)

while True:
    try:
        emsdb = mysql.connector.connect(
            host="121.242.232.211",
            user="emsroot",
            password="22@teneT",
            database='EMS',
            port=3306
        )
        emscur = emsdb.cursor()
    except mysql.connector.Error as e:
        print(f"Error connecting to database: {e}")

    emscur.execute("select controlStatus,functionCode,polledTime from EMS.hotWaterInstant order by polledTime desc limit 1;")

    res = emscur.fetchall()

    curtime = datetime.now()

    timer = (curtime-res[0][2]).total_seconds()

    print(timer,"in secs")

    if timer <= 60:
        print(res[0][0],res[0][1])
        if res[0][0] == "DCHG":
            if res[0][1] == "ON":
                HwDCON()
            elif res[0][1] == "OFF":
                HwIDON()
        
        if res[0][0] == "CHGRW":
            if res[0][1] == "ON":
                HwRCON()
            elif res[0][1] == "OFF":
                HwIDON()

        if res[0][0] == "CHGFW":
            if res[0][1] == "ON":
                HwFWON()
            elif res[0][1] == "OFF":
                HwIDON()
        
    time.sleep(10)