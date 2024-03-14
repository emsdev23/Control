from datetime import datetime
import time
import psycopg2
from hotWaterReply import hot_water
import requests
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
        conn2 = psycopg2.connect(
            host="10.9.200.203",
            database="cpm_phase_2",
            user="rpuser",
            password="parkPassword")

        cpmcur2 = conn2.cursor()

        emsdb = mysql.connector.connect(
            host="121.242.232.211",
            user="emsroot",
            password="22@teneT",
            database='EMS',
            port=3306
        )
        emscur = emsdb.cursor()
    
    except Exception as ex:
        print("CPM database not connected")
        print(ex)
        continue

    cpmcur2.execute("select act_pwr, device from energymeter where to_timestamp(timestamp)::date = CURRENT_DATE and device = 0 order by timestamp desc limit 1;")

    res = cpmcur2.fetchall()

    power = res[0][0]

    emscur.execute("SELECT tankFuildVolume FROM EMS.HotWaterStorage where date(recordtimestamp) = curdate() order by recordtimestamp desc limit 1;")

    res = emscur.fetchall()

    mass = res[0][0]

    curtime = str(datetime.now())

    mint = int(curtime[11:13])

    print(mint)
    # AutoOn = bytes.fromhex("002D00000006010600100000")
    # hotWaterClient.send(AutoOn)
    # print("Auto On sent")

    if mint >= 21 and mint <= 6:
        if power > 0 and mass!= None:
            if mass < 500:
                print("Chiller 1 ON")

                res = hot_water()

                auto_sts = res['Status']

                print(auto_sts)

                if auto_sts != None:
                    if auto_sts !='CHGFW' and auto_sts != 'IDLE':

                        HwDCON()
                        time.sleep(5)
                        HwFWON()
                    
                    elif auto_sts != 'CHGFW' and auto_sts == 'IDLE':
                        
                        HwFWON()
        if mass >= 800:
            res = hot_water()

            auto_sts = res['Status']

            print(auto_sts)

            if auto_sts != None:
                if auto_sts =='DCHG':
                    HwIDON()

        elif power <= 0:
            print("Chiller 1 OFF")
            res = hot_water()

            auto_sts = res['Status']
            print(auto_sts)
            if auto_sts =='DCHG':
                    HwIDON()

    
    elif mint >= 8 and mint <= 18:
        res = hot_water()
        
        auto_sts = res['Status']
        storedTemp = res['Temp']

        print(auto_sts)
            
        if storedTemp < 40 and (auto_sts == 'IDLE' or auto_sts == 'DCHG'):
            if power > 0:
                print("Chiller 1 is ON")
                HwRCON()
            else:
                print("Chiller 1 is OFF")
        
        elif storedTemp >= 45 and auto_sts == 'CHGRW':
            HwDCON()
        
        elif auto_sts == "CHGRW" and power <= 0:
            HwIDON()

    else:
        res = hot_water()
        
        auto_sts = res['Status']

        print(auto_sts)

        if auto_sts != None:
            HwIDON()

    time.sleep(60)