import mysql.connector
import time
from datetime import datetime
import requests

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
    except Exception as ex:
        print(ex)
        time.sleep(3)
        continue

    curtime = datetime.now()

    emscur.execute("select polledTime,httpLink,controlStatus,functionCode,strings from EMS.ioeInstantaneous where date(polledTime) = curdate() order by polledTime desc limit 1;")

    res = emscur.fetchall()

    if len(res) > 0:
        try:
            timer = res[0][0]
            Url = res[0][1]
        except Exception as ex:
            print(ex)
            time.sleep(3)
            continue

        print(curtime)
        print(timer)

        secs = (curtime-timer).total_seconds()
        print('seconds:',secs)
        if secs <= 60:
            print(Url)

            ONurl = requests.get(Url)

            print(ONurl.json())
        
        time.sleep(10)
    else:
        print("No data")
        time.sleep(10)