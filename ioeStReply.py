from kafka import KafkaConsumer
from flask import Flask, jsonify, request
import time
from collections import deque
from datetime import datetime
import mysql.connector

app = Flask(__name__)


def batteryProcess(hex_data):
    batteryStsDict = {}
    batteryVolt = hex_data[18:20]+hex_data[15:18] 
    batteryVolt = int(batteryVolt,16)/10

    batteryCurent = hex_data[24:26]+ hex_data[21:23]
    batteryCurent = int(batteryCurent,16)/100

    preConSts = hex_data[27]
    mainConsSts = hex_data[28]

    batterySts = hex_data[31]
    print(batterySts)
    if batterySts == '2':
        batterySts = 'IDLE'
    elif batterySts == '3':
        if batteryCurent > 3:
            batterySts = 'CHG'
        else:
            batterySts = 'IDLE'
    elif batterySts == '4':
                    # print(batteryCurent)
        if batteryCurent > 3:
            batterySts = 'DCHG'
        else:
            batterySts = 'IDLE'
    elif batterySts == '5':
        batterySts = 'FAULT'

    batteryStsDict['batteryVolt'] = batteryVolt
    batteryStsDict['batteryCurent']  = batteryCurent
    batteryStsDict['mainConsSts'] = mainConsSts
    batteryStsDict['preConSts'] = preConSts
    batteryStsDict['batterySts'] = batterySts

    return batteryStsDict

def get_last_record_timestamp(table):
    try:
        unprocesseddb = mysql.connector.connect(
                host="121.242.232.211",
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )
    
        ltocur = unprocesseddb.cursor()
    except Exception as ex:
        print(ex)
        return "Mysql Connection Error"
    
    try:
        ltocur.execute(f"SELECT recordTimestamp FROM {table} where date(recordTimestamp) = curdate() order by recordId desc limit 1;")
        res = ltocur.fetchall()
        ltocur.close()
        return res if res else None
    except Exception as ex:
        print(ex)
        ltocur.close()
        return None

@app.route('/ioest1reply', methods = ['GET'])
def ioest1():
    if 1==1:
        curtime = datetime.now()

        res = get_last_record_timestamp("ioeSt1BatteryData")

        if len(res) > 0:
            lasttime = res[0][0]
            time_diff = (curtime - lasttime).total_seconds()
            if time_diff <= 360:
                try:
                    consumer = KafkaConsumer(
                        'ioeBattery',  # Specify the topic(s) to subscribe to
                        bootstrap_servers='43.205.196.66:9092',  # Specify the Kafka server(s)
                        enable_auto_commit=True,  # Automatically commit offsets
                        auto_commit_interval_ms=1000,  # Auto commit interval in milliseconds
                    )

                except Exception as ex:
                    print(ex)
                    return "Kafka Error"


                for message in consumer:
                    hex_data = message.value.decode('utf-8')
                    if len(hex_data) > 0:
                        if hex_data[9:11] == '11' and hex_data[6:8] == '03':
                            # 88 18 03 13 0C AE 1A 25 1E 21 24 26 2A 88 18 13 13 0C 00 00 17 D0 3F 00 00 00
                            batteryStsDict = batteryProcess(hex_data)

                            print(batteryStsDict)

                            return batteryStsDict
                        else:
                            continue
            else:
                return f"Last polledDate {lasttime}"
        else:
            return f"No data till {curtime}"


@app.route('/ioest2reply', methods = ['GET'])
def ioest2():
    if 1==1:

        curtime = datetime.now()

        res = get_last_record_timestamp("ioeSt2BatteryData")

        if len(res) > 0:
            lasttime = res[0][0]
            time_diff = (curtime - lasttime).total_seconds()
            if time_diff <= 360:
                try:
                    consumer = KafkaConsumer(
                        'ioeBattery',  # Specify the topic(s) to subscribe to
                        bootstrap_servers='43.205.196.66:9092',  # Specify the Kafka server(s)
                        enable_auto_commit=True,  # Automatically commit offsets
                        auto_commit_interval_ms=1000,  # Auto commit interval in milliseconds
                    )

                except Exception as ex:
                    print(ex)
                    return "Kafka Error"


                for message in consumer:
                    hex_data = message.value.decode('utf-8')
                    if len(hex_data) > 0:
                        if hex_data[9:11] == '12' and hex_data[6:8] == '03':
                            # 88 18 03 13 0C AE 1A 25 1E 21 24 26 2A 88 18 13 13 0C 00 00 17 D0 3F 00 00 00
                            batteryStsDict = batteryProcess(hex_data)

                            print(batteryStsDict)

                            return batteryStsDict
                        else:
                            continue
            else:
                return f"Last polledDate {lasttime}"
        else:
            return f"No data till {curtime}"

@app.route('/ioest3reply', methods = ['GET'])
def ioest3():
    if 1==1:
        curtime = datetime.now()

        res = get_last_record_timestamp("ioeSt3BatteryData")

        if len(res) > 0:
            lasttime = res[0][0]
            time_diff = (curtime - lasttime).total_seconds()
            if time_diff <= 360:
                try:
                    consumer = KafkaConsumer(
                        'ioeBattery',  # Specify the topic(s) to subscribe to
                        bootstrap_servers='43.205.196.66:9092',  # Specify the Kafka server(s)
                        enable_auto_commit=True,  # Automatically commit offsets
                        auto_commit_interval_ms=1000,  # Auto commit interval in milliseconds
                    )

                except Exception as ex:
                    print(ex)
                    return "Kafka Error"


                for message in consumer:
                    hex_data = message.value.decode('utf-8')
                    if len(hex_data) > 0:
                        if hex_data[9:11] == '13' and hex_data[6:8] == '03':
                            # 88 18 03 13 0C AE 1A 25 1E 21 24 26 2A 88 18 13 13 0C 00 00 17 D0 3F 00 00 00
                            batteryStsDict = batteryProcess(hex_data)

                            print(batteryStsDict)

                            return batteryStsDict
                        else:
                            continue
            else:
                return f"Last polledDate {lasttime}"
        else:
            return f"No data till {curtime}"

@app.route('/ioest4reply', methods = ['GET'])
def ioest4():
    if 1==1:
        curtime = datetime.now()

        res = get_last_record_timestamp("ioeSt4BatteryData")

        if len(res) > 0:
            lasttime = res[0][0]
            time_diff = (curtime - lasttime).total_seconds()
            if time_diff <= 360:
                try:
                    consumer = KafkaConsumer(
                        'ioeBattery',  # Specify the topic(s) to subscribe to
                        bootstrap_servers='43.205.196.66:9092',  # Specify the Kafka server(s)
                        enable_auto_commit=True,  # Automatically commit offsets
                        auto_commit_interval_ms=1000,  # Auto commit interval in milliseconds
                    )

                except Exception as ex:
                    print(ex)
                    return "Kafka Error"


                for message in consumer:
                    li = []
                    hex_data = message.value.decode('utf-8')
                    if len(hex_data) > 0:
                        if hex_data[9:11] == '14' and hex_data[6:8] == '03':
                            # 88 18 03 13 0C AE 1A 25 1E 21 24 26 2A 88 18 13 13 0C 00 00 17 D0 3F 00 00 00

                            batteryStsDict = batteryProcess(hex_data)

                            print(batteryStsDict)

                            return batteryStsDict
                        else:
                            continue
            else:
                return f"Last polledDate {lasttime}"
        else:
            return f"No data till {curtime}"

@app.route('/ioest5reply', methods = ['GET'])
def ioest5():
    if 1==1:
        curtime = datetime.now()

        res = get_last_record_timestamp("ioeSt5BatteryData")

        if len(res) > 0:
            lasttime = res[0][0]
            time_diff = (curtime - lasttime).total_seconds()
            if time_diff <= 360:
                consumer = KafkaConsumer(
                    'ioeBattery',  # Specify the topic(s) to subscribe to
                    bootstrap_servers='43.205.196.66:9092',  # Specify the Kafka server(s)
                    enable_auto_commit=True,  # Automatically commit offsets
                    auto_commit_interval_ms=1000,  # Auto commit interval in milliseconds
                )


                for message in consumer:
                    li = []
                    batteryStsDict = {}
                    hex_data = message.value.decode('utf-8')
                    if len(hex_data) > 0:
                        if hex_data[9:11] == '15' and hex_data[6:8] == '03':
                            # 88 18 03 13 0C AE 1A 25 1E 21 24 26 2A 88 18 13 13 0C 00 00 17 D0 3F 00 00 00
                            batteryStsDict = batteryProcess(hex_data)

                            print(batteryStsDict)

                            return batteryStsDict
                        else:
                            continue
            else:
                return f"Last polledDate {lasttime}"
        else:
            return f"No data till {curtime}"


if __name__ == '__main__':
    app.run(host="localhost",port=5008)


