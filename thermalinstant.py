import mysql.connector
import paho.mqtt.client as mqtt
import time
import datetime
import json

#MQTT broker info
broker_address = "121.242.232.175"
broker_port = 1883 
mqtt_username = "swadha"
mqtt_password = "dhawas@123"

# MQTT topic to be published
mqtt_topic_publish = "swadha/IITMRPTS00000002/IITMRPTS/set"

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("swadha/IITMRPTS00000002/IITMRPTS/log",1)


# Initialize MQTT client
try:
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
    mqtt_client.connect(broker_address, broker_port)
    mqtt_client.on_connect = on_connect
except KeyboardInterrupt:
    mqtt_client.disconnect()
except:
    print("connection lost")
    time.sleep(5)
    mqtt_client.reconnect()
    
# Start the MQTT client background thread
mqtt_client.loop_start()

def instantaneous():
    # MYSQL connection
    emsdb = mysql.connector.connect(
                        host="121.242.232.211",
                        user="emsroot",
                        password="22@teneT",
                        database='EMS',
                        port=3306
                        )
    
    emscur = emsdb.cursor()
    
    emscur.execute("select polledTime,functionCode,controlStatus from EMS.thermalInstantaneous order by polledTime desc limit 1")
    
    data = emscur.fetchall()
    
    current_datetime = datetime.datetime.now()
    mysql_formatted_datetime = current_datetime.strftime("%Y-%m-%d %H:%M:%S")
    datetim = datetime.datetime.strptime(mysql_formatted_datetime,"%Y-%m-%d %H:%M:%S")
    
    timer = (datetim-data[0][0]).total_seconds()
    
    if timer <= 60:
        bms_cntrl = {"BMSCNTRL":1}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        time.sleep(15) 
          
        if data[0][2] == "discharge":
            # "MOD" = AUTO -> 0
            # "VFS" = OFF -> 0 ,ON -> 1
            # BMSCNTRL = OFF -> 0 ,ON -> 1
            if data[0][1] == "ON":
                data_dict = {
                "MOD" : 0,
                "VFS" : 1
                }
            if data[0][1] == "OFF":
                data_dict = {
                "MOD" : 0,
                "VFS" : 0
                }
            json_data = json.dumps(data_dict)
            print(json_data)
            
            

while True: 
    instantaneous()
    time.sleep(60)
