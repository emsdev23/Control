import mysql.connector
import time
import logging
import paho.mqtt.client as mqtt
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import json

html_head = """<head>
            <style>
                .container {
                display: flex;
                align-items: center;
                justify-content: center
              }
           
              img {
                max-width: 75%;
                max-height:50%;
            }
           
              .text {
                font-size: 25px;
                padding-left: 20px;
              }
              a:link, a:visited {
                background-color: #8bc9ab;
                color: black;
                padding: 12px 25px;
                text-align: center;
                text-decoration: none;
                display: inline-block;
              }
              td {
                font-size: 19px
              }
              a:hover, a:active {
                background-color: #4c727d;
              }
              hr {
                border-color: green;
              }
                </style>  
        </head>"""
smtp_server = 'smtp.gmail.com'
smtp_port = 587
smtp_username = 'emsteamrp@gmail.com'
smtp_password = 'eebpnvgyfzzdtitb'

sender = 'emsteamrp@gmail.com'
recipient = ['ems@respark.iitm.ac.in','arun.kumar@tenet.res.in']

logging.basicConfig(
    filename='ThermalSchedule.log',  # Specify the log file name
    level=logging.DEBUG,     # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

broker_address = "121.242.232.175"
broker_port = 1883 
mqtt_username = "swadha"
mqtt_password = "dhawas@123"

mqtt_topic_publish = "swadha/IITMRPTS00000002/IITMRPTS/set"

def on_connect(client, userdata, flags, rc):
    print("Connected with result code "+str(rc))
    client.subscribe("swadha/IITMRPTS00000002/IITMRPTS/log",1)

while True:
    cur = datetime.now()

    curtime = str(cur)[0:19]

    mint = (curtime[11:16])

    print(curtime)

    print(mint)

    logger.info(curtime)

    print("Thermal Schedule")
    
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
        time.sleep(10)
        continue

    if mint == '07:55':

        on_time = curtime[0:11] +"07:55:00" 
        
        try:
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="bmsrouser6",
                    password="bmsrouser6@151U",
                    database='bmsmgmtprodv13',
                    port=3306
                )
        
            bmscur = bmsdb.cursor()
        except Exception as ex:
            print(ex)
            time.sleep(10)
            continue
        
        try:
            mqtt_client = mqtt.Client()
            mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
            mqtt_client.connect(broker_address, broker_port)
            mqtt_client.on_connect = on_connect
        except KeyboardInterrupt:
            mqtt_client.disconnec()
        except:
            print("connection lost")
            time.sleep(5)
            mqtt_client.reconnect()
    
        mqtt_client.loop_start()
        # print(mint)
        logger.info(mint)
        bms_cntrl = {"BMSCNTRL":1}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL ON published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(5) 
        data_dict = {
                    "MOD" : 0,
                    "VFS" : 1
                    }
        json_data = json.dumps(data_dict)
        try:
            mqtt_client.publish(mqtt_topic_publish,json_data)
            print("Thermal schedule ON comand")
            logger.info(curtime)
            logger.info("thermal ON command sent")

        except Exception as e:
            print(f"Error occured {e}")
            logger.info((f"Error occured {e}"))
        
        time.sleep(10)
                
        bms_cntrl = {"BMSCNTRL":0}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL OFF published to MQTT broker",bms_data)
            logger.info(("BMS CTRL OFF published to MQTT broker",bms_data))
        except Exception as e:
            print(f"Error occured {e}")
            logger.info(f"Error occured {e}")
        
        message = MIMEMultipart('alternative')
        message['Subject'] = 'EMS ALERT -  Thermal Schedule ON'
        message['From'] = sender
        message['To'] = ', '.join(recipient)

        time.sleep(1200)

        bmscur.execute(f"SELECT vfdStatus,polledTime FROM bmsmgmtprodv13.thermalStorageMQTTReadings where polledTime > '{on_time}' and vfdStatus = 1 order by recordId limit 1;")

        thres = bmscur.fetchall()


        if len(thres) > 0:
            print(thres[0])

            ontime = str(thres[0][1])

            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server ON time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge ON time<b></td>
                                    <td>{ontime}</td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)
            
            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'ON',ontime)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())
        else:
            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server ON time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge ON time<b></td>
                                    <td> - </td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'ON',None)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())

        bmscur.close()
        
        time.sleep(50)

    elif mint == '10:00':

        on_time = curtime[0:11] +"10:00:00" 

        try:
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="bmsrouser6",
                    password="bmsrouser6@151U",
                    database='bmsmgmtprodv13',
                    port=3306
                )
        
            bmscur = bmsdb.cursor()
        except Exception as ex:
            print(ex)
            time.sleep(10)
            continue

        try:
            mqtt_client = mqtt.Client()
            mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
            mqtt_client.connect(broker_address, broker_port)
            mqtt_client.on_connect = on_connect
        except KeyboardInterrupt:
            mqtt_client.disconnec()
        except:
            print("connection lost")
            time.sleep(5)
            mqtt_client.reconnect()
    
        mqtt_client.loop_start()

        logger.info(mint)
        bms_cntrl = {"BMSCNTRL":1}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL ON published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(5) 
        data_dict = {
                "MOD" : 0,
                "VFS" : 0
                }
        json_data = json.dumps(data_dict)
        try:
            mqtt_client.publish(mqtt_topic_publish,json_data)
            print("Thermal schedule OFF comand")
            logger.info(curtime)
            logger.info("thermal ON command sent")
        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(10)
                
        bms_cntrl = {"BMSCNTRL":0}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL OFF published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        logger.info(" ")

        message = MIMEMultipart('alternative')
        message['Subject'] = 'EMS ALERT -  Thermal Schedule OFF'
        message['From'] = sender
        message['To'] = ', '.join(recipient)
        
        time.sleep(1200)


        bmscur.execute(f"SELECT vfdStatus,polledTime FROM bmsmgmtprodv13.thermalStorageMQTTReadings where polledTime > '{on_time}' and vfdStatus = 0 order by recordId limit 1;")

        thres = bmscur.fetchall()


        if len(thres) > 0:
            print(thres[0])

            ontime = str(thres[0][1])

            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server OFF time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge OFF time<b></td>
                                    <td>{ontime}</td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'OFF',ontime)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())
        else:
            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server OFF time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge OFF time<b></td>
                                    <td> - </td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'OFF',None)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())

        bmscur.close()

        time.sleep(50)


    elif mint == '17:55':

        on_time = curtime[0:11] +"17:55:00" 

        try:
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="bmsrouser6",
                    password="bmsrouser6@151U",
                    database='bmsmgmtprodv13',
                    port=3306
                )
        
            bmscur = bmsdb.cursor()
        except Exception as ex:
            print(ex)
            time.sleep(10)
            continue

        try:
            mqtt_client = mqtt.Client()
            mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
            mqtt_client.connect(broker_address, broker_port)
            mqtt_client.on_connect = on_connect
        except KeyboardInterrupt:
            mqtt_client.disconnec()
        except:
            print("connection lost")
            time.sleep(5)
            mqtt_client.reconnect()
    
        mqtt_client.loop_start()

        logger.info(mint)
        bms_cntrl = {"BMSCNTRL":1}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL ON published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(5) 
        data_dict = {
                    "MOD" : 0,
                    "VFS" : 1
                    }
        json_data = json.dumps(data_dict)
        try:
            mqtt_client.publish(mqtt_topic_publish,json_data)
            print("Thermal schedule ON comand")
            logger.info(curtime)
            logger.info("thermal ON command sent")

        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(10)
                
        bms_cntrl = {"BMSCNTRL":0}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL OFF published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        message = MIMEMultipart('alternative')
        message['Subject'] = 'EMS ALERT -  Thermal Schedule ON'
        message['From'] = sender
        message['To'] = ', '.join(recipient)

        
        time.sleep(1200)

        bmscur.execute(f"SELECT vfdStatus,polledTime FROM bmsmgmtprodv13.thermalStorageMQTTReadings where polledTime > '{on_time}' and vfdStatus = 1 order by recordId limit 1;")

        thres = bmscur.fetchall()


        if len(thres) > 0:
            print(thres[0])

            ontime = str(thres[0][1])
            
            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server ON time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge ON time<b></td>
                                    <td>{ontime}</td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'ON',ontime)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())
        else:
            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server ON time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge ON time<b></td>
                                    <td> - </td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'ON',None)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())

        emscur.close()
        bmscur.close()
        
        time.sleep(50)

    elif mint == '22:00':

        on_time = curtime[0:11] +"22:00:00" 

        try:
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="bmsrouser6",
                    password="bmsrouser6@151U",
                    database='bmsmgmtprodv13',
                    port=3306
                )
        
            bmscur = bmsdb.cursor()
        except Exception as ex:
            print(ex)
            time.sleep(10)
            continue

        try:
            mqtt_client = mqtt.Client()
            mqtt_client.username_pw_set(username=mqtt_username, password=mqtt_password)
            mqtt_client.connect(broker_address, broker_port)
            mqtt_client.on_connect = on_connect
        except KeyboardInterrupt:
            mqtt_client.disconnec()
        except:
            print("connection lost")
            time.sleep(5)
            mqtt_client.reconnect()
    
        mqtt_client.loop_start()
        
        logger.info(mint)
        bms_cntrl = {"BMSCNTRL":1}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL ON published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(5) 
        data_dict = {
                "MOD" : 0,
                "VFS" : 0
                }
        json_data = json.dumps(data_dict)
        try:
            mqtt_client.publish(mqtt_topic_publish,json_data)
            print("Thermal schedule OFF comand")
            logger.info(curtime)
            logger.info("thermal ON command sent")
        except Exception as e:
            print(f"Error occured {e}")
        
        time.sleep(10)
                
        bms_cntrl = {"BMSCNTRL":0}
        bms_data = json.dumps(bms_cntrl)
        try:
            mqtt_client.publish(mqtt_topic_publish,bms_data)
            print("BMS CTRL OFF published to MQTT broker",bms_data)
        except Exception as e:
            print(f"Error occured {e}")
        
        logger.info(" ")

        message = MIMEMultipart('alternative')
        message['Subject'] = 'EMS ALERT - Thermal Schedule OFF'
        message['From'] = sender
        message['To'] = ', '.join(recipient)

        time.sleep(1200)

        bmscur.execute(f"SELECT vfdStatus,polledTime FROM bmsmgmtprodv13.thermalStorageMQTTReadings where polledTime > '{on_time}' and vfdStatus = 0 order by recordId limit 1;")

        thres = bmscur.fetchall()

        if len(thres) > 0:
            print(thres[0])

            ontime = str(thres[0][1])

            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server OFF time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge OFF time<b></td>
                                    <td>{ontime}</td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'OFF',ontime)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())
        else:
            html_content = html_head + f"""
                        <body style="background-color:white;">
                        <div class="container">
                            <div class="image">
                                <img src="https://media.licdn.com/dms/image/C560BAQFAVLoL6j71Kg/company-logo_200_200/0/1657630844148?e=1692230400&v=beta&t=yOQNePjpzF0yycZuFep1AcyaXcMmfmt9Lb-5P8xa6L4" height="100px" width="100px">
                            </div>
                            <div class="text">
                                <h3>EMS Alert</h3>
                            </div>
                        </div>
                        <hr>
                        <br>
                        <center>
                            <table>
                                <tr>
                                <td><b>Alert<b></td>
                                <td>Thermal Schedule</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>INFO</td>
                                </tr>
                                <tr>
                                    <td><b>Server ON time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>Discharge ON time<b></td>
                                    <td> - </td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>Thermal Storage</td>
                                </tr>
                            </table>
                            <br>
                            <hr>
                            <p>EMS team</p></center>
                            </body>"""
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            sql = "Insert into EMS.ThermalSchedule(recordTime,Thermalstatus,dischargeTime) values(%s,%s,%s)"
            val = (cur,'OFF',None)
            try:
                print(val)
                logger.info(val)
                emscur.execute(sql,val)
                emsdb.commit()
            except Exception as ex:
                print(ex)
                logger.info(ex)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())

        bmscur.close()

        time.sleep(52)
    
    emscur.close()

    time.sleep(8)

