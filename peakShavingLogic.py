import mysql.connector
import time
from datetime import datetime
import logging
import socket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

ip_bat = '10.9.244.1'
ip_conv = "10.9.244.45"

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
recipient = ['ems@respark.iitm.ac.in','harisankarm@tenet.res.in','ritvika@respark.iitm.ac.in']

logging.basicConfig(
    filename='LtoAutomation.log',  # Specify the log file name
    level=logging.DEBUG,     # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

hexON = "11870000000902100011000102001B"
bytON= bytes.fromhex(hexON)

hexOFF = "11870000000902100011000102001C"
bytOFF = bytes.fromhex(hexOFF)

preOFF = bytes.fromhex("851803FF1202FFFFFFFF000000")
preON = bytes.fromhex("851803FF1201FFFFFFFF000000")

mainOFF = bytes.fromhex("851803FF12FF01FFFFFF000000")

resetConv = "1187000000090210000F0001020017"
resetON = bytes.fromhex(resetConv)

def batData():
    bat_server_ip = ip_bat  # Replace with your server's IP address
    bat_server_port = 15153 

    bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        bat_client_socket.connect((bat_server_ip, bat_server_port))
    except Exception as ex:
        print(ex,'to battery')
        logger.info((ex,'to battery'))
        return "exception"

    bat_response = bat_client_socket.recv(10240) 

    bat_hex_string = ' '.join(f"{byte:02X}" for byte in bat_response)

    # print(bat_hex_string)
        
    initial_li = bat_hex_string.split('88 18')

    def ltoBattery(clean_li):
        global batteryVolt
        global mainConsSts
        global preConSts
        global batterySts
        global batteryCurent
        try:
            batteryVolt = clean_li[1] + clean_li[0]
            batteryVolt = int(batteryVolt,16)/10
        except:
            batteryVolt = None

        try:
            mainConsSts = clean_li[4][1]
            preConSts = clean_li[4][0]
                # print("main sts",mainConsSts)
                # print("prests",preConSts)
        except:
            mainConsSts = None
            preConSts = None
        
        try:
            batteryCurent = clean_li[3] + clean_li[2]
            batteryCurent = int(batteryCurent,16) / 100
        except:
            batteryCurent = None
        
        try:
            batterySts = clean_li[5][1]
                # print(batteryCurent)
                # CHG -> 3 , DCHG -> 2 ,
                # print("sts",batterySts)
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
            
        except:
            batterySts = None
                    

    def convertLTO(cleaned_li):
        if cleaned_li[0] == "03":
            now = datetime.now()
            ltoBattery(cleaned_li[3:])
    
    def clean_resp(raw_li):
        li = []
        order_li = raw_li.split(" ")
        for i in order_li:
            if len(i) > 1:
                li.append(i)
            if len(li) > 1:
                # print(li)
                convertLTO(li)

    for i in initial_li:
        # print(i)
        clean_resp(i)

    # print(batteryVolt,mainConsSts,preConSts,batterySts)

    return batteryVolt,mainConsSts,preConSts,batterySts,batteryCurent

def Convertor_data():
    conv_server_ip = ip_conv  # Replace with your server's IP address
    conv_server_port = 443 
        
    conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        conv_client_socket.connect((conv_server_ip, conv_server_port))
    except Exception as ex:
        print(ex,'to convertor')
        return "exception"

    hex_data = "418000000006020300000014"
    request_packet = bytes.fromhex(hex_data)

    conv_client_socket.send(request_packet)
    conv_response = conv_client_socket.recv(1024)

    conv_resp = ''.join([f'{byte:02x}' for byte in conv_response])

            # resp = str(response)
    clean_li = conv_resp[22:]

    chunk_size = 4

    conv_hex_data = [clean_li[i:i + chunk_size] for i in range(0, len(clean_li), chunk_size)]

    # print(conv_hex_data)

    conv_int_lis = []

    for i in conv_hex_data:
        if int(i,16) > 60000:
                    # print(i,int(i,16)-65535)
            conv_int_lis.append(int(i,16)-65535)
        else:
                    # print(i,int(i,16))
            conv_int_lis.append(int(i,16))

    try:
        currentSet = conv_int_lis[11]
    except:
        currentSet = None

    try:
        voltageSet = conv_int_lis[12]
    except:
        voltageSet = None
    
    try:
        bytSet = conv_int_lis[16]
    except:
        bytSet = None
    
    try:
        outputVoltage = conv_int_lis[7]/10
    except:
        outputVoltage = None
    
    try:
        outputCurrent = conv_int_lis[8]/10
    except:
        outputCurrent = None
    
    try:
        statusFault = bin(conv_int_lis[10])[2:]
        statusFault = statusFault.rjust(16,'0')
    except:
        statusFault = None
            
    return voltageSet,currentSet,bytSet,outputVoltage,outputCurrent,statusFault


while True:

    curtime = datetime.now()

    hr = int(str(curtime)[11:13])
    mint = int(str(curtime)[14:16])

    print(hr)

    if (hr >= 9  and hr < 13) or (hr >= 14 and hr <17): 
        print("Discharge Timing")
        try:
            emsdb = mysql.connector.connect(
                    host="121.242.232.211",
                    user="emsroot",
                    password="22@teneT",
                    database='bmsunprocessed_prodv13',
                    port=3306
                    )
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmt_olap_prod_v13',
                    port=3306
                    )
            awsdb = mysql.connector.connect(
                host="43.205.196.66",
                user="emsroot",
                password="22@teneT",
                port= 3307
            )
            awscur = awsdb.cursor()
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
        
        except Exception as ex:
            print(ex)
            continue

        awscur.execute("select polledTime,totalApparentPower,dischargeStatus from EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 1")

        peakres = awscur.fetchall()

        polledTime = peakres[0][0]
        peakDemand = peakres[0][1]
        status = peakres[0][2]

        print(peakDemand)

        curtime = datetime.now()

        hr = int(str(curtime)[11:13])

        curtime = str(datetime.now())[0:20]

        convData = Convertor_data()

        statusFault = convData[5]

        if statusFault != None: 
            try:
                inputUnderVoltage = int(statusFault[12])
            except:
                inputUnderVoltage = None

            try:
                inputOverVoltage = int(statusFault[11])
            except:
                inputOverVoltage = None

            try:
                outputUnderVoltage = int(statusFault[10])
            except:
                outputUnderVoltage = None

            try:
                outputOverVoltage = int(statusFault[9])
            except:
                outputOverVoltage = None

            try:
                dcdcTrip = int(statusFault[13])
            except:
                dcdcTrip = None

        else:
            continue

        print("InUnderVolt",inputUnderVoltage)
        print("InOverVolt",inputOverVoltage)
        print("OutUnderVolt",outputUnderVoltage)
        print("OutOverVolt",outputOverVoltage)
        print("DCDCtrip",dcdcTrip)

        logger.info(("InUnderVolt",inputUnderVoltage))
        logger.info(("InOverVolt",inputOverVoltage))
        logger.info(("OutUnderVolt",outputUnderVoltage))
        logger.info(("OutOverVolt",outputOverVoltage))
        logger.info(("DCDCtrip",dcdcTrip))

        if inputOverVoltage == 1 or inputUnderVoltage == 1 or outputOverVoltage == 1 or outputUnderVoltage == 1 or dcdcTrip == 1:
            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                continue
            # print("inunder :",inputUnderVoltage,"inover :",inputOverVoltage,"outunder :",outputUnderVoltage,"outUnder :",outputOverVoltage)
            logger.info(("inunder :",inputUnderVoltage,"inover :",inputOverVoltage,"outunder :",outputUnderVoltage,"outUnder :",outputOverVoltage))
            conv_client_socket.send(resetON)
            print("Convertor Fault")
            print("Convertor Reset Sent")
            logger.info("Convertor Fault")
            logger.info("Convertor Reset Sent")

        def ProcessOn():
            battData = batData()
            batVoltage = battData[0]
            mainConsSts = battData[1]
            preConSts = battData[2]
            batSts = battData[3]

            if batVoltage != None and batVoltage != 'exception':
                batVoltage = int(batVoltage)
                bat_hex = hex(batVoltage)[2:]
                if len(bat_hex) == 3:
                    bat_hex = '0'+bat_hex.upper()
            else:
                time.sleep(2)
                ProcessOn() 

            print(batteryVolt,mainConsSts,preConSts,batSts)

            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                time.sleep(2)
                ProcessOn() 

            bat_server_ip = ip_bat  # Replace with your server's IP address
            bat_server_port = 15153 

            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                ProcessOn()
            
            if batSts != 'DCHG' and batSts != 'FAULT' and batSts != 'CHG':
                if batteryVolt >= 399 and batteryVolt <= 420:
                    def setDCCurrent(crate):
                        dchg_mode = bytes.fromhex(crate)
                        conv_client_socket.send(dchg_mode)
                        time.sleep(2)
                        try:
                            convCurrent = int(Convertor_data()[1])
                        except Exception:
                            setDCCurrent(crate)
                        print(convCurrent)
                        try:
                            if (convCurrent-1) >= -40:
                                print("Current set on Convertor Set Success")
                                print("Disharge started")
                                logger.info("Current set on Convertor Set Success")
                                logger.info("Disharge started")
                                time.sleep(50)
                            else:
                                setDCCurrent(crate)
                        except Exception as ex:
                            print(ex)
                            setDCCurrent(crate)
                        
                        
                    def CheckPreONDC(crate):
                        preConSts = batData()[2]
                        convData = Convertor_data()
                        try:
                            convOutVolt = int(convData[3])
                        except Exception:
                            CheckPreONDC(crate)
                        try:
                            if preConSts == '1' and convOutVolt > 300:
                                print("Pre ON Success")
                                logger.info("Pre ON Success")
                                setDCCurrent(crate)
                            else:
                                SetPreONDC(crate)
                        except Exception as ex:
                            print(ex)
                            logger.info(ex)
                            SetPreONDC(crate)
                        
                    def SetPreONDC(crate):
                        bat_server_ip = ip_bat  # Replace with your server's IP address
                        bat_server_port = 15153 

                        bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        try:
                            bat_client_socket.connect((bat_server_ip, bat_server_port))
                        except Exception as ex:
                            print(ex,'to battery')
                            SetPreONDC(crate)
                        preON = bytes.fromhex("851803FF1201FFFFFFFF000000")
                        bat_client_socket.send(preON)
                        time.sleep(2)
                        print("Pre ON sent")
                        logger.info("Pre ON sent")
                        time.sleep(2)
                        preConSts = batData()[2]
                        print('pre',preConSts)
                        logger.info(('pre',preConSts))
                        CheckPreONDC(crate)
                        
                    def setLowerCurrentDC(crate):
                        print("setting current")
                        logger.info("setting current")
                        cur_mode = bytes.fromhex("1D5A000000090210000C0001020000")
                        conv_client_socket.send(cur_mode)
                        conv_client_socket.send(bytON)
                        time.sleep(3)
                        convData = Convertor_data()
                        try:
                            convByteON = int(convData[2])
                            convOutVolt = int(convData[3])
                        except Exception:
                            setLowerCurrentDC(crate)
                        print(convByteON,convOutVolt)
                        logger.info((convByteON,convOutVolt))
                        try:
                            if convByteON == 27:
                                print("Lower current set on Convertor success and Conv out voltage > 300")
                                logger.info("Lower current set on Convertor success and Conv out voltage > 300")
                                SetPreONDC(crate)
                            else:
                                setLowerCurrentDC(crate)
                        except Exception as ex:
                            print(ex)
                            logger.info(ex)
                            setLowerCurrentDC(crate)
                        
                    def SendConvVoltageDC(bat_hex,crate):
                        setConvVolt = "029A000000090210000D000102"+bat_hex
                        setConvVolt = bytes.fromhex(setConvVolt)   
                        conv_client_socket.send(setConvVolt)
                        time.sleep(2)
                        print(setConvVolt)
                        print("Convert voltage sent")
                        logger.info(setConvVolt)
                        logger.info("Convert voltage sent")
                        try:
                            convVoltage = int(Convertor_data()[0])
                        except Exception:
                            SendConvVoltageDC(bat_hex,crate)
                        batVoltage = batData()[0]
                        try:
                            if convVoltage == batVoltage:
                                print("Battery and Conv volt equal success")
                                logger.info("Battery and Conv volt equal success")
                                setLowerCurrentDC(crate)
                            else:
                                SendConvVoltageDC(bat_hex,crate)
                        except Exception as ex:
                            print(ex)
                            SendConvVoltageDC(bat_hex,crate)

                    def DischargeON(crate):
                        SendConvVoltageDC(bat_hex,crate)
                    print("Discharge on called")
                    logger.info(("Discharge on called"))
                    crate = "1D5A000000090210000C000102FFD8"
                    DischargeON(crate)

                    sql = "INSERT INTO EMS.PeakAutomation(polledTime,Batterysts,Peakvalue,peakTime) values(%s,%s,%s,%s)"
                    val = (curtime,'ON',peakDemand,polledTime)
                    emscur.execute(sql,val)
                    emsdb.commit()
                    print("Discharge ON from EMS")
                    print("DCHG ON saved in database")
                    logger.info(("Discharge ON from EMS"))
                    logger.info(curtime)

                    message = MIMEMultipart('alternative')
                    message['Subject'] = f'EMS ALERT -  Peak Demand {round(peakDemand)} LTO Battery ON'
                    message['From'] = sender
                    message['To'] = ', '.join(recipient)

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
                                    <td>Peak Demand Limt {peakDemand}</td>
                                    </tr>
                                    <tr>
                                        <td><b>Severity<b></td>
                                        <td>Medium</td>
                                    </tr>
                                    <tr>
                                        <td><b>Time<b></td>
                                        <td>{curtime}</td>
                                    </tr>
                                    <tr>
                                        <td><b>System<b></td>
                                        <td>LTO Battery ON</td>
                                    </tr>
                                </table>
                                <br>
                                <hr>
                                <p>EMS team</p></center>
                                </body>awscur.execute("select dischargeStatus from EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 1")
                    """
                    html_part = MIMEText(html_content, 'html')
                    message.attach(html_part)

                    with smtplib.SMTP(smtp_server, smtp_port) as server:
                        server.starttls()
                        server.login(smtp_username, smtp_password)
                        server.sendmail(sender, recipient, message.as_string())
                        
                    time.sleep(50)

        status = peakres[0][2]

        battData = batData()
        batVoltage = battData[0]
        mainConsSts = battData[1]
        preConSts = battData[2]
        batSts = battData[3]

        print(status,batSts)

        if status == 'ID':
            ProcessOn()

        if status == 'GD':
            awscur.execute("select dischargeStatus from EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 5")
            
            stsRes = awscur.fetchall()

            count_gd = 0

            for item in stsRes:
                if item == ('GD',):
                    count_gd += 1
            
            print('count GD:',count_gd)

            if count_gd >= 5:
                ProcessOn()

    elif hr == 13 or (hr == 17 and mint >= 30):
        try:
            emsdb = mysql.connector.connect(
                    host="121.242.232.211",
                    user="emsroot",
                    password="22@teneT",
                    database='bmsunprocessed_prodv13',
                    port=3306
                    )
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmt_olap_prod_v13',
                    port=3306
                    )
            awsdb = mysql.connector.connect(
                host="43.205.196.66",
                user="emsroot",
                password="22@teneT",
                port= 3307
            )
            awscur = awsdb.cursor()
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
        
        except Exception as ex:
            print(ex)
            continue

        battData = batData()
        batVoltage = battData[0]
        mainConsSts = battData[1]
        preConSts = battData[2]
        batSts = battData[3]

        print(batSts,batVoltage)

        convData = Convertor_data()

        statusFault = convData[5]

        if statusFault != None: 
            try:
                inputUnderVoltage = int(statusFault[12])
            except:
                inputUnderVoltage = None

            try:
                inputOverVoltage = int(statusFault[11])
            except:
                inputOverVoltage = None

            try:
                outputUnderVoltage = int(statusFault[10])
            except:
                outputUnderVoltage = None

            try:
                outputOverVoltage = int(statusFault[9])
            except:
                outputOverVoltage = None

            try:
                dcdcTrip = int(statusFault[13])
            except:
                dcdcTrip = None

        else:
            continue

        print("InUnderVolt",inputUnderVoltage)
        print("InOverVolt",inputOverVoltage)
        print("OutUnderVolt",outputUnderVoltage)
        print("OutOverVolt",outputOverVoltage)
        print("DCDCtrip",dcdcTrip)

        logger.info(("InUnderVolt",inputUnderVoltage))
        logger.info(("InOverVolt",inputOverVoltage))
        logger.info(("OutUnderVolt",outputUnderVoltage))
        logger.info(("OutOverVolt",outputOverVoltage))
        logger.info(("DCDCtrip",dcdcTrip))

        if inputOverVoltage == 1 or inputUnderVoltage == 1 or outputOverVoltage == 1 or outputUnderVoltage == 1 or dcdcTrip == 1:
            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                continue
            # print("inunder :",inputUnderVoltage,"inover :",inputOverVoltage,"outunder :",outputUnderVoltage,"outUnder :",outputOverVoltage)
            logger.info(("inunder :",inputUnderVoltage,"inover :",inputOverVoltage,"outunder :",outputUnderVoltage,"outUnder :",outputOverVoltage))
            conv_client_socket.send(resetON)
            print("Convertor Fault")
            print("Convertor Reset Sent")
            logger.info("Convertor Fault")
            logger.info("Convertor Reset Sent")

        if batSts == 'IDLE' and batVoltage <=400 and mainConsSts != '3' and preConSts != '3':

            bat_server_ip = ip_bat  # Replace with your server's IP address
            bat_server_port = 15153 

            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                continue

            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex)
                continue
            
            battData = batData()

            batVoltage = battData[0]

            if batVoltage != None and batVoltage != 'exception':
                batVoltage = int(batVoltage)
                bat_hex = hex(batVoltage)[2:]
                if len(bat_hex) == 3:
                    bat_hex = '0'+bat_hex.upper()


            def setLowerCurrent(crate):
                print("setting current")
                logger.info("setting current")
                cur_mode = bytes.fromhex("1D5A000000090210000C0001020000")
                conv_client_socket.send(cur_mode)
                conv_client_socket.send(bytON)
                time.sleep(2)
                convData = Convertor_data()
                try:
                    convByteON = int(convData[2])
                    convOutVolt = int(convData[3])
                except Exception:
                    setLowerCurrent(crate)
                print(convByteON,convOutVolt)
                logger.info((convByteON,convOutVolt))
                try:
                    if convByteON == 27:
                        print("Lower current set on Convertor success and Conv out voltage > 300")
                        logger.info("Lower current set on Convertor success and Conv out voltage > 300")
                        SetPreON(crate)
                    else:
                        setLowerCurrent(crate)
                except Exception as ex:
                    print(ex)
                    setLowerCurrent(crate)


            def SendConvVoltage(bat_hex,crate):
                setConvVolt = "029A000000090210000D000102"+bat_hex
                setConvVolt = bytes.fromhex(setConvVolt)   
                conv_client_socket.send(setConvVolt)
                time.sleep(2)
                print(setConvVolt)
                print("Convert voltage sent")
                logger.info("Convert voltage sent")
                try:
                    convVoltage = int(Convertor_data()[0])
                except Exception:
                    SendConvVoltage(bat_hex,crate)
                batVoltage = batData()[0]
                try:
                    if convVoltage == batVoltage:
                        print("Battery and Conv volt equal success")
                        logger.info("Battery and Conv volt equal success")
                        setLowerCurrent(crate)
                    else:
                        SendConvVoltage(bat_hex,crate)
                except Exception as ex:
                    print(ex)
                    logger.info(ex)
                    SendConvVoltage(bat_hex,crate)

            def SetPreON(crate):
                bat_server_ip = ip_bat  # Replace with your server's IP address
                bat_server_port = 15153 

                bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                try:
                    bat_client_socket.connect((bat_server_ip, bat_server_port))
                except Exception as ex:
                    print(ex,'to battery')
                    SetPreON(crate)
                preON = bytes.fromhex("851803FF1201FFFFFFFF000000")
                bat_client_socket.send(preON)
                time.sleep(2)
                print("Pre ON sent")
                logger.info("Pre ON sent")
                preConSts = batData()[2]
                convOutVolt = Convertor_data()[3]
                try:
                    print(preConSts,convOutVolt)
                    #if preConSts == '1' and convOutVolt > 300:
                    if convOutVolt > 300:
                        print("Pre ON Success")
                        logger.info("Pre ON Success")
                        setCHVolt(crate)
                    else:
                        SetPreON(crate)
                except Exception as ex:
                    print(ex)
                    SetPreON(crate)

            def setCHVolt(crate):
                volt_limit = "01A4"
                setConvVolt = "029A000000090210000D000102"+volt_limit
                setConvVolt = bytes.fromhex(setConvVolt)  
                conv_client_socket.send(setConvVolt)
                time.sleep(2)
                print("Max voltage sent")
                logger.info("Max voltage sent")
                CheckCHVolatge(crate)
            
            def setCHCurrent(crate):
                chg_mode = bytes.fromhex(crate)
                conv_client_socket.send(chg_mode)
                time.sleep(2)
                try:
                    convCurrent = int(Convertor_data()[1])
                except Exception:
                    setCHCurrent(crate)
                try:
                    if convCurrent == 40:
                        print("Current set on Convertor Set Success")
                        print("Charge started")
                        logger.info("Current set on Convertor Set Success")
                        logger.info("Charge started")
                        conv_client_socket.close()
                        bat_client_socket.close()
                        global success
                        success = "success"
                    else:
                        setCHCurrent(crate)
                except Exception as ex:
                    print(ex)
                    setCHCurrent(crate)


            def CheckCHVolatge(crate):
                try:
                    convVoltage = int(Convertor_data()[0])
                except Exception:
                    CheckCHVolatge(crate)
                try:
                    if convVoltage == 420:
                        print("Voltage max set on Conv success")
                        logger.info("Voltage max set on Conv success")
                        setCHCurrent(crate)
                    else:
                        print("Convertor voltage and current not set")
                        logger.info("Convertor voltage and current not set")
                        setCHVolt(crate)
                except Exception as ex:
                    print(ex)
                    setCHVolt(crate)


            def ChargeON(crate):
                SendConvVoltage(bat_hex,crate)

            crate = "1D5A000000090210000C0001020028"
            ChargeON(crate)

            print("Chharge ON from EMS")
            print("CHG ON saved in database")
            logger.info(("Discharge ON from EMS"))
            logger.info(curtime)

            message = MIMEMultipart('alternative')
            message['Subject'] = f'EMS ALERT - LTO Battery Scheduled Charge ON'
            message['From'] = sender
            message['To'] = ', '.join(recipient)

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
                                    <td>LTO Battery Charge ON</td>
                                    </tr>
                                    <tr>
                                        <td><b>Severity<b></td>
                                        <td>Medium</td>
                                    </tr>
                                    <tr>
                                        <td><b>Time<b></td>
                                        <td>{curtime}</td>
                                    </tr>
                                    <tr>
                                        <td><b>System<b></td>
                                        <td>LTO Battery ON</td>
                                    </tr>
                                </table>
                                <br>
                                <hr>
                                <p>EMS team</p></center>
                                </body>
                    """
            html_part = MIMEText(html_content, 'html')
            message.attach(html_part)

            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(smtp_username, smtp_password)
                server.sendmail(sender, recipient, message.as_string())
                        
            time.sleep(50)

    battData = batData()
    batSts = battData[3]
    if batSts == 'DCHG':
        try:
            emsdb = mysql.connector.connect(
                    host="121.242.232.211",
                    user="emsroot",
                    password="22@teneT",
                    database='bmsunprocessed_prodv13',
                    port=3306
                    )
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmt_olap_prod_v13',
                    port=3306
                    )
            awsdb = mysql.connector.connect(
                host="43.205.196.66",
                user="emsroot",
                password="22@teneT",
                port= 3307
            )
            awscur = awsdb.cursor()
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
        except Exception as ex:
            print(ex)
            continue
        emscur.execute("select polledTime from EMS.PeakAutomation order by polledTime desc limit 1")

        ltoON = emscur.fetchall()

        print(ltoON)

        if ltoON[0][0] != None:
            print(ltoON[0][0])
            pkno = 0

            curtime = datetime.now()

            print(curtime)

            secs = (curtime - ltoON[0][0]).total_seconds()

            print("Timer",secs)

            if secs >=600 and secs <= 1800:
                awscur.execute("select dischargeStatus from EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 5")

                stsRes = awscur.fetchall()

                count_nd = 0

                for item in stsRes:
                    if item == ('ND',):
                        count_nd += 1
                
                print(count_nd)

                if count_nd >= 5:
                    print("Dchg off")
                    # -------------------------------------------------DischargeOFF---------------------------------------------------------
                    conv_server_ip = ip_conv  # Replace with your server's IP address
                    conv_server_port = 443 
                            
                    conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                    try:
                        conv_client_socket.connect((conv_server_ip, conv_server_port))
                    except Exception as ex:
                        print(ex,'to convertor')
                        continue
                    def PreOFFDCof():
                        bat_server_ip = ip_bat  # Replace with your server's IP address
                        bat_server_port = 15153 

                        bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        try:
                            bat_client_socket.connect((bat_server_ip, bat_server_port))
                        except Exception as ex:
                            print(ex,'to battery')
                            PreOFFDCof()
                        preOFF = bytes.fromhex("851803FF1202FFFFFFFF000000")
                        bat_client_socket.send(preOFF)
                        preConSts = batData()[2]
                        print("Pre OFF sent")
                        logger.info(("Pre OFF sent"))
                        time.sleep(2) 
                        try:
                            if preConSts == '2':
                                print("Pre OFF Success.")
                                logger.info("Pre OFF Success.")
                                time.sleep(3)
                                checkBatStsDC()
                            else:
                                PreOFFDCof()
                        except Exception as ex:
                            print(ex)
                            PreOFFDCof()

                    def setMainOFFDC():
                        print("Main OFF called")
                        logger.info("Main OFF called")
                        bat_server_ip = ip_bat # Replace with your server's IP address
                        bat_server_port = 15153 

                        bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        try:
                            bat_client_socket.connect((bat_server_ip, bat_server_port))
                        except Exception as ex:
                            print(ex,'to battery')
                            setMainOFFDC()
                        mainOFF = bytes.fromhex("851803FF12FF01FFFFFF000000")
                        bat_client_socket.send(mainOFF)
                        time.sleep(2) 
                        BatData = batData()
                        mainConsSts = BatData[1]
                        batCur = BatData[4]
                        print(mainConsSts,batCur)
                        try:
                            if mainConsSts == '2':
                                print("Discharge OFF Completed")
                                logger.info("Discharge OFF Completed")
                                time.sleep(3)
                                PreOFFDCof()
                            else:
                                print("Main not OFF")
                                print("Main",mainConsSts)
                                logger.info("Main not OFF")
                                logger.info(("Main",mainConsSts))
                                time.sleep(2)
                                setMainOFFDC()
                        except Exception as ex:
                            print(ex)
                            setMainOFFDC()
                    
                    def SetOFFCurDC():
                        print("current and voltage sending to conv")
                        logger.info(("current and voltage sending to conv"))
                        cur_mode = bytes.fromhex("1D5A000000090210000C0001020000")
                        conv_client_socket.send(cur_mode)
                        # setConvVolt = "029A000000090210000D000102"+bat_hex
                        # setConvVolt = bytes.fromhex(setConvVolt)   
                        # conv_client_socket.send(setConvVolt) 
                        time.sleep(2) 
                        convCurrent = Convertor_data()[1]
                        convOutVolt = Convertor_data()[3]
                        convOutCur = Convertor_data()[4]
                        print(convCurrent)
                        # abs(convVoltage - batVoltage) <= 2
                        try:
                            if convCurrent == 0 and convOutVolt > 300 and convOutCur < 2:
                                print("Conv and Bat voltage equal and conv current 0 success")
                                logger.info("Conv and Bat voltage equal and conv current 0 success")
                                setMainOFFDC()
                            else:
                                print("Conv and Bat voltage not set")
                                logger.info("Conv and Bat voltage not set")
                                SetOFFCurDC()
                        except Exception as ex:
                            print(ex)
                            SetOFFCurDC()
                    
                    
                    def checkBatStsDC():
                        conv_client_socket.send(bytOFF)
                        print("Byte OFF sent")
                        logger.info(("Byte OFF sent"))
                        time.sleep(2)
                        convByteON = int(Convertor_data()[2])
                        try:
                            if convByteON == 28:
                                print("Discharge OFF completed")
                                time.sleep(50)
                                logger.info("Discharge OFF completed")
                            else:
                                checkBatStsDC()
                        except Exception as ex:
                            print(ex)
                            checkBatStsDC()
                        
                    def DischargeOFF():
                        SetOFFCurDC()
                    
                    DischargeOFF()

    time.sleep(10)