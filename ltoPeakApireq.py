import requests
import time
from datetime import datetime
import mysql.connector
import socket
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

headers = {'Authorization': 'VKOnNhH2SebMU6S'}

dchgonurl = 'http://localhost:8007/ltoDischargeon'
dchgoffurl = 'http://localhost:8007/ltoDischargeoff'
chgonurl = 'http://localhost:8007/ltoChargeon'
chgoffurl = 'http://localhost:8007/ltoChargeoff'

peak_limit = 3800
peak_low_limit = peak_limit - 50

def batData():
    bat_server_ip = "10.9.220.42"  # Replace with your server's IP address
    bat_server_port = 15153 

    bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        bat_client_socket.connect((bat_server_ip, bat_server_port))
    except Exception as ex:
        print(ex,'to battery')
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
                batterySts = 'CHG'
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
    conv_server_ip = "10.9.220.43"  # Replace with your server's IP address
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
            
    return voltageSet,currentSet,bytSet,outputVoltage,outputCurrent

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
recipient = ['ems@respark.iitm.ac.in','harisankarm@tenet.res.in']


def mail_send():
    sql = "INSERT INTO EMS.PeakAutomation(polledTime,Batterysts,Peakvalue,peakTime) values(%s,%s,%s,%s)"
    val = (curtime,'ON',peak,peak_time)
    emscur.execute(sql,val)
    emsdb.commit()
    print("Discharge ON from EMS")
    print("DCHG ON saved in database")

    message = MIMEMultipart('alternative')
    message['Subject'] = f'EMS ALERT -  Peak Demand {peak_limit} LTO Battery ON'
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
                                <td>Peak Demand Limt {peak_limit}</td>
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
                            </body>"""
    html_part = MIMEText(html_content, 'html')
    message.attach(html_part)

    with smtplib.SMTP(smtp_server, smtp_port) as server:
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(sender, recipient, message.as_string())


def dchgon():
    print("Discharge ON")
    upsres = requests.get(dchgonurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        dchgon()
    else:
        print(data)
        time.sleep(50)
    time.sleep(10)

def dchgoff():
    print("Discharge OFF")
    upsres = requests.get(dchgoffurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        dchgoff()
    else:
        print(data)
        time.sleep(50)
    time.sleep(10)

def chgoff():
    print("Charge OFF")
    upsres = requests.get(chgoffurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        chgoff()
    else:
        print(data)
        time.sleep(50)
        dchgoff()
    time.sleep(10)


function_names = ['dchgon','dchgoff','chgoff']

while True:
    try:
        emsdb = mysql.connector.connect(
                host="121.242.232.211",
                user="emsroot",
                password="22@teneT",
                database='bmsunprocessed_prodv13',
                port=3306
                )
    except Exception as ex:
        print("EMS database not connected")
        print(ex)

    try:
        processeddb = mysql.connector.connect(
		host="121.242.232.151",
		user="emsrouser",
		password="emsrouser@151",
		database='bmsmgmt_olap_prod_v13',
		port=3306
		)
    except Exception as ex:
        print(ex)
        time.sleep(10)
        continue

    try:
        proscur = processeddb.cursor()
    except Exception as ex:
        print(ex)
        time.sleep(20)
        continue

    emscur = emsdb.cursor()

    proscur.execute("SELECT totalApparentPower2,polledTime FROM hvacSchneider7230Polling where totalApparentPower2 is not null order by polledTime desc limit 1")

    res = proscur.fetchall()

    print(res)
    try:
        peak = res[0][0]
        peak_time = res[0][1]
    except Exception as ex:
        print(ex)
        peak = 0
        peak_time = ''
    
    curtime = datetime.now()
    print(peak)

    proscur.execute("SELECT batterystatus FROM bmsmgmtprodv13.ltoBatteryData where batterystatus is not null order by recordId desc limit 1")

    batterysts = proscur.fetchall()

    print(batterysts[0][0])
    if peak >= peak_limit:
        BatData = batData()
        voltsts = BatData[0]
        batterysts = BatData[3]

        if batterysts == 'CHG':
            if voltsts > 360:
                cname = 'chgoff'
                if cname in globals() and callable(globals()[cname]):
                    function = globals()[cname]
                    try:
                        result = function()
                    except:
                        continue
                else:
                    print(f"Function {cname} not found.")
                    continue

    time.sleep(8)
