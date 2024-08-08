import requests
from datetime import datetime
import time
import mysql.connector
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import smtplib
from pymodbus.client.tcp import ModbusTcpClient
import socket

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
recipient = ['faheera@respark.iitm.ac.in','iswarya@tenet.res.in','vetrivel@tenet.res.in','arun.kumar@tenet.res.in','ritvika@respark.iitm.ac.in']

def send_mail_off(peak,process):
    curtime = datetime.now()
    curtime = str(curtime)[0:16]

    message = MIMEMultipart('alternative')
    message['Subject'] = f'EMS ALERT - IOE Battery {process} OFF'
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
                                <td>Peak Limt {peak}</td>
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
                                    <td>IOE Battery</td>
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

def send_mail(peak,strings,crate):
    curtime = datetime.now()
    curtime = str(curtime)[0:16]

    message = MIMEMultipart('alternative')
    message['Subject'] = f'EMS ALERT - IOE Battery {strings[-1]} ON'
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
                                <td>Peak Limt {peak}</td>
                                </tr>
                                <tr>
                                    <td><b>Severity<b></td>
                                    <td>Medium</td>
                                </tr>
                                <tr>
                                    <td><b>Strings<b></td>
                                    <td>{strings[0:-1]}</td>
                                </tr>
                                <tr>
                                    <td><b>C-Rate<b></td>
                                    <td>{crate}</td>
                                </tr>
                                <tr>
                                    <td><b>Time<b></td>
                                    <td>{curtime}</td>
                                </tr>
                                <tr>
                                    <td><b>System<b></td>
                                    <td>IOE Battery</td>
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

host = '121.242.232.211'

startConv = bytes.fromhex("D9CD00000009011004870001020009")
stopConv = bytes.fromhex("D9CD00000009011004870001020000")

netCloseON = bytes.fromhex("DAEE00000009011006430001020002")
netCloseOFF = bytes.fromhex("DAEE0000000901100643000102000F")

netSyncON = bytes.fromhex("DB7600000009011006410001020002")
netSyncOFF = bytes.fromhex("DB760000000901100641000102000E")

API_mapping = {
    '1': f'http://{host}:8009/ioesinglestr',
    '2': f'http://{host}:8009/ioedoublestr',
    '3': f'http://{host}:8009/ioetriplestr',
    '4': f'http://{host}:8009/ioefourstr',
    '5': f'http://{host}:8009/ioefivestr',
}

str_reply = {
    '1':f'http://{host}:5008/ioest1reply',
    '2':f'http://{host}:5008/ioest2reply',
    '3':f'http://{host}:5008/ioest3reply',
    '4':f'http://{host}:5008/ioest4reply',
    '5':f'http://{host}:5008/ioest5reply'
}

Crate_Map = {
    '0.1':50,
    '0.2':100,
    '0.3':150,
    '0.4':180,
    '0.5':200
}

conv_server_ip = "10.9.242.6"  # Replace with your server's IP address
conv_server_port = 502 

def dchgHex(amps):
    hx = '{:04x}'.format(amps)

    return hx

def CurrentSet(crate):
    hex= crate[-4:]
    curset = int(hex,16)
    cur_mode = bytes.fromhex(crate)
    conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        conv_client_socket.connect((conv_server_ip, conv_server_port))
    except Exception as ex:        
        print(ex,'to convertor')
    conv_client_socket.send(cur_mode)
    print("Current SET")
    time.sleep(1)
    client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
    client_conv.connect()
    read=client_conv.read_holding_registers(address = 1532)
    current = read.registers[0]
    print(current)
    client_conv.close()
    if current == curset:
        conv_client_socket.close()
        print("Current Set Sucessful")
        return 'Success'
    else:
        time.sleep(2)
        conv_client_socket.close()
        return 'Failed'
        

while True:
    curdate = datetime.now()
    curntime = int(str(curdate)[11:13])

    print(curdate)

    if curntime >= 9 and curntime <= 18:
        dchgLi = []
        chgLi = []
        MainLi = []
        PreLi = []
        peakLi = []
        voltLi = []
        try:
            emsdb = mysql.connector.connect(
                    host="121.242.232.211",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3306
                )
            
            bmsdb = mysql.connector.connect(
                    host="121.242.232.151",
                    user="emsrouser",
                    password="emsrouser@151",
                    database='bmsmgmtprodv13',
                    port=3306
                    )
            
            awsdb = mysql.connector.connect(
                    host="3.111.70.53",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )
            
            emscur = emsdb.cursor()
            bmscur = bmsdb.cursor()
            awscur = awsdb.cursor()
        except Exception as ex:
            print(ex)
            continue

        emscur.execute("select polledTime,onReason,offReason from EMS.ioeOnOff where date(polledTime) = curdate() order by polledTime desc limit 1;")
        sts = None
        fchk = emscur.fetchall()
        if len(fchk) > 0:
            curntime = int(str(fchk[0][0])[11:13])
            if curntime >= 9 and curntime <= 18:
                if fchk[0][2]:
                    sts = "NO"
            else:
                sts = "OK"
        else:
            sts = "OK"

        if sts == "OK":
            awscur.execute("select polledTime,maxAvgPeak,dischargeStatus from EMS.peakShavingLogic where date(polledTime) = curdate() order by polledTime desc limit 1")

            peakres = awscur.fetchall()

            polledTime = peakres[0][0]
            peakDemand = peakres[0][1]
            status = peakres[0][2]

            print(peakDemand)

            if peakDemand != None:
                peakDemand = peakDemand - ((peakDemand*5)/100)
                if peakDemand < 4200:
                    peakDemand = 4200
            
            print('GD:',peakDemand)

            bmscur.execute("SELECT totalApparentPower1 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = curdate() order by recordId desc limit 1;")
            res = bmscur.fetchall()
            peak = res[0][0]

            print('Peak:',peak)

            bmscur.execute("SELECT totalApparentPower1 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = curdate() order by recordId desc limit 40;")
            avgRes = bmscur.fetchall()

            for i in avgRes:
                if i[0] != None:
                    peakLi.append(i[0])

            if len(peakLi) > 0:
                peakAvg = round(sum(peakLi)/len(peakLi))
            else:
                peakAvg = None

            print('peakAvg:',peakAvg)

            if peak != None and (peak >= peakDemand or peak > 4200):

                curtime = datetime.now()
                current_date = curtime.replace(hour=0, minute=0, second=0, microsecond=0)

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt1BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str1Res = emscur.fetchall()

                if len(str1Res) > 0:
                    try:
                        str1Time = str1Res[0][4]
                        voltLi.append(str1Res[0][0])
                        dchgLi.append(str1Res[0][3])
                        MainLi.append(str1Res[0][1])
                        PreLi.append(str1Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str1Time = current_date

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt2BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str2Res = emscur.fetchall()

                if len(str2Res) > 0:
                    try:
                        voltLi.append(str2Res[0][0])
                        str2Time = str2Res[0][4]
                        dchgLi.append(str2Res[0][3])
                        MainLi.append(str2Res[0][1])
                        PreLi.append(str2Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str2Time = current_date
                

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt3BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str3Res = emscur.fetchall()

                if len(str3Res) > 0:
                    try:
                        voltLi.append(str3Res[0][0])
                        str3Time = str3Res[0][4]
                        dchgLi.append(str3Res[0][3])
                        MainLi.append(str3Res[0][1])
                        PreLi.append(str3Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str3Time = current_date
                
                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt4BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str4Res = emscur.fetchall()

                if len(str4Res) > 0:
                    try:
                        voltLi.append(str4Res[0][0])
                        str4Time = str4Res[0][4]
                        dchgLi.append(str4Res[0][3])
                        MainLi.append(str4Res[0][1])
                        PreLi.append(str4Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str4Time = current_date
                
                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt5BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str5Res = emscur.fetchall()

                if len(str5Res) > 0:
                    try:
                        voltLi.append(str5Res[0][0])
                        str5Time = str5Res[0][4]
                        dchgLi.append(str5Res[0][3])
                        MainLi.append(str5Res[0][1])
                        PreLi.append(str5Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str5Time = current_date

                print(dchgLi)
                print(MainLi)
                print(voltLi)

                volt = 0
                count = 0

                for i in voltLi:
                    if i != None and i > 0:
                        volt += i
                        count += 1

                if volt != 0:
                    avgVolt = volt/count
                
                print('Average Voltage :', avgVolt)

                if len(dchgLi) > 0 and len(MainLi) > 0:
                   
                    if 'CHG' in dchgLi and avgVolt > 675:

                        ApiUrl = f"http://{host}:8009/ioeprocessoff"    
                        OFFurl = requests.get(ApiUrl)
                        resJson = OFFurl.json()

                        if 'DCHG' not in dchgLi and 'ON' not in MainLi and avgVolt > 670:
                            print(dchgLi,MainLi)

                            str1lag = (curtime - str1Time).total_seconds()
                            str2lag = (curtime - str2Time).total_seconds()
                            str3lag = (curtime - str3Time).total_seconds()
                            str4lag = (curtime - str4Time).total_seconds()
                            str5lag = (curtime - str5Time).total_seconds()

                            def GetCloseVoltage(dictionary):
                                close_pairs = []
                                keys = list(dictionary.keys())
                                for i in range(len(keys)):
                                    for j in range(i + 1, len(keys)):
                                        key1 = keys[i]
                                        key2 = keys[j]
                                        diff = abs(dictionary[key1] - dictionary[key2])
                                        if diff <= 10:
                                            close_pairs.append((key1, key2))
                                return close_pairs

                            strtime = {}

                            if str1lag <= 500:
                                strtime['str1'] = str1Res[0][0]
                            if str2lag <= 500:
                                strtime['str2'] = str2Res[0][0]
                            if str3lag <= 500:
                                strtime['str3'] = str3Res[0][0]
                            if str4lag <= 500:
                                strtime['str4'] = str4Res[0][0]
                            if str5lag <= 500:
                                strtime['str5'] = str5Res[0][0]
                            
                            print(strtime)

                            finalStr = GetCloseVoltage(strtime)

                            all_strings = [item for sublist in finalStr for item in sublist]

                            unique_strings = set(all_strings)

                            strings = ','.join(t for t in unique_strings)

                            strings = strings+',DCHG'

                            print(strings)

                            li = strings.split(',')

                            if len(li) >= 2:
                                apiNo = str(len(li)-1)

                                ApiUrl = API_mapping[apiNo]
                            else:
                                time.sleep(3)
                                continue

                            crate = '0.1'

                            ApiUrl = ApiUrl+f'?strings={strings}&crate={crate}'
                            print(ApiUrl)

                            ONurl = requests.get(ApiUrl)

                            resJson = ONurl.json()

                            if resJson:
                                send_mail(peak,li,crate)

                    if 'DCHG' not in dchgLi and 'ON' not in MainLi and avgVolt > 670:
                        print(dchgLi,MainLi)

                        str1lag = (curtime - str1Time).total_seconds()
                        str2lag = (curtime - str2Time).total_seconds()
                        str3lag = (curtime - str3Time).total_seconds()
                        str4lag = (curtime - str4Time).total_seconds()
                        str5lag = (curtime - str5Time).total_seconds()

                        def GetCloseVoltage(dictionary):
                            close_pairs = []
                            keys = list(dictionary.keys())
                            for i in range(len(keys)):
                                for j in range(i + 1, len(keys)):
                                    key1 = keys[i]
                                    key2 = keys[j]
                                    diff = abs(dictionary[key1] - dictionary[key2])
                                    if diff <= 10:
                                        close_pairs.append((key1, key2))
                            return close_pairs

                        strtime = {}

                        if str1lag <= 500:
                            strtime['str1'] = str1Res[0][0]
                        if str2lag <= 500:
                            strtime['str2'] = str2Res[0][0]
                        if str3lag <= 500:
                            strtime['str3'] = str3Res[0][0]
                        if str4lag <= 500:
                            strtime['str4'] = str4Res[0][0]
                        if str5lag <= 500:
                            strtime['str5'] = str5Res[0][0]
                       
                        print(strtime)

                        finalStr = GetCloseVoltage(strtime)

                        all_strings = [item for sublist in finalStr for item in sublist]

                        unique_strings = set(all_strings)

                        strings = ','.join(t for t in unique_strings)

                        strings = strings+',DCHG'

                        print(strings)

                        li = strings.split(',')

                        if len(li) >= 2:
                            apiNo = str(len(li)-1)

                            ApiUrl = API_mapping[apiNo]
                        else:
                            time.sleep(3)
                            continue

                        crate = '0.1'

                        ApiUrl = ApiUrl+f'?strings={strings}&crate={crate}'
                        print(ApiUrl)

                        ONurl = requests.get(ApiUrl)

                        resJson = ONurl.json()

                        if resJson:
                            send_mail(peak,li,crate)

                    elif 'DCHG' in dchgLi:
                        print("CHECK DCHG")
                        client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
                        client_conv.connect()
                        read=client_conv.read_holding_registers(address = 1532)
                        current = read.registers[0]
                        print(current)

                        count = 0
                        for i in dchgLi:
                            if i == 'DCHG':
                                count += 1
                        current = int(current / count)


                        print("current:",current)

                        if peakAvg != None and peakAvg <= 4300 and (current >= 100 or current <= 150):
                            current = 50*count
                            hx = dchgHex(current)
                            crate = "5C6500000009011005FC000102"+hx

                            CrateRes = CurrentSet(crate)

                            if CrateRes == 'Success':
                                print("Crate set to:",current)
                                time.sleep(100)
                            else:
                                time.sleep(5)
                                continue
                        
                        elif peakAvg != None and peakAvg >= 4300 and peakAvg <= 4400 and (current >= 50 or current <= 150):
                            print(peakAvg,"> 4300")
                            current = 100*count
                            hx = dchgHex(current)
                            crate = "5C6500000009011005FC000102"+hx

                            CrateRes = CurrentSet(crate)

                            if CrateRes == 'Success':
                                print("Crate set to:",current)
                                time.sleep(100)
                            else:
                                time.sleep(5)
                                continue

                        elif peak != None and peak >= 4400 and (current >= 50 or current <= 100):
                            print(peak, ">4400")
                            current = 150*count
                            hx = dchgHex(current)
                            crate = "5C6500000009011005FC000102"+hx

                            CrateRes = CurrentSet(crate)

                            if CrateRes == 'Success':
                                print("Crate set to:",current)
                                time.sleep(600)
                            else:
                                time.sleep(5)
                                continue
                        
                        if peakAvg != None:
                            bmscur.execute("SELECT totalApparentPower1 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = curdate() order by recordId desc limit 20;")

                            peakChkRes = bmscur.fetchall()

                            count = 0

                            for i in peakChkRes:
                                if i[0] != None:
                                    if peakDemand - i[0] >= 150:
                                        count += 1
                            print(count)
                            if count >= 20:
                                ApiUrl = f"http://{host}:8009/ioeprocessoff"    
                                OFFurl = requests.get(ApiUrl)

                                resJson = OFFurl.json()

                                if resJson:
                                    send_mail_off(peak,'DCHG')                
                            else:
                                print("Peak above the limit")
                    # http://localhost:8000/ioetriplestr?strings=str2,str3,str4,CHG
            else:
                curtime = datetime.now()

                current_date = curtime.replace(hour=0, minute=0, second=0, microsecond=0)

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt1BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str1Res = emscur.fetchall()

                if len(str1Res) > 0:
                    try:
                        str1Time = str1Res[0][4]
                        voltLi.append(str1Res[0][0])
                        dchgLi.append(str1Res[0][3])
                        MainLi.append(str1Res[0][1])
                        PreLi.append(str1Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str1Time = current_date

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt2BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str2Res = emscur.fetchall()

                if len(str2Res) > 0:
                    try:
                        str2Time = str2Res[0][4]
                        voltLi.append(str2Res[0][0])
                        dchgLi.append(str2Res[0][3])
                        MainLi.append(str2Res[0][1])
                        PreLi.append(str2Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str2Time = current_date
                

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt3BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str3Res = emscur.fetchall()

                if len(str3Res) > 0:
                    try:
                        str3Time = str3Res[0][4]
                        voltLi.append(str3Res[0][0])
                        dchgLi.append(str3Res[0][3])
                        MainLi.append(str3Res[0][1])
                        PreLi.append(str3Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str3Time = current_date
                

                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt4BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str4Res = emscur.fetchall()

                if len(str4Res) > 0:
                    try:
                        str4Time = str4Res[0][4]
                        voltLi.append(str4Res[0][0])
                        dchgLi.append(str4Res[0][3])
                        MainLi.append(str4Res[0][1])
                        PreLi.append(str4Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str4Time = current_date
                
                emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                                FROM EMS.ioeSt5BatteryData 
                                where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
                
                str5Res = emscur.fetchall()

                if len(str5Res) > 0:
                    try:
                        str5Time = str5Res[0][4]
                        voltLi.append(str5Res[0][0])
                        dchgLi.append(str5Res[0][3])
                        MainLi.append(str5Res[0][1])
                        PreLi.append(str5Res[0][2])
                    except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
                else:
                    str5Time = current_date

                print(dchgLi)
                print(MainLi)
                print(voltLi)

                volt = 0
                count = 0

                for i in voltLi:
                    if i != None and i > 0:
                        volt += i
                        count += 1

                if volt != 0:
                    avgVolt = volt/count

                if 'DCHG' in dchgLi:
                    try:
                        client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
                        client_conv.connect()
                        read=client_conv.read_holding_registers(address = 1532)
                        current = read.registers[0]
                    except Exception as ex:
                        print(ex)
                        continue
                    print(current)

                    count = 0

                    for i in dchgLi:
                        if i == 'DCHG':
                            count += 1

                    current = int(current / count)
                    print("current:",current)

                    if peakAvg != None and peakAvg <= 4300 and (current == 100 or current == 150):
                        current = 50*count
                        hx = dchgHex(current)
                        crate = "5C6500000009011005FC000102"+hx

                        CrateRes = CurrentSet(crate)
                        if CrateRes == 'Success':
                            print("Crate set to:",current)
                            time.sleep(100)
                        else:
                            time.sleep(5)
                            continue


                    if peakAvg != None and peakAvg >= 4300 and peakAvg <= 4400 and (current == 50 or current == 150):
                        current = 100*count
                        hx = dchgHex(current)
                        crate = "5C6500000009011005FC000102"+hx

                        CrateRes = CurrentSet(crate)
                        if CrateRes == 'Success':
                            print("Crate set to:",current)
                            time.sleep(100)
                        else:
                            time.sleep(5)
                            continue

                    if peak != None and peak >= 4400 and (current == 50 or current == 100):
                        current = 150*count
                        hx = dchgHex(current)
                        crate = "5C6500000009011005FC000102"+hx

                        CrateRes = CurrentSet(crate)
                        if CrateRes == 'Success':
                            print("Crate set to:",current)
                            time.sleep(600)
                        else:
                            time.sleep(5)
                            continue
                    
                    if peakAvg != None:

                        bmscur.execute("SELECT totalApparentPower1 FROM bmsmgmt_olap_prod_v13.hvacSchneider7230Polling where date(polledTime) = curdate() order by recordId desc limit 20;")

                        peakChkRes = bmscur.fetchall()

                        count = 0

                        for i in peakChkRes:
                            if i[0] != None:
                                if peakDemand - i[0] >= 150:
                                    count += 1
                        print(count)
                        if count >= 20:
                            ApiUrl = f"http://{host}:8009/ioeprocessoff"    
                            OFFurl = requests.get(ApiUrl)

                            resJson = OFFurl.json()

                            if resJson:
                                send_mail_off(peakAvg,'DCHG')                
                        else:
                            print("Peak above the limit")
            awscur.close()
            emscur.close()
            bmscur.close()

    if curntime >= 22 or curntime <= 3:
        chgLi = []
        MainLi = []
        voltLi = []
        PreLi = []
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
            continue

        curtime = datetime.now()

        current_date = curtime.replace(hour=0, minute=0, second=0, microsecond=0)

        emscur.execute("select polledTime,onReason,offReason from EMS.ioeOnOff where date(polledTime) = curdate() order by polledTime desc limit 1;")
        sts = None
        fchk = emscur.fetchall()
        if len(fchk) > 0:
            curntime = int(str(fchk[0][0])[11:13])
            if curntime >= 22 or curntime <= 3:
                if fchk[0][2]:
                    sts = "NO"
            else:
                sts = "OK"
        else:
            sts = "OK"

        if sts == "OK":
            emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                            FROM EMS.ioeSt1BatteryData 
                            where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
            
            str1Res = emscur.fetchall()

            if len(str1Res) > 0:
                try:
                    str1Time = str1Res[0][4]
                    voltLi.append(str1Res[0][0])
                    chgLi.append(str1Res[0][3])
                    MainLi.append(str1Res[0][1])
                    PreLi.append(str1Res[0][2])
                except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
            else:
                str1Time = current_date

            emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                            FROM EMS.ioeSt2BatteryData 
                            where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
            
            str2Res = emscur.fetchall()

            if len(str2Res) > 0:
                try:
                    str2Time = str2Res[0][4]
                    voltLi.append(str2Res[0][0])
                    chgLi.append(str2Res[0][3])
                    MainLi.append(str2Res[0][1])
                    PreLi.append(str2Res[0][2])
                except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
            else:
                str2Time = current_date

            emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                            FROM EMS.ioeSt3BatteryData 
                            where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
            
            str3Res = emscur.fetchall()

            if len(str3Res) > 0:
                try:
                    str3Time = str3Res[0][4]
                    voltLi.append(str3Res[0][0])
                    chgLi.append(str3Res[0][3])
                    MainLi.append(str3Res[0][1])
                    PreLi.append(str3Res[0][2])
                except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
            else:
                str3Time = current_date

            emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                            FROM EMS.ioeSt4BatteryData 
                            where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
            
            str4Res = emscur.fetchall()

            if len(str4Res) > 0:
                try:
                    str4Time = str4Res[0][4]
                    voltLi.append(str4Res[0][0])
                    chgLi.append(str4Res[0][3])
                    MainLi.append(str4Res[0][1])
                    PreLi.append(str4Res[0][2])
                except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
            else:
                str4Time = current_date

            emscur.execute("""SELECT batteryVoltage,mainContactorStatus,prechargeContactorStatus,batteryStatus,recordTimestamp 
                            FROM EMS.ioeSt5BatteryData 
                            where date(recordTimestamp) = curdate() order by recordId desc limit 1;""")
            
            str5Res = emscur.fetchall()

            if len(str5Res) > 0:
                try:
                    str5Time = str5Res[0][4]
                    voltLi.append(str5Res[0][0])
                    chgLi.append(str5Res[0][3])
                    MainLi.append(str5Res[0][1])
                    PreLi.append(str5Res[0][2])
                except Exception as ex:
                        print(ex)
                        time.sleep(10)
                        continue
            else:
                str5Time = current_date
            
            volt = sum(voltLi)/len(voltLi)
            if len(chgLi) > 0 and len(MainLi) > 0:
                if 'CHG' not in chgLi and 'ON' not in MainLi:
                    str1lag = (curtime - str1Time).total_seconds()
                    str2lag = (curtime - str2Time).total_seconds()
                    str3lag = (curtime - str3Time).total_seconds()
                    str4lag = (curtime - str4Time).total_seconds()
                    str5lag = (curtime - str5Time).total_seconds()

                    def GetCloseVoltage(dictionary):
                        close_pairs = []
                        keys = list(dictionary.keys())
                        for i in range(len(keys)):
                            for j in range(i + 1, len(keys)):
                                key1 = keys[i]
                                key2 = keys[j]
                                diff = abs(dictionary[key1] - dictionary[key2])
                                if diff <= 10:
                                    close_pairs.append((key1, key2))
                        return close_pairs

                    strtime = {}

                    try:
                        if str1lag <= 500:
                            strtime['str1'] = str1Res[0][0]
                        if str2lag <= 500:
                            strtime['str2'] = str2Res[0][0]
                        if str3lag <= 500:
                            strtime['str3'] = str3Res[0][0]
                        if str4lag <= 500:
                            strtime['str4'] = str4Res[0][0]
                        if str5lag <= 500:
                            strtime['str5'] = str5Res[0][0]
                    except Exception as ex:
                        print(ex)
                        time.sleep(30)
                        continue
                    
                    print(strtime)

                    finalStr = GetCloseVoltage(strtime)

                    print(finalStr)

                    all_strings = [item for sublist in finalStr for item in sublist]

                    unique_strings = set(all_strings)

                    strings = ','.join(t for t in unique_strings)

                    strings = strings+',CHG'

                    print(strings)

                    li = strings.split(',')

                    if len(li) >= 2:
                        apiNo = str(len(li)-1)

                        ApiUrl = API_mapping[apiNo]
                    else:
                        time.sleep(3)
                        continue

                    crate = '0.2'

                    ApiUrl = ApiUrl+f'?strings={strings}&crate={crate}'
                    print(ApiUrl)
                    if volt <= 770:
                        ONurl = requests.get(ApiUrl)

                        print(ONurl.json())
                    else:
                        print("Charged!")

        emscur.close()
    time.sleep(10)