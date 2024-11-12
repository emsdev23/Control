import requests
import time
from datetime import datetime
import mysql.connector
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

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


def send_mail(voltage):
    curtime = datetime.now()

    message = MIMEMultipart('alternative')
    message['Subject'] = f'EMS ALERT - LTO Battery OFF'
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
                                <td>Voltage Limt {voltage}</td>
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
                                    <td>LTO Battery OFF</td>
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

headers = {'Authorization': 'VKOnNhH2SebMU6S'}

params = {'crate': '1D5A000000090210000C000102FFD8'}

dchgonurl = 'http://localhost:8007/ltoDischargeon'
dchgoffurl = 'http://localhost:8007/ltoDischargeoff'
chgonurl = 'http://localhost:8007/ltoChargeon'
chgoffurl = 'http://localhost:8007/ltoChargeoff'
chkurl = 'http://localhost:8007/ltofault'
limurl = 'http://localhost:8007/ltolimit'

def dchgon():
    print("Discharge ON")
    upsres = requests.get(dchgonurl,params=params ,headers=headers)
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

def chgon():
    print("Charge ON")
    upsres = requests.get(chgonurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        chgon()
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
    time.sleep(10)

def chkflt():
    print("Fault Check")
    upsres = requests.get(chkurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        chkflt()
    else:
        print(data)
    time.sleep(7)

def chklim():
    print("Voltage Limit")
    upsres = requests.get(limurl, headers=headers)
    data = upsres.json()
    if data == None:
        time.sleep(2)
        print(data)
        chklim()
    else:
        print(data)
        try:
            volt = int(data['message'][0])
            sts = data['message'][1]
        except:
            volt = None
            sts = None
            print("Error")
        if volt != None and sts != None:
            if volt >= 417 and sts == "CHG":
                chgoff()
                send_mail(volt)
            elif volt <= 350 and sts == "DCHG":
                dchgoff()
                send_mail(volt)
    time.sleep(7)

# function_names = ['dchgon','dchgoff','chgon','chgoff','chkflt','chklim']

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
    except:
        continue

    emscur.execute("select functionCode,controlStatus,recordTime from ltoInstantControl order by recordid desc limit 1")

    result = emscur.fetchall()

    curtime = datetime.now()
    
    secs = (curtime-result[0][2]).total_seconds()
    sts = result[0][1]
    code = result[0][0]

    if secs <= 60:
        print(secs)
        if sts == "CHARGE":
            if code == "ON":
                chgon()      
            elif code == "OFF":
                chgoff()
        elif sts == "DISCHARGE":
            if code == "ON":
                dchgon()
            elif code == "OFF":
                dchgoff()
    else:
        print(f"Last command before {secs}")
    

    chklim()    

    # chkflt()

    time.sleep(8)