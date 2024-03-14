from flask import Flask, jsonify, request
import socket
import time
from hotWaterReply import hot_water

app = Flask(__name__)

server_ip = "10.9.240.70"
server_port = 22555

    
manual = "003A00000006010600100001"
FrWOn = "008C00000009011000110001020000"
RcOn = "002500000009011000110001020001"
DchgOn = "005A00000009011000110001020002"
Idle = "007600000009011000110001020003"


def check_authentication(token):
    # Replace this with your own logic to validate the token
    valid_token = "VKOnNhH2SebMU6S"
    return token == valid_token


def ChgRCOn():
    RcOn = "002500000009011000110001020001"
    hotWaterClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        hotWaterClient.connect((server_ip, server_port))
    except Exception as ex:
        print(f"Error connecting to server: {ex}")
        ChgRCOn()
        
    chargerc = bytes.fromhex(RcOn)
    hotWaterClient.send(chargerc)
    print("Charge Recirculate sent")

    time.sleep(2)

    res = hot_water()

    print(res)
    
    return res





def ChgFWMetOn():
    hotWaterClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        hotWaterClient.connect((server_ip, server_port))
    except Exception as ex:
        print(f"Error connecting to server: {ex}")
        ChgFWMetOn()

        # ManualOn = bytes.fromhex(manual)
        # hotWaterClient.send(ManualOn)
        # print("Manual On sent")

    chargefw = bytes.fromhex(FrWOn)
    hotWaterClient.send(chargefw)
    print("Charge Freshwater sent")

    time.sleep(2)

    res = hot_water()

    print(res)
    
    return res
        

def DchgMetOn():
    hotWaterClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        hotWaterClient.connect((server_ip, server_port))
    except Exception as ex:
        print(f"Error connecting to server: {ex}")
        DchgMetOn()

        # ManualOn = bytes.fromhex(manual)
        # hotWaterClient.send(ManualOn)
        # print("Manual On sent")

    discharge = bytes.fromhex(DchgOn)
    hotWaterClient.send(discharge)
    print("Discharge sent")

    time.sleep(2)

    res = hot_water()

    print(res)
    
    return res


def ProcessOff():
    hotWaterClient = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        hotWaterClient.connect((server_ip, server_port))
    except Exception as ex:
        print(f"Error connecting to server: {ex}")
        ProcessOff()

        # ManualOn = bytes.fromhex(manual)
        # hotWaterClient.send(ManualOn)
        # print("Manual On sent")

    idlecom = bytes.fromhex(Idle)
    hotWaterClient.send(idlecom)
    print("Idle sent")

    time.sleep(5)

    res = hot_water()

    print(res)
    
    return res



@app.route('/hotWaterRCon', methods = ['GET'])
def hotRC():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        res = ChgRCOn()

        print('result',res)

        if res['Status'] == 'CHGRW':
            return {'message':'CHGRW'}
        else:
            ChgRCOn()
        



@app.route('/hotWaterFWon', methods = ['GET'])
def hotFW():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        res = ChgFWMetOn()

        print('result',res)

        if res['Status'] == 'CHGFW':
            return {'message':'CHGFW'}
        else:
            ChgFWMetOn()



@app.route('/hotWaterDCon', methods = ['GET'])
def hotDC():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        res = DchgMetOn()

        print('result',res)

        if res['Status'] == 'DCHG':
            return {'message':'DCHG'}
        else:
            DchgMetOn()


@app.route('/hotWaterIDon', methods = ['GET'])
def hotID():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        res = ProcessOff()

        print('result',res)

        if res['Status'] == 'IDLE':
            return {'message':'IDLE'}
        else:
            ProcessOff()

        
if __name__ == '__main__':
    app.run(host="localhost",port=8008)