from flask import Flask, jsonify, request
import socket
from pymodbus.client.tcp import ModbusTcpClient
import time
import requests

app = Flask(__name__)

startConv = bytes.fromhex("D9CD00000009011004870001020009")
stopConv = bytes.fromhex("D9CD00000009011004870001020000")

netCloseON = bytes.fromhex("DAEE00000009011006430001020002")
netCloseOFF = bytes.fromhex("DAEE0000000901100643000102000F")

netSyncON = bytes.fromhex("DB7600000009011006410001020002")
netSyncOFF = bytes.fromhex("DB760000000901100641000102000E")

preOFFst5 = bytes.fromhex("851825150C0200FFFFFFFF0000")
preONst5 = bytes.fromhex("851825150C01FFFFFFFF000000")
preOFFst4 = bytes.fromhex("851824140C0200FFFFFFFF0000")
preONst4 = bytes.fromhex("851824140C01FFFFFFFF000000")
preOFFst3 = bytes.fromhex("851823130C0200FFFFFFFF0000")
preONst3 = bytes.fromhex("851823130C01FFFFFFFF000000")
preOFFst2 = bytes.fromhex("851822120C02FFFFFFFF000000")
preONst2 = bytes.fromhex("851822120C0100FFFFFFFF0000")
preOFFst1 = bytes.fromhex("851821110C0200FFFFFFFF0000")
preONst1 = bytes.fromhex("851821110C01FFFFFFFF000000")

preOFF_mapping = {
    '1': preOFFst1,
    '2': preOFFst2,
    '3': preOFFst3,
    '4': preOFFst4,
    '5': preOFFst5,
}

preON_mapping = {
    '1': preONst1,
    '2': preONst2,
    '3': preONst3,
    '4': preONst4,
    '5': preONst5,
}

Crate_Map = {
    '0.1':50,
    '0.2':100,
    '0.3':150,
    '0.4':180,
    '0.5':200
}

mainONst5 = bytes.fromhex("851825150C0001FFFFFFFF0000")
mainOFFs5 = bytes.fromhex("851825150C0002FFFFFFFF0000")
mainONst4 = bytes.fromhex("851824140C0001FFFFFFFF0000")
mainOFFs4 = bytes.fromhex("851824140C0002FFFFFFFF0000")
mainONst3 = bytes.fromhex("851823130C0001FFFFFFFF0000")
mainOFFs3 = bytes.fromhex("851823130C0002FFFFFFFF0000")
mainONst2 = bytes.fromhex("851822120C0001FFFFFFFF0000")
mainOFFs2 = bytes.fromhex("851822120C0002FFFFFFFF0000")
mainONst1 = bytes.fromhex("851821110C0001FFFFFFFF0000")
mainOFFs1 = bytes.fromhex("851821110C0002FFFFFFFF0000")

def dchgHex(amps):
    hx = '{:04x}'.format(amps)

    return hx

def chgHex(amps):
    amps = -abs(amps)
    hx = '{:X}'.format((amps + (1 << 16)) % (1 << 16))

    return hx

mainON_mapping = {
    '1': mainONst1,
    '2': mainONst2,
    '3': mainONst3,
    '4': mainONst4,
    '5': mainONst5,
}

mainOFF_mapping = {
    '1': mainOFFs1,
    '2': mainOFFs2,
    '3': mainOFFs3,
    '4': mainOFFs4,
    '5': mainOFFs5,
}

host = '121.242.232.211'

str_reply = {
    '1':f'http://{host}:5008/ioest1reply',
    '2':f'http://{host}:5008/ioest2reply',
    '3':f'http://{host}:5008/ioest3reply',
    '4':f'http://{host}:5008/ioest4reply',
    '5':f'http://{host}:5008/ioest5reply'
}

bat_server_ip = "10.9.244.8"  # Replace with your server's IP address
bat_server_port = 502

conv_server_ip = "10.9.242.6"  # Replace with your server's IP address
conv_server_port = 502 



@app.route('/ioesinglestr', methods = ['GET'])
def ioe1str():
    value = request.args.get('strings')
    crate = request.args.get('crate')
    if value and crate:
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')

        strs = value.split(',')
        str1 = strs[0][-1]
        func = strs[-1].upper()
        print(str1)
        print(func)

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
                print("Process Started")
                time.sleep(40)
            else:
                time.sleep(2)
                conv_client_socket.close()
                CurrentSet(crate)        

        def NetCloseON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netCloseON)
            print("Net Close ON")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1603)
            convNetclose = read.registers[0]
            print(convNetclose)
            client_conv.close()
            if convNetclose == 2:
                conv_client_socket.close()
                CurrentSet(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetCloseON(crate)

        def NetSyncON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netSyncON)
            print("Net sync on")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1601)
            convNetsync = read.registers[0]
            print(convNetsync)
            client_conv.close()
            if convNetsync == 2:
                conv_client_socket.close()
                NetCloseON(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetSyncON(crate)

        def ConvertorON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(startConv)
            print("Convertor Start sent")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1159)
            convStrt = (read.registers[0])
            print(convStrt)
            client_conv.close()
            if convStrt == 9:
                conv_client_socket.close()
                NetSyncON(crate)
            else:
                time.sleep(2)
                conv_client_socket.close()
                ConvertorON(crate) 

        def CheckBatSts():
            url = str_reply[str1]
            str1res = requests.get(url)
            str1data = str1res.json()
            if str1data['batteryCurent'] <= 5:
                if func == "CHG":
                    amps = Crate_Map[crate]
                    hx = chgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
                if func == "DCHG":
                    amps = Crate_Map[crate]
                    hx = dchgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
            else:
                time.sleep(3)
                CheckBatSts()

        def MainON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                MainON()
            bat_client_socket.send(mainON_mapping[str1])
            time.sleep(2)
            url = str_reply[str1]
            str1res = requests.get(url)
            str1data = str1res.json()
            if str1data['mainConsSts'] == '1':
                print(f"Main ON string{str1} success")
                CheckBatSts()
            else:
                MainON()


        def PreON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                PreON()
            bat_client_socket.send(preON_mapping[str1])
            time.sleep(2)
            url = str_reply[str1]
            str1res = requests.get(url)
            str1data = str1res.json()
            if str1data['preConSts'] == '1':
                print(f"Pre ON string{str1} success")
                MainON()
            else:
                PreON()

        if str1 in preON_mapping:
            PreON()

        response = {'message': f'Process started : {value}'}
        return response
    else:
        response = {'message': 'No string(s) provided in the request'}

        return response


@app.route('/ioedoublestr', methods = ['GET'])
def ioe2str():
    value = request.args.get('strings')
    crate = request.args.get('crate')
    if value and crate:
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')

        strs = value.split(',')
        str1 = strs[0][-1]
        str2 = strs[1][-1]
        func = strs[-1].upper()

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
                print("Process Started")
                time.sleep(40)
            else:
                time.sleep(2)
                conv_client_socket.close()
                CurrentSet(crate)        

        def NetCloseON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netCloseON)
            print("Net Close ON")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1603)
            convNetclose = read.registers[0]
            print(convNetclose)
            client_conv.close()
            if convNetclose == 2:
                conv_client_socket.close()
                CurrentSet(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetCloseON(crate)

        def NetSyncON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netSyncON)
            print("Net sync on")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1601)
            convNetsync = read.registers[0]
            print(convNetsync)
            client_conv.close()
            if convNetsync == 2:
                conv_client_socket.close()
                NetCloseON(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetSyncON(crate)

        def ConvertorON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(startConv)
            print("Convertor Start sent")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1159)
            convStrt = (read.registers[0])
            print(convStrt)
            client_conv.close()
            if convStrt == 9:
                conv_client_socket.close()
                NetSyncON(crate)
            else:
                time.sleep(2)
                conv_client_socket.close()
                ConvertorON(crate)

        def CheckBatSts():
            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            if str1data['batteryCurent'] <= 5 and str2data['batteryCurent'] <= 5:
                if func == "CHG":
                    amps = Crate_Map[crate]*2
                    hx = chgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
                if func == "DCHG":
                    amps = Crate_Map[crate]*2
                    hx = dchgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
            else:
                time.sleep(3)
                CheckBatSts()

        def MainON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                MainON()
            bat_client_socket.send(mainON_mapping[str1])
            bat_client_socket.send(mainON_mapping[str2])
            time.sleep(2)

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            if str1data['mainConsSts'] == '1' and str2data['mainConsSts'] == '1':
                print(f"Main ON string{str1} success")
                CheckBatSts()
            else:
                MainON()

        def PreON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                PreON()
            bat_client_socket.send(preON_mapping[str1])
            bat_client_socket.send(preON_mapping[str2])
            time.sleep(2)

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            if str1data['preConSts'] == '1' and str2data['preConSts'] == '1':
                print(f"Pre ON string{str1} success")
                MainON()
            else:
                PreON()

        if str1 in preON_mapping and str2 in preON_mapping:
            PreON()

        response = {'message': f'Process started : {value}'}
        return response
    else:
        response = {'message': 'No string(s) provided in the request'}

        return response



@app.route('/ioetriplestr', methods = ['GET'])
def ioe3str():
    value = request.args.get('strings')
    crate = request.args.get('crate')
    if value and crate:
        strs = value.split(',')
        str1 = strs[0][-1]
        str2 = strs[1][-1]
        str3 = strs[2][-1]
        func = strs[-1].upper()

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
                print("Process Started")
                time.sleep(40)
            else:
                time.sleep(2)
                conv_client_socket.close()
                CurrentSet(crate)        

        def NetCloseON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netCloseON)
            print("Net Close ON")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1603)
            convNetclose = read.registers[0]
            print(convNetclose)
            client_conv.close()
            if convNetclose == 2:
                conv_client_socket.close()
                CurrentSet(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetCloseON(crate)

        def NetSyncON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netSyncON)
            print("Net sync on")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1601)
            convNetsync = read.registers[0]
            print(convNetsync)
            client_conv.close()
            if convNetsync == 2:
                conv_client_socket.close()
                NetCloseON(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetSyncON(crate)

        def ConvertorON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(startConv)
            print("Convertor Start sent")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1159)
            convStrt = (read.registers[0])
            print(convStrt)
            client_conv.close()
            if convStrt == 9:
                conv_client_socket.close()
                NetSyncON(crate)
            else:
                time.sleep(2)
                conv_client_socket.close()
                ConvertorON(crate) 

        def CheckBatSts():
            print("Checking Battery Status")
            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            if str1data['batteryCurent'] <= 5 and str2data['batteryCurent'] <= 5 and str3data['batteryCurent'] <= 5:
                if func == "CHG":
                    amps = Crate_Map[crate]*3
                    hx = chgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
                if func == "DCHG":
                    amps = Crate_Map[crate]*3
                    hx = dchgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
            else:
                time.sleep(3)
                CheckBatSts()

        def MainON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                MainON()
            bat_client_socket.send(mainON_mapping[str1])
            bat_client_socket.send(mainON_mapping[str2])
            bat_client_socket.send(mainON_mapping[str3])
            time.sleep(2)

            print("Main ON sent")

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            if str1data['mainConsSts'] == '1' and str2data['mainConsSts'] == '1' and str3data['mainConsSts'] == '1':
                print(f"Main ON string{str1,str2,str3} success")
                CheckBatSts()
            else:
                print("Main not ON")
                MainON()

        def PreON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                PreON()
            bat_client_socket.send(preON_mapping[str1])
            bat_client_socket.send(preON_mapping[str2])
            bat_client_socket.send(preON_mapping[str3])
            time.sleep(2)

            print("PRE ON sent")

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            if str1data['preConSts'] == '1' and str2data['preConSts'] == '1' and str3data['preConSts'] == '1':
                print(f"Pre ON string{str1,str2,str3} success")
                MainON()
            else:
                print("Pre not ON")
                PreON()

        if str1 in preON_mapping and str2 in preON_mapping and str3 in preON_mapping:
            PreON()

        response = {'message': f'Process started : {value}'}
        return response
    else:
        response = {'message': 'No string(s) provided in the request'}

        return response


@app.route('/ioefourstr', methods = ['GET'])
def ioe4str():
    value = request.args.get('strings')
    crate = request.args.get('crate')
    if value and crate:
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')

        strs = value.split(',')
        str1 = strs[0][-1]
        str2 = strs[1][-1]
        str3 = strs[2][-1]
        str4 = strs[3][-1]
        func = strs[-1].upper()

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
                print("Process Started")
                time.sleep(40)
            else:
                time.sleep(2)
                conv_client_socket.close()
                CurrentSet(crate)        

        def NetCloseON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netCloseON)
            print("Net Close ON")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1603)
            convNetclose = read.registers[0]
            print(convNetclose)
            client_conv.close()
            if convNetclose == 2:
                conv_client_socket.close()
                CurrentSet(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetCloseON(crate)

        def NetSyncON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netSyncON)
            print("Net sync on")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1601)
            convNetsync = read.registers[0]
            print(convNetsync)
            client_conv.close()
            if convNetsync == 2:
                conv_client_socket.close()
                NetCloseON(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetSyncON(crate)

        def ConvertorON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(startConv)
            print("Convertor Start sent")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1159)
            convStrt = (read.registers[0])
            print(convStrt)
            client_conv.close()
            if convStrt == 9:
                conv_client_socket.close()
                NetSyncON(crate)
            else:
                time.sleep(2)
                conv_client_socket.close()
                ConvertorON(crate)  

        def CheckBatSts():
            print("Checking Battery Status")
            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            url4 = str_reply[str4]
            str4res = requests.get(url4)
            str4data = str4res.json()


            if str1data['batteryCurent'] <= 5 and str2data['batteryCurent'] <= 5 and str3data['batteryCurent'] <= 5 and str4data['batteryCurent'] <= 5:
                if func == "CHG":
                    amps = Crate_Map[crate]*4
                    hx = chgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
                if func == "DCHG":
                    amps = Crate_Map[crate]*4
                    hx = dchgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
            else:
                time.sleep(3)
                CheckBatSts()

        def MainON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                MainON()
            bat_client_socket.send(mainON_mapping[str1])
            bat_client_socket.send(mainON_mapping[str2])
            bat_client_socket.send(mainON_mapping[str3])
            bat_client_socket.send(mainON_mapping[str4])
            time.sleep(2)

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            url4 = str_reply[str4]
            str4res = requests.get(url4)
            str4data = str4res.json()

            if str1data['mainConsSts'] == '1' and str2data['mainConsSts'] == '1' and str3data['mainConsSts'] == '1' and str4data['mainConsSts'] == '1':
                print(f"Main ON string{str1,str2,str3,str4} success")
                CheckBatSts()
            else:
                MainON()

        def PreON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                PreON()
            bat_client_socket.send(preON_mapping[str1])
            bat_client_socket.send(preON_mapping[str2])
            bat_client_socket.send(preON_mapping[str3])
            bat_client_socket.send(preON_mapping[str4])
            time.sleep(2)

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            url4 = str_reply[str4]
            str4res = requests.get(url4)
            str4data = str4res.json()

            if str1data['preConSts'] == '1' and str2data['preConSts'] == '1' and str3data['preConSts'] == '1' and str4data['preConSts'] == '1':
                print(f"Pre ON string{str1,str2,str3,str4} success")
                MainON()
            else:
                PreON()

        if str1 in preON_mapping and str2 in preON_mapping and str3 in preON_mapping and str4 in preON_mapping:
            PreON()

        response = {'message': f'Process started : {value}'}
        return response
    else:
        response = {'message': 'No string(s) provided in the request'}

        return response
    
@app.route('/ioefivestr', methods = ['GET'])
def ioe5str():
    value = request.args.get('strings')
    crate = request.args.get('crate')
    if value and crate:
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')

        strs = value.split(',')
        str1 = strs[0][-1]
        str2 = strs[1][-1]
        str3 = strs[2][-1]
        str4 = strs[3][-1]
        str5 = strs[4][-1]
        func = strs[-1].upper()

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
                print("Process Started")
                time.sleep(40)
            else:
                time.sleep(2)
                conv_client_socket.close()
                CurrentSet(crate)        

        def NetCloseON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netCloseON)
            print("Net Close ON")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1603)
            convNetclose = read.registers[0]
            print(convNetclose)
            client_conv.close()
            if convNetclose == 2:
                conv_client_socket.close()
                CurrentSet(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetCloseON(crate)

        def NetSyncON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(netSyncON)
            print("Net sync on")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1601)
            convNetsync = read.registers[0]
            print(convNetsync)
            client_conv.close()
            if convNetsync == 2:
                conv_client_socket.close()
                NetCloseON(crate)
            else:
                conv_client_socket.close()
                time.sleep(2)
                NetSyncON(crate)

        def ConvertorON(crate):
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:        
                print(ex,'to convertor')
            conv_client_socket.send(startConv)
            print("Convertor Start sent")
            time.sleep(1)
            client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
            client_conv.connect()
            read=client_conv.read_holding_registers(address = 1159)
            convStrt = (read.registers[0])
            print(convStrt)
            client_conv.close()
            if convStrt == 9:
                conv_client_socket.close()
                NetSyncON(crate)
            else:
                time.sleep(2)
                conv_client_socket.close()
                ConvertorON(crate) 

        def CheckBatSts():
            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            url4 = str_reply[str4]
            str4res = requests.get(url4)
            str4data = str4res.json()

            url5 = str_reply[str5]
            str5res = requests.get(url5)
            str5data = str5res.json()


            if str1data['batteryCurent'] <= 5 and str2data['batteryCurent'] <= 5 and str3data['batteryCurent'] <= 5 and str4data['batteryCurent'] <= 5 and str5data['batteryCurent'] <= 5:
                if func == "CHG":
                    amps = Crate_Map[crate]*5
                    hx = chgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
                if func == "DCHG":
                    amps = Crate_Map[crate]*5
                    hx = dchgHex(amps)
                    Crate = "5C6500000009011005FC000102"+hx
                    ConvertorON(Crate) 
            else:
                time.sleep(3)
                CheckBatSts()

        def MainON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                MainON()
            bat_client_socket.send(mainON_mapping[str1])
            bat_client_socket.send(mainON_mapping[str2])
            bat_client_socket.send(mainON_mapping[str3])
            bat_client_socket.send(mainON_mapping[str4])
            bat_client_socket.send(mainON_mapping[str5])
            time.sleep(2)

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            url4 = str_reply[str4]
            str4res = requests.get(url4)
            str4data = str4res.json()

            url5 = str_reply[str5]
            str5res = requests.get(url5)
            str5data = str5res.json()

            if str1data['mainConsSts'] == '1' and str2data['mainConsSts'] == '1' and str3data['mainConsSts'] == '1' and str4data['mainConsSts'] == '1' and str5data['mainConsSts'] == '1':
                print(f"Main ON string{str1} success")
                CheckBatSts()
            else:
                MainON()

        def PreON():
            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                time.sleep(2)
                PreON()
            bat_client_socket.send(preON_mapping[str1])
            bat_client_socket.send(preON_mapping[str2])
            bat_client_socket.send(preON_mapping[str3])
            bat_client_socket.send(preON_mapping[str4])
            bat_client_socket.send(preON_mapping[str5])
            time.sleep(2)

            url1 = str_reply[str1]
            str1res = requests.get(url1)
            str1data = str1res.json()

            url2 = str_reply[str2]
            str2res = requests.get(url2)
            str2data = str2res.json()

            url3 = str_reply[str3]
            str3res = requests.get(url3)
            str3data = str3res.json()

            url4 = str_reply[str4]
            str4res = requests.get(url4)
            str4data = str4res.json()

            url5 = str_reply[str5]
            str5res = requests.get(url5)
            str5data = str5res.json()

            if str1data['preConSts'] == '1' and str2data['preConSts'] == '1' and str3data['preConSts'] == '1' and str4data['preConSts'] == '1' and str5data['preConSts'] == '1':
                print(f"Pre ON string{str1} success")
                MainON()
            else:
                PreON()

        if str1 in preON_mapping and str2 in preON_mapping and str3 in preON_mapping and str4 in preON_mapping and str4 in preON_mapping:
            PreON()

        response = {'message': f'Process started : {value}'}
        return response
    else:
        response = {'message': 'No string(s) provided in the request'}

        return response


@app.route('/ioeprocessoff', methods = ['GET'])
def ioeOff():
    def MainOff():
        bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            bat_client_socket.connect((bat_server_ip, bat_server_port))
        except Exception as ex:
            print(ex,'to battery')
            time.sleep(2)
            MainOff()
        bat_client_socket.send(mainOFFs1)
        bat_client_socket.send(mainOFFs2)
        bat_client_socket.send(mainOFFs3)
        bat_client_socket.send(mainOFFs4)
        bat_client_socket.send(mainOFFs5)
        print("Main OFF sent")

    def ConvStop():
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')
            time.sleep(2)
            ConvStop()
        conv_client_socket.send(stopConv)
        print("Convertor Stoped")
        time.sleep(1)
        client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
        client_conv.connect()
        read=client_conv.read_holding_registers(address = 1159)
        StopVal = read.registers[0]
        client_conv.close()
        if StopVal == 0:
            print("Convertor Stopped")
            print("Process OFF")
            conv_client_socket.close()
            time.sleep(500)
            MainOff()
        else:
            conv_client_socket.close()
            ConvStop()

    def NetCloseOff():
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')
            time.sleep(2)
            NetCloseOff()
        conv_client_socket.send(netCloseOFF)
        print("Net Close OFF")
        time.sleep(1)
        client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
        client_conv.connect()
        read=client_conv.read_holding_registers(address = 1603)
        netsyncval = read.registers[0]
        client_conv.close()
        if netsyncval == 15:
            conv_client_socket.close()
            ConvStop()
        else:
            conv_client_socket.close()
            NetCloseOff()

    def NetSyncOff():
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')
            time.sleep(2)
            NetSyncOff()
        conv_client_socket.send(netSyncOFF)
        print("Net sync off")
        time.sleep(1)
        client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
        client_conv.connect()
        read=client_conv.read_holding_registers(address = 1601)
        netsyncval = read.registers[0]
        client_conv.close()
        if netsyncval == 14:
            conv_client_socket.close()
            print("Net Sync OFF")
            NetCloseOff()
        else:
            conv_client_socket.close()
            NetSyncOff()

    def convProsOFF():
        conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            conv_client_socket.connect((conv_server_ip, conv_server_port))
        except Exception as ex:        
            print(ex,'to convertor')
            time.sleep(2)
            convProsOFF()
        cur_mode = bytes.fromhex("5C6500000009011005FC0001020000")
        conv_client_socket.send(cur_mode)
        print("Current SET to 0")
        time.sleep(1)
        client_conv = ModbusTcpClient("10.9.242.6", port=502, timeout=3)
        client_conv.connect()
        read=client_conv.read_holding_registers(address = 1532)
        current = read.registers[0]
        client_conv.close()
        if current == 0:
            conv_client_socket.close()
            print("Current Set to 0")
            NetSyncOff()
        else:
            conv_client_socket.close()
            convProsOFF()
    
    convProsOFF()

    response = {'message': 'IOE OFF'}
    return response

if __name__ == '__main__':
    app.run(host="localhost",port=8009)