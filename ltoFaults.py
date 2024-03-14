import socket
import time
from datetime import datetime

hexOFF = "11870000000902100011000102001C"
bytOFF = bytes.fromhex(hexOFF)

preOFF = bytes.fromhex("851803FF1202FFFFFFFF000000")
preON = bytes.fromhex("851803FF1201FFFFFFFF000000")

mainOFF = bytes.fromhex("851803FF12FF01FFFFFF000000")

def batData():
    bat_server_ip = "10.9.220.42"  # Replace with your server's IP address
    bat_server_port = 15153 

    bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        bat_client_socket.connect((bat_server_ip, bat_server_port))
    except Exception as ex:
        print(ex)
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

    return batteryVolt,mainConsSts,preConSts,batterySts


def Convertor_data():
    conv_server_ip = "10.9.220.43"  # Replace with your server's IP address
    conv_server_port = 443 
        
    conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        conv_client_socket.connect((conv_server_ip, conv_server_port))
    except Exception as ex:
        print(ex)
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

while True:

    bat_server_ip = "10.9.220.42"  # Replace with your server's IP address
    bat_server_port = 15153 

    bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        bat_client_socket.connect((bat_server_ip, bat_server_port))
    except Exception as ex:
        print(ex,'to battery')
        continue

    conv_server_ip = "10.9.220.43"  # Replace with your server's IP address
    conv_server_port = 443 
        
    conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    try:
        conv_client_socket.connect((conv_server_ip, conv_server_port))
    except Exception as ex:
        print(ex,'to convertor')
        continue

    BatData = batData()
    mainConsSts = BatData[1]
    preConSts = BatData[2]
    batSts = BatData[3]
    print("Battery Status",batSts)
    print("Main",mainConsSts)
    print("pre",preConSts)    

    def setByteOFF():
        conv_client_socket.send(bytOFF)
        print("Byte OFF sent")
        time.sleep(3)
        convByteON = int(Convertor_data()[2])
        try:
            if convByteON == 28:
                print("Process OFF completed")
                time.sleep(45)
            else:
                setByteOFF()
        except Exception as ex:
            print(ex)
            setByteOFF()

    if mainConsSts == '3' or preConSts == '3' or batSts == 'FAULT':
        setByteOFF()
    
    time.sleep(3)

