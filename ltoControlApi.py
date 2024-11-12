from flask import Flask, jsonify, request
import logging
import time
import socket
from datetime import datetime
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

app = Flask(__name__)

# Flags to track busy state of APIs
charge_off_busy = False
charge_on_busy = False
discharge_off_busy = False
discharge_on_busy = False

ip_bat = "10.9.244.1"
ip_conv = "10.9.244.45"

hexON = "11870000000902100011000102001B"
bytON= bytes.fromhex(hexON)

resetConv = "1187000000090210000F0001020017"
resetON = bytes.fromhex(resetConv)

hexOFF = "11870000000902100011000102001C"
bytOFF = bytes.fromhex(hexOFF)

preOFF = bytes.fromhex("851803FF1202FFFFFFFF000000")
preON = bytes.fromhex("851803FF1201FFFFFFFF000000")

mainOFF = bytes.fromhex("851803FF12FF01FFFFFF000000")


logging.basicConfig(
    filename='ltoContactor.log',  # Specify the log file name
    level=logging.DEBUG,     # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)

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


def check_authentication(token):
    # Replace this with your own logic to validate the token
    valid_token = "VKOnNhH2SebMU6S"
    return token == valid_token

success = None

# Middleware function to check if the API is busy
@app.before_request
def check_all_processes_idle():
    global charge_off_busy, discharge_off_busy, charge_on_busy, discharge_on_busy
    if charge_off_busy or charge_on_busy or discharge_off_busy or discharge_on_busy:
        return jsonify({'error': 'One or more processes are busy, try again later'}), 409



# ----------------------------------- Discharge ON API ----------------------------


@app.route('/ltoDischargeon', methods = ['GET'])
def ltoDischargeON():
    global discharge_on_busy
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        discharge_on_busy= True
        try:
            curnttime = datetime.now()

            crate = request.args.get('crate')

            hex_string = crate[-4:]

            current = int(hex_string, 16)

            if current & (1 << (len(hex_string) * 4 - 1)):
                current -= 1 << (len(hex_string) * 4)


            logger.info(curnttime)
            bat_server_ip = ip_bat  # Replace with your server's IP address
            bat_server_port = 15153 

            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                return None   

            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                return None

            battData = batData()

            batVoltage = battData[0]

            if batVoltage != None and batVoltage != 'exception':
                batVoltage = int(batVoltage)
                bat_hex = hex(batVoltage)[2:]
                if len(bat_hex) == 3:
                    bat_hex = '0'+bat_hex.upper()
            

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
                    if (convCurrent-1) >= current:
                        print("Current set on Convertor Set Success")
                        print("Disharge started")
                        logger.info("Current set on Convertor Set Success")
                        logger.info("Disharge started")
                        conv_client_socket.close()
                        bat_client_socket.close()
                        global success
                        success = "success" 
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
                bat_server_ip =ip_bat  # Replace with your server's IP address
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

            DischargeON(crate)

            if success == "success":
                batVoltage = batData()[0]
                return {"message":"LTO Discharge ON"}
        finally:
            discharge_on_busy = False
    
    else:
        return {'error': 'Unauthorized'}, 401
    

# ----------------------------------- Charge ON API ----------------------------


@app.route('/ltoChargeon', methods = ['GET'])
def ltoChargeON():
    global charge_on_busy
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        charge_on_busy = True
        try:
            curnttime = datetime.now()

            logger.info(curnttime)
            bat_server_ip = ip_bat  # Replace with your server's IP address
            bat_server_port = 15153 

            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                return None   

            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                return None
            
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

            if success == "success":
                return {"message":"LTO Charge ON success"}     
        finally:
            charge_on_busy = False
    else:
        return {'error': 'Unauthorized'}, 401


# ----------------------------------- Discharge OFF API ----------------------------


@app.route('/ltoDischargeoff', methods = ['GET'])
def ltoDischargeOFF():
    global discharge_off_busy
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        discharge_off_busy = True
        
        try:
            curnttime = datetime.now()

            logger.info(curnttime)
            bat_server_ip = ip_bat # Replace with your server's IP address
            bat_server_port = 15153 

            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')   
                return None

            conv_server_ip = ip_conv  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                return None

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
                bat_server_ip = ip_bat  # Replace with your server's IP address
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
                        logger.info("Discharge OFF completed")
                        conv_client_socket.close()
                        bat_client_socket.close()
                        global success
                        success = "success"
                    else:
                        checkBatStsDC()
                except Exception as ex:
                    print(ex)
                    checkBatStsDC()
                
            def DischargeOFF():
                SetOFFCurDC()

            DischargeOFF()

            if success == "success":
                batVoltage = batData()[0]
                return {"message":"LTO Discharge OFF success"} 
        finally:
            discharge_off_busy = False

    else:
        return {'error': 'Unauthorized'}, 401



# ----------------------------------- Charge OFF API ----------------------------


@app.route('/ltoChargeoff', methods = ['GET'])
def ltoChargeOFF():
    global charge_off_busy
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        charge_off_busy = True
        try:
            curnttime = datetime.now()

            logger.info(curnttime)
            bat_server_ip = ip_bat  # Replace with your server's IP address
            bat_server_port = 15153 

            bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                bat_client_socket.connect((bat_server_ip, bat_server_port))
            except Exception as ex:
                print(ex,'to battery')
                return None   

            conv_server_ip = ip_conv # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                return None

            battData = batData()

            batVoltage = battData[0]

            if batVoltage != None and batVoltage != 'exception':
                batVoltage = int(batVoltage)
                bat_hex = hex(batVoltage)[2:]
                if len(bat_hex) == 3:
                    bat_hex = '0'+bat_hex.upper()


            def setMainOFF():
                print("Main OFF called")
                logger.info("Main OFF called")
                bat_server_ip = ip_bat  # Replace with your server's IP address
                bat_server_port = 15153 

                bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                try:
                    bat_client_socket.connect((bat_server_ip, bat_server_port))
                except Exception as ex:
                    print(ex,'to battery')
                    setMainOFF()
                mainOFF = bytes.fromhex("851803FF12FF01FFFFFF000000")
                bat_client_socket.send(mainOFF)
                BatData = batData()
                mainConsSts = BatData[1]
                batCur = BatData[4]
                print(mainConsSts,batCur)
                try:
                    if mainConsSts == '2':
                        print("Charge OFF Completed")
                        logger.info("Charge OFF Completed")
                        PreOFFchfOg()
                    else:
                        print("Main not OFF")
                        logger.info("Main not OFF")
                        setMainOFF()
                except Exception as ex:
                    print(ex)
                    setMainOFF()

            
            def SetOFFCur():
                print("current and voltage sending to conv")
                cur_mode = bytes.fromhex("1D5A000000090210000C0001020000")
                conv_client_socket.send(cur_mode)
                setConvVolt = "029A000000090210000D000102"+bat_hex
                setConvVolt = bytes.fromhex(setConvVolt)   
                conv_client_socket.send(setConvVolt)
                time.sleep(2) 
                convData = Convertor_data()
                try:
                    convVoltage = int(convData[0])
                except Exception:
                    SetOFFCur()
                convCurrent = convData[1]
                convOutCur = convData[4]
                batVoltage = batData()[0]
                print(convVoltage,batVoltage,convCurrent)
                # abs(convVoltage - batVoltage) <= 2
                try:
                    if abs(convVoltage - batVoltage) <= 10 and convCurrent == 0 and convOutCur < 2: #and convOutVolt > 300
                        print("Conv and Bat voltage equal and conv current 0 success")
                        logger.info(("Conv and Bat voltage equal and conv current 0 success"))
                        setMainOFF()
                    else:
                        print("Conv and Bat voltage not set")
                        logger.info(("Conv and Bat voltage not set"))
                        SetOFFCur()
                except Exception as ex:
                    print(ex)
                    SetOFFCur()

            def PreOFFchfOg():
                bat_server_ip = ip_bat  # Replace with your server's IP address
                bat_server_port = 15153 

                bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                try:
                    bat_client_socket.connect((bat_server_ip, bat_server_port))
                except Exception as ex:
                    print(ex,'to battery')
                    PreOFFchfOg()
                preOFF = bytes.fromhex("851803FF1202FFFFFFFF000000")
                bat_client_socket.send(preOFF)
                preConSts = batData()[2]
                try:
                    if preConSts == '2':
                        print("Pre OFF Success")
                        logger.info(("Pre OFF Success."))
                        time.sleep(2)
                        checkBatSts()
                    else:
                        PreOFFchfOg()
                except Exception as ex:
                    print(ex)
                    PreOFFchfOg()
            
            def checkBatSts():
                conv_client_socket.send(bytOFF)
                print("Byte OFF sent")
                logger.info(("Byte OFF sent"))
                time.sleep(3)
                try:
                    convByteON = int(Convertor_data()[2])
                except Exception:
                    checkBatSts()
                try:
                    if convByteON == 28:
                        print("Charge OFF completed")
                        logger.info(("Charge OFF completed"))
                        conv_client_socket.close()
                        bat_client_socket.close()
                        global success
                        success = "success"
                    else:
                        checkBatSts()
                except Exception as ex:
                    print(ex)
                    checkBatSts()
            
            def ChargeOFF():
                SetOFFCur()
            
            ChargeOFF()

            if success == "success":
                return {"message":"LTO Charge OFF Success"} 
        finally:
            charge_off_busy = False

    else:
        conv_client_socket.close()
        bat_client_socket.close()
        return {'error': 'Unauthorized'}, 401
    

@app.route('/ltofault', methods = ['GET'])
def ltoFault():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        curnttime = datetime.now()

        logger.info(curnttime)
        bat_server_ip = ip_bat  # Replace with your server's IP address
        bat_server_port = 15153 

        bat_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        try:
            bat_client_socket.connect((bat_server_ip, bat_server_port))
        except Exception as ex:
            print(ex,'to battery')
            return None   
        
        BatData = batData()
        mainConsSts = BatData[1]
        preConSts = BatData[2]
        batSts = BatData[3]
        print("Battery Status",batSts)
        print("Main",mainConsSts)
        print("pre",preConSts)    

        def setByteOFF():
            conv_server_ip = "10.9.220.43"  # Replace with your server's IP address
            conv_server_port = 443 
                
            conv_client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

            try:
                conv_client_socket.connect((conv_server_ip, conv_server_port))
            except Exception as ex:
                print(ex,'to convertor')
                return None
            
            conv_client_socket.send(bytOFF)
            print("Byte OFF sent")
            time.sleep(3)
            convByteON = int(Convertor_data()[2])
            try:
                if convByteON == 28:
                    print("Process OFF completed")
                    conv_client_socket.close()
                    bat_client_socket.close()
                    data = {"message":"Fault Convertor Switched OFF"}
                    return data , 200
                else:
                    setByteOFF()
            except Exception as ex:
                print(ex)
                setByteOFF()

        if mainConsSts == '3' or preConSts == '3' or batSts == 'FAULT':
            setByteOFF()
        else:
            bat_client_socket.close()
            data = {"message":"No Fault"}
            return data , 200
    else:
        return {'error': 'Unauthorized'}, 401

@app.route('/ltolimit', methods = ['GET'])
def ltoLimits():
    token = request.headers.get('Authorization')
    print(token)

    if token and check_authentication(token):
        curnttime = datetime.now()
        battData = batData()

        batVoltage = battData[0]
        batterySts = battData[3]
        print(batVoltage)
        data = {"message":(batVoltage,batterySts)}
        return data

    else:
        return {'message': 'Unauthorized'}, 401


if __name__ == '__main__':
    app.run(host="localhost",port=8007)