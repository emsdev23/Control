import mysql.connector
from redmail import EmailSender
from datetime import date,timedelta,datetime
import time
import pdfkit

head="""
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Report</title>
        <style>
            body {
                margin: 0;
                font-family: Sans-Serif;
            }
            .energycontainer{
                margin-left: 1%;
                width: 95%;
                border-radius: 5px;
                background-color: #F7F7F7;
                border: 5px solid white;
            }
            .buildcontainer{
                margin-left: 1%;
                width: 95%;
                border-radius: 5px;
                background-color: #F7F7F7;
                border: 5px solid white;
            }
            .container {
                width: 96.5%;
                height: 14%;
                background-color: white;
                border: 1px solid white;
                padding: 1%;
                box-sizing: border-box;
                position: relative;
                border-radius: 5px;
                margin-left: 1%;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .container1 {
                margin-top:0%;
                width: 95%;
                height: 14%;
                background-color: #F7F7F7;
                padding: 1%;
                box-sizing: border-box;
                position: relative;
                border-radius: 5px;
                justify-content: space-between;
                align-items: center;
            }
            .evcontainer {
                width: 100%;
                height: 14%;
                background-color: white;
                border: 1px solid white;
                box-sizing: border-box;
                border-radius: 5px;
                margin-left: 1%;
                position: relative;
                display: flex;
                align-items: center;
            }
            .content {
                background-color: #3d403d;
                color: white;
                padding: 0.35%;
                border-radius: 5px;
                width: 25%;
                text-align: center;
            }
            .content1 {
                margin-left: 1%;
                background-color: #947F9B;
                color: white;
                height : 50%;
                border-radius: 5px;
                width: 22%;
                text-align: center;
            }
            h3 {
                margin: 0;
                font-size: 25px;
            }
            .bar-container {
                width: 95%;
                height: 25px;
                margin-left: 1%;
                background-color: #F7F7F7;
                overflow: hidden;
                position: relative;
                display: flex;
                align-items: flex-start;
                justify-content: center;
            }
            .bar {
                height: 100%;
                transition: width 0.5s ease-in-out;
                float: left;
            }

            .bar.clients {
                background-color: #FF9F0E;
            }

            .bar.chillers {
                background-color: #FFB443;
            }

            .bar.utilities {
                background-color: #FFCF87;
            }
            .bar.others{
                background-color: #FFE7C4;
            }
            .bar.grid{
                background-color: #1550eb;
            }
            .bar.wheel{
                background-color: #406ee6;
            }
            .bar.wheel2{
                background-color: #597fe3;
            }
            .bar.wind{
                background-color: #6d8cde;
            }
            .bar.roof{
                background-color: #829be0;
            }
            .bar.diesel{
                background-color: #b0a9a5;
            }
        </style>
    </head>
"""

while True:

    curdate = datetime.now()
    mint = str(curdate)[11:16]
    print(mint)

    if 1==1:

    # if mint == "10:00":

        try:
            emsdb = mysql.connector.connect(
                host="121.242.232.211",
                user="emsroot",
                password="22@teneT",
                database='EMS',
                port=3306
            )

            emscur = emsdb.cursor()

            awsdb = mysql.connector.connect(
                    host="3.111.70.53",
                    user="emsroot",
                    password="22@teneT",
                    database='EMS',
                    port=3307
                )

            awscur = awsdb.cursor()
        
        except Exception as ex:
            print(ex)
            continue

        current_date = date.today()
                    
        yesterday = current_date - timedelta(days = 1)
                    
        formatted_date = yesterday.strftime("%d %B %Y")

        datelist = formatted_date.split(' ')
        # print(datelist)
        if int(formatted_date[0]) == 0:
            yesdate = datelist[0][1:] + " " + datelist[1] + " " + datelist[2]
        #     dateyes = datelist[1]+','+' '+datelist[0][1:]+' '+datelist[2]
        else:
            yesdate = datelist[0] + " " + datelist[1] + " " + datelist[2]



        awscur.execute("SELECT totalApparentPower2,polledTime FROM bmsunprocessed_prodv13.hvacSchneider7230Polling WHERE DATE(polledTime) = DATE_SUB(CURDATE(), INTERVAL 1 DAY) ORDER BY totalApparentPower2 DESC LIMIT 1;")

        peak_res = awscur.fetchall()

        peakDemand = round(peak_res[0][0])
        peakTime = str(peak_res[0][1])[11:16]


        awscur.execute("select Chillers,Client,Utilities,Others from EMS.electricDaywiseUsage where polledDate = date_sub(curdate(), interval 1 day);")

        conres = awscur.fetchall()

        awscur.execute("SELECT sum(Energy) FROM EMS.Comareahourlysum where date(polledtime) = date_sub(curdate(),interval 1 day);")

        utires = awscur.fetchall()

        if conres[0][0] != None:
            Chillers = round(conres[0][0])
        else:
            Chillers = 0

        if conres[0][1] != None:
            Client = round(conres[0][1])
        else:
            Client = 0

        if utires[0][0] != None:
            Utilities = round(utires[0][0])
        else:
            Utilities = 0

        if conres[0][3] != None:
            Others = round(conres[0][3])
        else:
            Others = 0


        overall = Chillers+Client+Utilities+Others

        Chillerspr = round((Chillers/overall)*100,1)
        Clientpr = round((Client/overall)*100,1)
        Utilitiespr = round((Utilities/overall)*100,1)
        Otherspr = round((Others/overall)*100,1)

        if Utilitiespr >= 0 and Utilitiespr <= 10:
            UtilitiesperDiv = f""" <div class="bar utilities" style="width: 10%; background-color: #FFCF87;"> &nbsp; {Utilitiespr}%</div>"""
            UtilitiesNameDiv = f"""<div class="bar white" style="width: 10%;">Utilities</div>"""
            UtilitiesvalDiv = f"""<div class="bar white" style="width: 10%;"><b>{Utilities}</b></div>"""
        elif Utilitiespr > 10:
            UtilitiesperDiv = f""" <div class="bar utilities" style="width: {Utilitiespr}%; background-color: #FFCF87;"> &nbsp; {Utilitiespr}%</div>"""
            UtilitiesNameDiv = f"""<div class="bar white" style="width: {Utilitiespr}%;">Utilities</div>"""
            UtilitiesvalDiv = f"""<div class="bar white" style="width: {Utilitiespr}%;"><b>{Utilities}</b></div>"""

        if Chillerspr >= 0 and Chillerspr <= 10:
            ChillerperDiv = f"""<div class="bar white" style="width: 10%; background-color: #FFB443;"> &nbsp; {Chillerspr}%</div>"""
            ChillersNameDiv = f"""<div class="bar white" style="width: 10%;">Chillers</div>"""
            ChillersvalDiv = f"""<div class="bar white" style="width: 10%;"><b>{Chillers}</b></div>"""
        elif Chillerspr > 10:
            ChillerperDiv = f"""<div class="bar white" style="width: {Chillerspr}%; background-color: #FFB443;"> &nbsp; {Chillerspr}%</div>"""
            ChillersNameDiv = f"""<div class="bar white" style="width: {Chillerspr}%;">Chillers</div>"""
            ChillersvalDiv = f"""<div class="bar white" style="width: {Chillerspr}%;"><b>{Chillers}</b></div>"""

        if Clientpr >= 0 and Clientpr <= 10:
            ClientNameDiv = f"""<div class="bar white" style="width: 10%;">Clients</div>"""
            ClientperDiv = f"""<div class="bar clients" style="width: 10%; background-color: #FF9F0E;"> &nbsp; {Clientpr}%</div>"""
            ClientvalDiv = f"""<div class="bar white" style="width: 10%;"><b>{Client}</b></div>"""
        elif Clientpr > 10:
            ClientNameDiv = f"""<div class="bar white" style="width: {Clientpr}%;">Clients</div>"""
            ClientperDiv = f"""<div class="bar clients" style="width: {Clientpr}%; background-color: #FF9F0E;"> &nbsp; {Clientpr}%</div>"""
            ClientvalDiv = f"""<div class="bar white" style="width: {Clientpr}%;"><b>{Client}</b></div>"""

        if Otherspr >= 0 and Otherspr <= 10:
            OthersNameDiv = f"""<div class="bar othres" style="width: 10%;">Others</div>"""
            OthersvalDiv = f"""<div class="bar othres" style="width: 10%;"><b>{Others}</b></div>"""
            OtherperDiv = f"""<div class="bar others" style="width: 10%; background-color: #FFE7C4;"> &nbsp; {Otherspr}%</div>"""
        elif Otherspr > 10:
            OthersNameDiv = f"""<div class="bar othres" style="width: {Otherspr}%;">Others</div>"""
            OthersvalDiv = f"""<div class="bar othres" style="width: {Otherspr}%;"><b>{Others}</b></div>"""
            OtherperDiv = f"""<div class="bar others" style="width: {Otherspr}%; background-color: #FFE7C4;"> &nbsp; {Otherspr}%</div>"""

        # print(Chillerspr)
        # print(Clientpr)
        # print(Utilitiespr)
        # print(Otherspr)



        awscur.execute("SELECT round(sum(bmsgrid)),round(sum(rooftopEnergy)),round(sum(wheeledinEnergy)),round(sum(diesel)),round(sum(wheeledinEnergy2)),round(sum(windEnergy)) FROM EMS.buildingConsumption where date(polledTime) = date_sub(curdate(), interval 1 day);")

        total_res = awscur.fetchall()

        if total_res[0][0] != None:
            grid = round(total_res[0][0])
        else:
            grid = 0

        if total_res[0][1] != None:
            roof = round(total_res[0][1])
        else:
            roof = 0

        if total_res[0][2] != None:
            wheel = round(total_res[0][2])
        else:
            wheel = 0

        total_grid = grid - wheel

        if total_res[0][3] != None:
            diesel = round(total_res[0][3])
        else:
            diesel = 0 
        
        if total_res[0][4] != None:
            wheel2 = round(total_res[0][4])
        else:
            wheel2 = 0 
        
        if total_res[0][5] != None:
            wind = round(total_res[0][5])
        else:
            wind = 0

        total_grid = grid-wheel-wheel2-wind

        if total_grid < 0:
            total_grid = 0 

        print(total_grid)
        print(roof)
        print(wheel)
        print(wheel2)
        print(wind)

        total = total_grid+roof+wheel+diesel+wheel2+wind

        gridpr = round((total_grid/total)*100,2)
        roofpr = round((roof/total)*100,2)
        wheelpr = round((wheel/total)*100,2)
        dieselpr = round((diesel/total)*100,2)
        wheel2pr = round((wheel2/total)*100,2)
        windpr = round((wind/total)*100,2)

        if dieselpr > 10:
            dieselNameVal = f"""<div class="bar white" style="width: {dieselpr}%;">Diesel</div>"""
            dieselDiv =f"""<div class="bar diesel" style="width: {dieselpr}%; background-color: #b0a9a5; color:white;"> &nbsp;{dieselpr}%</div>"""
            dieselValDiv = f"""<div class="bar othres" style="width: {dieselpr}%;"><b>{diesel}</b></div>"""
        elif dieselpr > 0 and dieselpr < 10:
            dieselNameVal = f"""<div class="bar white" style="width: 10%;">Diesel</div>"""
            dieselDiv =f"""<div class="bar diesel" style="width: 10%; background-color: #b0a9a5; color:white;"> &nbsp;{dieselpr}%</div>"""
            dieselValDiv = f"""<div class="bar othres" style="width: 10%;"><b>{diesel}</b></div>"""
        else:
            dieselNameVal = f"""<div class="bar white" style="width: 10%;">Diesel</div>"""
            dieselDiv =f"""<div class="bar diesel" style="width: 10%; background-color: #F7F7F7;">{dieselpr}%</div>"""
            dieselValDiv = f"""<div class="bar othres" style="width: 10%;"><b>{diesel}</b></div>"""

        if roofpr > 10:
            roofNameVal = f"""<div class="bar white" style="width: {roofpr}%;">Rooftop</div>"""
            roofDiv = f"""<div class="bar roof" style="width: {roofpr}%; background-color: #7b94d4; color:white;"><b> &nbsp;{roofpr}%</b></div>"""
            roofValDiv =f"""<div class="bar white" style="width: {roofpr}%; "><b>{roof}</b></div>"""
        elif roofpr > 0 and roofpr < 10:
            roofNameVal = f"""<div class="bar white" style="width: 10%;">Rooftop</div>"""
            roofDiv = f"""<div class="bar roof" style="width: 10%; background-color: #7b94d4; color:white;"><b> &nbsp;{roofpr}%</b></div>"""
            roofValDiv =f"""<div class="bar white" style="width: 10%;"><b>{roof}</b></div>"""
        else:
            roofNameVal = f"""<div class="bar white" style="width: 10%;">Rooftop</div>"""
            roofDiv =f"""<div class="bar white" style="width: 10%; background-color: #F7F7F7;"><b> &nbsp;{roofpr}%</b></div>"""
            roofValDiv = f"""<div class="bar white" style="width: 10%;"><b>{roof}</b></div>"""

        if windpr > 10:
            windNameVal = f"""<div class="bar white" style="width: {windpr}%;">Wind</div>"""
            windDiv =f"""<div class="bar wind" style="width: {windpr}%; background-color: #6383d4; color:white;"> &nbsp;{windpr}%</div>"""
            windValDiv = f"""<div class="bar othres" style="width: {windpr}%;"><b>{wind}</b></div>"""
        elif windpr > 0 and windpr < 10:
            windNameVal = f"""<div class="bar white" style="width: 10%;">Wind</div>"""
            windDiv =f"""<div class="bar wind" style="width: 10%; background-color: #6383d4; color:white;"> &nbsp;{windpr}%</div>"""
            windValDiv = f"""<div class="bar othres" style="width: 10%;"><b>{wind}</b></div>"""
        else:
            windNameVal = f"""<div class="bar white" style="width: 10%;">Wind</div>"""
            windDiv =f"""<div class="bar wind" style="width: 10%; background-color: #F7F7F7;">{windpr}%</div>"""
            windValDiv = f"""<div class="bar othres" style="width: 10%;"><b>{wind}</b></div>"""

        if wheel2pr >= 25:
            wheel2NameVal = f"""<div class="bar white" style="width: {wheel2pr}%;">Solar (Trackers)</div>"""
            wheel2Div =f"""<div class="bar wheel2" style="width: {wheel2pr}%; background-color: #4f76db; color:white;"> &nbsp;{wheel2pr}%</div>"""
            wheel2ValDiv = f"""<div class="bar othres" style="width: {wheel2pr}%;"><b>{wheel2}</b></div>"""
        elif wheel2pr > 0 and wheel2pr < 25:
            wheel2NameVal = f"""<div class="bar white" style="width: 30%;">Solar (Trackers)</div>"""
            wheel2Div =f"""<div class="bar wheel2" style="width: 30%; background-color: #4f76db; color:white;"> &nbsp;{wheel2pr}%</div>"""
            wheel2ValDiv = f"""<div class="bar othres" style="width: 30%;"><b>{wheel2}</b></div>"""
        else:
            wheel2NameVal = f"""<div class="bar white" style="width: 10%;">Solar (Trackers)</div>"""
            wheel2Div =f"""<div class="bar wheel2" style="width: 10%; background-color: #F7F7F7;">{wheel2pr}%</div>"""
            wheel2ValDiv = f"""<div class="bar othres" style="width: 10%;"><b>{wheel2}</b></div>"""

        if wheelpr >= 25:
            wheelNameVal = f"""<div class="bar white" style="width: {wheelpr}%;">Solar</div>"""
            wheelDiv = f"""<div class="bar wheel" style="width: {wheelpr}%; background-color: #3a68e0;  color:white;"> &nbsp;{wheelpr}%</div>"""
            wheelValDiv = f"""<div class="bar white" style="width: {wheelpr}%;"><b>{wheel}</b></div>"""
        elif wheelpr > 0 and wheelpr < 25:
            wheelNameVal = f"""<div class="bar white" style="width: 30%;">Solar</div>"""
            wheelDiv = f"""<div class="bar wheel" style="width: 30%; background-color: #3a68e0; color:white;"> &nbsp;{wheelpr}%</div>"""
            wheelValDiv = f"""<div class="bar white" style="width: 30%;"><b>{wheel}</b></div>"""
        else:
            wheelNameVal = f"""<div class="bar white" style="width: 10%;">Solar</div>"""
            wheelDiv = f"""<div class="bar wheel" style="width: 10%; background-color: #F7F7F7;"> &nbsp;{wheelpr}%</div>"""
            wheelValDiv = f"""<div class="bar white" style="width: 10%;"><b>{wheel}</b></div>"""

        if gridpr > 10:
            gridValName = f"""<div class="bar white" style="width: {gridpr}%;">Grid</div>"""
            gridDiv = f"""<div class="bar grid" style="width: {gridpr}%; background-color: #2E61E6; color:white;"><span> &nbsp;{gridpr}%</span></div>"""
            gridValDiv = f"""<div class="bar white" style="width: {gridpr}%;"><b>{total_grid}</b></div>"""
        elif gridpr > 0 and gridpr < 10:
            gridValName = f"""<div class="bar white" style="width: 10%;">Grid</div>"""
            gridDiv = f"""<div class="bar grid" style="width: 10%; background-color: #2E61E6; color:white;"><span> {gridpr}%</span></div>"""
            gridValDiv = f"""<div class="bar white" style="width: 10%;"><b>{total_grid}</b></div>"""
        else:
            gridValName = f"""<div class="bar white" style="width: 10%;">Grid</div>"""
            gridDiv = f"""<div class="bar white" style="width: 10%; background-color: #F7F7F7;"><span> {gridpr}%</span></div>"""
            gridValDiv =f"""<div class="bar white" style="width: 10%;"><b>{total_grid}</b></div>"""


        awscur.execute("SELECT sum(totalsessions),round(sum(energyconsumption)) FROM EMS.evcharger where date(servertime) = date_sub(curdate(), interval 1 day);")

        evres = awscur.fetchall()

        if evres[0][0] != None:
            total_EvSessions = evres[0][0]
        else:
            total_EvSessions = 0

        if evres[0][1] != None:
            total_EvEnergy = evres[0][1]
        else:
            total_EvEnergy = 0

        awscur.execute("""SELECT sum(thermalDischarge) FROM EMS.buildingConsumption 
        where polledTime >= CURDATE() - INTERVAL 1 DAY + INTERVAL 6 HOUR 
        and polledTime <= CURDATE() - INTERVAL 1 DAY + INTERVAL 10 HOUR 
        or polledTime >= CURDATE() - INTERVAL 1 DAY + INTERVAL 18 HOUR
        and polledTime <= CURDATE() - INTERVAL 1 DAY + INTERVAL 22 HOUR;""")

        thermal_res = awscur.fetchall()

        if thermal_res[0][0] != None:
            thermalEnergy = round(thermal_res[0][0])
        else:
            thermalEnergy = 0

        print("Thermal tarrif",thermalEnergy)
        
        if thermalEnergy > 0:
            ThermalTarrif = f"""<tr>
                    <td> <br><br>&#128350 06:00-10:00 & 18:00-22:00</td>
                    <td><b>System Used</b> <br><br> Thermal Storage</td>
                    <td><b>Savings</b> <br><br>Discharge Energy {thermalEnergy} ckWh</td>
                </tr>"""
        else:
            ThermalTarrif = None

        awscur.execute("""SELECT sum(st1dischargingEnergy),sum(st2dischargingEnergy),sum(st3dischargingEnergy), 
                  sum(st4dischargingEnergy),sum(st5dischargingEnergy) FROM EMS.IOEbatteryHourly 
                  where polledTime >= CURDATE() - INTERVAL 1 DAY + INTERVAL 6 HOUR
                  and polledTime <= CURDATE() - INTERVAL 1 DAY + INTERVAL 10 HOUR 
                  or polledTime >= CURDATE() - INTERVAL 1 DAY + INTERVAL 18 HOUR
                  and polledTime <= CURDATE() - INTERVAL 1 DAY + INTERVAL 22 HOUR;""")
    
        res = awscur.fetchall()

        if res[0][0] != None:
            st1 = res[0][0]
        else:
            st1 = 0
        if res[0][1] != None:
            st2 = res[0][1]
        else:
            st2 = 0
        if res[0][2] != None:
            st3 = res[0][2]
        else:
            st3 = 0
        if res[0][3] != None:
            st4 = res[0][3]
        else:
            st4 = 0
        if res[0][4] != None:
            st5 = res[0][4]
        else:
            st5 = 0 

        ioeEnergy = st1+st2+st3+st4+st5
        print("IOE tarrif hours", ioeEnergy)

        if ioeEnergy > 0:
            ioeTarrif = ThermalTarrif = f"""<tr>
                    <td> <br><br>&#xf017; 06:00-10:00 & 18:00-22:00</td>
                    <td><b>System Used</b> <br><br> IOE battery Storage</td>
                    <td><b>Savings</b> <br><br>Discharge Energy {ioeEnergy} kWh</td>
                </tr>"""
        else:
            ioeTarrif = None

        Tarrif = None

        if ThermalTarrif:
            Tarrif = ThermalTarrif
        
        if ioeTarrif and ThermalTarrif:
            Tarrif = Tarrif+ioeTarrif
        elif ioeTarrif:
            Tarrif = ioeTarrif
        
        if Tarrif == None:
            Tarrif = """<tr>
                        <td> &nbsp; No Discharge</td>
                    </tr>"""


        # if thermal_res[0][1] != None:
        #     thermalCost = thermal_res[0][1]
        # else:
        #     thermalCost = 0

        powerFailureDiv = ""

        if diesel > 0:
            awscur.execute("SELECT dg,polledTime FROM EMS.MVPvsDG where date(polledTime) = date_sub(curdate(),interval 1 day);")

            dgres = awscur.fetchall()

            stTime = ""
            edTime = ""

            print('DG',len(dgres))

            for i in range(3,len(dgres)-3):
                if dgres[i][0] != None and dgres[i-1][0] != None and dgres[i-2][0] != None and dgres[i-3][0] != None and dgres[i+1][0] != None and dgres[i+2][0] != None and dgres[i+3][0] != None:
                    if dgres[i][0] > 0: 
                        if dgres[i-1][0] == 0 and dgres[i-2][0] == 0 and dgres[i-3][0] == 0:
                            stTime = str(dgres[i][1])
                    
                    if dgres[i][0] > 0:
                        if dgres[i+1][0] == 0 and dgres[i+2][0] == 0 and dgres[i+3][0] == 0:
                            edTime = str(dgres[i][1])
                    
                if stTime and edTime:
                    print(stTime, edTime)
                    awscur.execute(f"SELECT sum(dg) FROM EMS.MVPvsDG where polledTime >= '{stTime}' and polledTime <= '{edTime}';")
                    Enres = awscur.fetchall()
                    powerFailureDiv = powerFailureDiv + f"""
                                    <tr>
                                    <td>&#128350 {stTime[11:16]}-{edTime[11:16]}</td>
                                    <td><b>System Used</b> <br><br> Diesel</td>
                                    <td><br><br>Energy {round(Enres[0][0])} kW</td>
                                    </tr>"""
                    stTime = ""
                    edTime = ""

        else:
            powerFailureDiv = f"""
                            <tr>
                            <td>No power failure</td>
                            </tr>"""
            

        emscur.execute("SELECT dischargeON,dischargeOFF,Energy FROM EMS.ltoLogDchg where recordDate = date_sub(curdate(), interval 1 day) and cause = 'peakdemand';")

        enerres = emscur.fetchall()

        peakDischarge = ""

        ltopeakdischarge = None

        if len(enerres) > 0:
            for i in enerres:
                disOn = str(i[0])[11:16]
                disOff = str(i[1])[11:16]
                ltopeakdischarge = f"""
                <tr>                
                <td> <br><br>&#128350 {disOn}-{disOff}</td>
                <td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>System Used</b> <br><br>&nbsp;&nbsp;&nbsp;&nbsp; LTO Battery</td>
                <td><b>Savings</b> <br><br>Discharge Energy {abs(i[2])} kWh</td>
                </tr>"""
        
        emscur.execute("select dischargeON,dischargeOFF,Energy from EMS.ioePeakDchg where date(dischargeON) = date_sub(curdate(), interval 1 day);")

        ioeeneRes = emscur.fetchall()

        ioepeakdischarge = None

        if len(ioeeneRes) > 0:
            for i in ioeeneRes:
                disOn = str(i[0])[11:16]
                disOff = str(i[1])[11:16]
                ioepeakdischarge = f"""
                <tr>                
                <td> <br><br>&#128350 {disOn} - {disOff}</td>
                <td>&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;<b>System Used</b> <br><br>&nbsp;&nbsp;&nbsp;&nbsp; IOE Battery</td>
                <td><b>Savings</b> <br><br>Discharge Energy {abs(i[2])} kWh</td>
                </tr>"""

        if ltopeakdischarge:
            peakDischarge = peakDischarge+ltopeakdischarge
        
        if ioepeakdischarge:
            peakDischarge = peakDischarge+ioepeakdischarge
        
        if len(peakDischarge) == 0:
            peakDischarge = """
                <tr>
                <td>No Peak Discharge</td>
                </tr>"""


        email = EmailSender(host="smtp.gmail.com", port=587, username='emsteamrp@gmail.com', password='eebpnvgyfzzdtitb')

        body = f"""
        <body>
            <center><h1>Daily Executive Report</h1></center>
            <div class="container">
                <div>
                    <h3>IIT Madras Research Park</h3>
                    <p>Chennai, India {yesdate}</p>
                </div>
                <div class="content">
                    <p><b>Peak Demand</b></p>
                    <p><b> <span style="font-size:17px">{peakDemand} kVA </span></b> &nbsp; &#128350 {peakTime} </p>
                </div>
            </div>

            <div class="energycontainer">
            <div class="container1">
                <p style="font-size: 18px;"><b>Total Energy Consumed - {total}</b>  &nbsp;<span style="color:#b0a9a5">Energy in kWh</span></p>
            <div class="bar-container">
                {gridValName}
                <div class="bar white" style="width: 5%;"></div>

                {wheelNameVal}
                <div class="bar white" style="width: 5%;"></div>

                {wheel2NameVal}
                <div class="bar white" style="width: 5%;"></div>

                {windNameVal}
                <div class="bar white" style="width: 5%;"></div>

                {roofNameVal}
                <div class="bar white" style="width: 5%;"></div>

                {dieselNameVal}
            </div>
            
            <div>
            </div>

            <div class="bar-container">
                {gridDiv}
                <div class="bar white" style="width: 5%;"></div>

                {wheelDiv}
                <div class="bar white" style="width: 5%;"></div>

                {wheel2Div}
                <div class="bar white" style="width: 5%;"></div>

                {windDiv}
                <div class="bar white" style="width: 5%;"></div>

                {roofDiv}
                <div class="bar white" style="width: 5%;"></div>

                {dieselDiv}
            </div>

            <div class="bar-container">
                {gridValDiv}
                <div class="bar white" style="width: 5%;"></div>

                {wheelValDiv}
                <div class="bar white" style="width: 5%;"></div>

                {wheel2ValDiv}
                <div class="bar white" style="width: 5%;"></div>

                {windValDiv}
                <div class="bar white" style="width: 5%;"></div>

                {roofValDiv}
                <div class="bar white" style="width: 5%;"></div>

                {dieselValDiv}   
            </div>
            </div>
            </div>


            <div class="buildcontainer">
                <div class="container1">
                <p style="font-size: 18px;"><b>Building Consumption</b> &nbsp;<span style="color:#b0a9a5">Energy in kWh</span></p>
                <div class="bar-container">
                    {ClientNameDiv}
                    {ChillersNameDiv}
                    {UtilitiesNameDiv}
                    {OthersNameDiv} 
                </div>
                <div class="bar-container">
                    {ClientperDiv}
                    {ChillerperDiv}
                    {UtilitiesperDiv}
                    {OtherperDiv}
                </div>
                <div class="bar-container">
                    {ClientvalDiv}
                    {ChillersvalDiv}
                    {UtilitiesvalDiv}
                    {OthersvalDiv} 
                </div>
                </div>
            </div>

            <p style="margin-left: 2%;"><b>EV Charges</b></p>
            <div class="evcontainer">
                <div class="content1">
                    <p style ="color:black;">Total Energy Consumed</p>
                    <p style ="color:black;"><b>{total_EvEnergy} kWh</b></p>
                </div>
                &nbsp; &nbsp; &nbsp; &nbsp;
                <div class="content1">
                    <p style ="color:black;">Total Sessions(s)</p>
                    <p style ="color:black;"><b>{total_EvSessions}</b></p>
                </div>
            </div>

            <br>

            <div class="container4">
            <table style="width:96%; margin-left: 2%;">
                <tr>
                <td bgcolor="#8D9EA7"; style ="color:white;">&nbsp;<b>Peak Tarrif Hours</b></td>
                </tr>
            </table>
            <table style="width:96%; margin-left: 2%;">
                {Tarrif}
            </table>
            <br>
            <br>

            <table style="width:96%; margin-left: 2%;">
                <tr>
                <td bgcolor="#8D9EA7"; style ="color:white;">&nbsp;<b>Peak Demand</b></td>
                </tr>
            </table>
            <table style="width:96%; margin-left: 2%;">
            {peakDischarge}
            </table>
            <br>
            <table style="width:96%; margin-left: 2%;">
                            <tr>
                            <td bgcolor="#8D9EA7"; style ="color:white;">&nbsp;<b>Power Failure</b></td>
                            </tr>
                        </table>
                        <table style="width:96%; margin-left: 2%;">
                        {powerFailureDiv}
                        </table>
            </div>
        </body>
        """

        html = head+body

        dateName = str(datetime.now())[0:11]
        
        pdf_path = f'./ExecutiveRep-{dateName}.pdf'

        pdfkit.from_string(html, pdf_path)
        
        print("HTML to PDF done...")

    time.sleep(60)
