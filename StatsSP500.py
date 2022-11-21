import lxml
import requests
from bs4 import BeautifulSoup
import re
from datetime import datetime
import mysql.connector
import os

# fetch statistics based on firmcode and paese into mysql data
def getFirmStats(result, firmCode, mydb, mycursor):
    ## check if firm exist:
    mycursor.execute("SELECT symbol FROM Firms")
    returnFlag = 1
    for firm in mycursor.fetchall():
        if str(firm[0]) == firmCode:
            returnFlag = 0
            firmCode = re.sub("\.", "-", firmCode)
    if returnFlag == 1:
        return "NoMatchFound"

    # fetch web page
    url = f"https://finance.yahoo.com/quote/{firmCode}/key-statistics"
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"}
    web = requests.get(url, headers=headers)
    soup = BeautifulSoup(web.content, 'lxml')

    keyHolder = 0
    valueHolder = 0
    flag = 0
    for index, item1 in enumerate(soup.find_all("tr")):
        for index, item2 in enumerate(item1.find_all("td")):
            if flag == 0:
                if index == 0:
                    if item2.find("span").text == "Beta (5Y Monthly)":
                        flag = 1
                        if index == 0:
                            keyHolder = item2.find("span").text
                            keyHolder = re.sub("\s", "", keyHolder)
                        if index == 1:
                            valueHolder = item2.text
                        result[keyHolder] = valueHolder
            else:
                if index == 0:
                    keyHolder = item2.find("span").text
                    keyHolder = re.sub("\s", "", keyHolder)
                if index == 1:
                    valueHolder = item2.text
                result[keyHolder] = valueHolder
    try:
        result["AvgVol3Month"] = result.pop("AvgVol(3month)")
        result["AvgVol10Day"] = result.pop("AvgVol(10day)")
        result["SharesShortPriorMonth"] = result.pop(list(result.keys())[17])
        for key in list(result.keys()):
            result[re.sub("\((.*)", "", key)] = result.pop(key)

        ## these are dates, it turns date info into mysql format
        for index in [21,22,24,25,26]:
            key = list(result.keys())[index]
            if result[key] == "N/A":
                result[key] = None
            else:
                result[key] = datetime.strptime(result[key], '%b %d, %Y').strftime('%Y-%m-%d 00:00:00')
        

        ## remove N/A and turn value with unit to pure value
        for key in list(result.keys()):
            value = str(result[key])
            # remove , in numbers
            value = re.sub("\,", "", value)
            result[key] = value
            # manage N/A
            if value == "None" or value == "N/A":
                result[key] = None
            # manage units
            switchItem = value[-1]
            match switchItem:
                case "%":
                    result[key] = round(float(value[:-1]) / 100, 5)
                case "B":
                    result[key] = round(float(value[:-1]) * 1000000000,5)
                case "M":
                    result[key] = round(float(value[:-1]) * 1000000, 5)
                case "k":
                    result[key] = round(float(value[:-1]) * 1000, 5)
                case default:
                    pass
    except Exception as e:
        return f"Error parsing: {str(e)}{os.linesep}"
    else:
        return 0

## return a list of SP500 Firms
def getFrims():
    ## fetch wiki sp500 webtree
    url = f"https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/74.0.3729.169 Safari/537.36"}
    web = requests.get(url, headers=headers)
    soup = BeautifulSoup(web.content, 'lxml')

    # parse data
    firm = {}
    symbolHolder = ""
    valueHolder = []
    table = soup.find("table", {"id":"constituents"})
    for tr in table.find_all("tr"):
        for index, td in enumerate(tr.find_all("td")):
            if index == 0:
                symbolHolder = str(td.text).strip()
            if index == 1:
                valueHolder.append(str(td.text).strip())
            if index == 3:
                valueHolder.append(str(td.text).strip())
            if index == 4:
                valueHolder.append(str(td.text).strip())
        firm[symbolHolder] = valueHolder
        symbolHolder = ""
        valueHolder = []
    del firm['']
    return firm

## Update mysql SP500:Firms
def updateSQLFirms(mydb, mycursor):
    firms = getFrims()
    for key in list(firms.keys()):
        value = firms[key]
        sql = "INSERT INTO Firms (symbol, scurity, sector, sub_ind) VALUES (%s, %s, %s, %s)"
        val = (str(key), value[0], value[1], value[2])
        mycursor.execute(sql, val)
    mydb.commit()

## Update mysql SP500:Statistics
def updateSQLFirmStats(firmCode, mydb, mycursor):
    # fetch and parse data
    firmStats = {}
    result = getFirmStats(firmStats, firmCode, mydb, mycursor)
    # check if data exist
    if result != 0:
        return result

    # mysql update
    try:
        sql = "INSERT IGNORE INTO Statistics (`Symbol&FiscalYear`, `stat_symbol`) VALUES (%s, %s)"
        uniqueCode = f"{firmCode}&{firmStats['FiscalYearEnds']}"
        val = (uniqueCode, firmCode)
        mycursor.execute(sql, val)
        for key in list(firmStats.keys()):
            sql = f"update Statistics set `{key}` = %s where `Symbol&FiscalYear` = %s"
            if firmStats[key] != None:
                firmStats[key] = str(firmStats[key])
            val = (firmStats[key], str(uniqueCode))
            mycursor.execute(sql, val)
        mydb.commit()
    except Exception as e:
        return f"{str(e)}{os.linesep}{sql%val}"
    else:
        return 0


if __name__ == "__main__":
    firmCode = "BF.B"

    mydb = mysql.connector.connect(
        host="127.01.01",
        user="root",
        password="123",
        database="SP500"
    )
    mycursor = mydb.cursor()

    print(updateSQLFirmStats(firmCode, mydb,  mycursor))
