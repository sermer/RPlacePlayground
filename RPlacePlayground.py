import psycopg2
from psycopg2 import Error
import csv
import datetime
import numpy as np

def ProcessUserData(connection, userID, csvWriter):
    cursor = connection.cursor()
    query = "SELECT user_id_simplified.new_id, pixeldata.timestamp FROM user_id_simplified Left JOIN pixeldata ON user_id_simplified.new_ID = pixeldata.user_id WHERE user_id_simplified.new_id = " + str(userID) + " ORDER BY pixeldata.timestamp asc;"
    cursor.execute(query)
    
    timeDeltas = []
    timeDeltas.append(str(userID))
    firstLoop = True
    timeDeltas.append(str(cursor.rowcount))
    for value in cursor:
        if firstLoop:
            firstLoop = False
            earlierDT = value[1]
        else:
            timeDeltas.append((value[1] - earlierDT).seconds)
            earlierDT = value[1]
    csvWriter.writerow(timeDeltas)
    return cursor



def ReadTopPlacerList():
    idList = list()
    with open('E:/Desktop/r-place/TopPlacers.csv', newline='') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)
        firstLoop = True
        for row in reader:
            if firstLoop:
                firstLoop = False
            else:
                idList.append(row[0])
    return idList


def CleanProcessedCSV():
    f = open('E:/Desktop/r-place/Data/UserAnalytics/TopCleanedV1.0.csv', 'w', encoding='UTF8', newline='')
    writer = csv.writer(f)
    with open('E:/Desktop/r-place/Data/UserAnalytics/TopPlacersProcessed.csv', newline='') as csvfile:
        dialect = csv.Sniffer().sniff(csvfile.read(1024))
        csvfile.seek(0)
        reader = csv.reader(csvfile, dialect)
        for row in reader:
            fieldList = list()
            valueList = list()
            for value in row:
                if value != 0:
                    fieldList.append(int(value))
                    if(len(fieldList) > 2):
                        valueList.append(int(value))
            valueArray = np.array(valueList)
            p = np.percentile(valueArray,95)
            i = 0
            while i < len(fieldList):
                if(i > 2 and fieldList[i] > p):
                    fieldList.pop(i)
                else:
                    i += 1
            writer.writerow(fieldList)


try:
    connection = psycopg2.connect(user="postgres",
                                  password="",
                                  host="127.0.0.1",
                                  port="5434",
                                  database="RPlace2022")
    
    CleanProcessedCSV()
    idList = ReadTopPlacerList()
    #f = open('E:/Desktop/r-place/Data/UserAnalytics/Last500k.csv', 'w', encoding='UTF8', newline='')
    f = open('E:/Desktop/r-place/Data/UserAnalytics/TopPlacersProcessed.csv', 'w', encoding='UTF8', newline='')
    writer = csv.writer(f)

    i = 0
    for uid in idList:
        ProcessUserData(connection, uid, writer)
        i += 1
        if(i % 2500 == 0):
            print(i)

    #i = 500001
    ##while i <= 10381162:
    #while i <= 10381162:
    #    if(i % 10000 == 0):
    #        print(i)
    #    ProcessUserData(connection, i, writer)
    #    i += 1

    f.close()
    

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        connection.close()
        print("PostgreSQL connection is closed")
