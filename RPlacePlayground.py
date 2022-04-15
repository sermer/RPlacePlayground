import psycopg2
from psycopg2 import Error
import csv
import datetime

def ProcessUserData(connection, userID, csvWriter):
    cursor = connection.cursor()
    query = "SELECT user_id_simplified.new_id, pixeldata.timestamp FROM user_id_simplified Left JOIN pixeldata ON user_id_simplified.new_ID = pixeldata.user_id WHERE user_id_simplified.new_id = " + str(userID) + " ORDER BY pixeldata.timestamp asc;"
    cursor.execute(query)
    
    timeDeltas = []
    timeDeltas.append(str(userID))
    firstLoop = False
    timeDeltas.append(str(cursor.rowcount))
    for value in cursor:
        if not firstLoop:
            firstLoop = True
            earlierDT = value[1]
        else:
            timeDeltas.append((value[1] - earlierDT).seconds)
            earlierDT = value[1]
    csvWriter.writerow(timeDeltas)
    return cursor

try:
    connection = psycopg2.connect(user="postgres",
                                  password="",
                                  host="127.0.0.1",
                                  port="5434",
                                  database="RPlace2022")
    
    f = open('E:/Desktop/r-place/Data/UserAnalytics/Last500k.csv', 'w', encoding='UTF8', newline='')
    writer = csv.writer(f)

    i = 500001
    #while i <= 10381162:
    while i <= 10381162:
        if(i % 10000 == 0):
            print(i)
        ProcessUserData(connection, i, writer)
        i += 1

    f.close()
    

except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
finally:
    if (connection):
        connection.close()
        print("PostgreSQL connection is closed")


