import csv
import pyodbc

server = 'ARTEMIS\\USMDATABASE'
dataBase = 'FUT-USM'
user = 'PYTHON'
password = 'gataloca'

try:
    conexion = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + dataBase +
        ';UID=' + user + ';PWD=' + password
        )
    print("Successful connection!\n\n")

except Exception as error:
    print("Error: ", error, "\n")

with open("FIFA - 1930.csv", "r") as file:
    line = csv.reader(file)
    lineCount = 0
    for data in line:
        if lineCount == 0:
            print(','.join(data))
            print("\n")
            lineCount +=1
        else:
            print(data, "\n\n")

    