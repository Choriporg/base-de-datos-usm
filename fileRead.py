import csv
import pyodbc

server = 'ARTEMIS\\USMDATABASE'
dataBase = 'FUT-USM'
user = 'PYTHON'
password = 'gataloca'

archivos = [
    'FIFA - 1930.csv', 'FIFA - 1934.csv', 'FIFA - 1938.csv', 'FIFA - 1950.csv',
    'FIFA - 1954.csv', 'FIFA - 1958.csv', 'FIFA - 1962.csv', 'FIFA - 1966.csv',
    'FIFA - 1970.csv', 'FIFA - 1974.csv', 'FIFA - 1978.csv', 'FIFA - 1982.csv',
    'FIFA - 1986.csv', 'FIFA - 1990.csv', 'FIFA - 1994.csv', 'FIFA - 1998.csv',
    'FIFA - 2002.csv', 'FIFA - 2006.csv', 'FIFA - 2010.csv', 'FIFA - 2014.csv',
    'FIFA - 2018.csv', 'FIFA - World Cup Summary.csv']

try:
    conexion = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + dataBase +
        ';UID=' + user + ';PWD=' + password
        )
    print("Successful connection!\n\n")

except Exception as error:
    print("Error: ", error, "\n")

for fileName in archivos:
    with open(fileName, "r") as file:
        line = csv.reader(file)
        lineCount = 0

        for data in line:
            if lineCount == 0:
                print(' | '.join(data))
                print("\n")
                lineCount +=1

            else:
                print(' | '.join(data), "\n\n")


    