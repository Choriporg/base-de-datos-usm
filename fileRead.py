import pandas
import pyodbc
import getpass

server = 'ARTEMIS\\USMDATABASE'
dataBase = 'FUT-USM'
user = str(input("User: "))
password = str(getpass.getpass())

try:
    conexion = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + dataBase +
        ';UID=' + user + ';PWD=' + password
        )
    print("Successful connection!\n\n")

except Exception as error:
    print("Error: ", error, "\n")

with conexion.cursor() as csr:
    with open("FIFA - World Cup Summary.csv", "r") as summary:
        print("FILE is open\n")
        dataFrame = pandas.read_csv(summary)
        print(dataFrame)
        crearTabla = '''
            IF OBJECT_ID(N'dbo.summary', N'U') IS NULL
                CREATE TABLE summary(
                YEAR INT NOT NULL PRIMARY KEY,
                HOST VARCHAR(25),
                CHAMPION VARCHAR(25),
                SECOND_PLACE VARCHAR(25),
                THIRD_PLACE VARCHAR(25),
                TEAMS INT,
                MATCHES_NUM INT, 
                GOALS_SCORED INT,
                MAX_GOALS INT
            );
            '''
        csr.execute(crearTabla)
        try:
            consulta = "INSERT INTO summary(YEAR, HOST, CHAMPION, SECOND_PLACE, THIRD_PLACE, TEAMS, MATCHES_NUM, GOALS_SCORED, MAX_GOALS) VALUES (?,?,?,?,?,?,?,?,?)"
            for i in range(len(dataFrame)):
                parametros = list(dataFrame.loc[i])
                csr.execute(consulta, (int(parametros[0]), parametros[1], parametros[2], parametros[3], parametros[4], int(parametros[5]), int(parametros[6]), int(parametros[7]), int(parametros[8])))
        except Exception as e:
            print("Error: ", e)
        finally:
            csr.close()
            summary.close()

archivos = [
    'FIFA - 1930.csv', 'FIFA - 1934.csv', 'FIFA - 1938.csv', 'FIFA - 1950.csv',
    'FIFA - 1954.csv', 'FIFA - 1958.csv', 'FIFA - 1962.csv', 'FIFA - 1966.csv',
    'FIFA - 1970.csv', 'FIFA - 1974.csv', 'FIFA - 1978.csv', 'FIFA - 1982.csv',
    'FIFA - 1986.csv', 'FIFA - 1990.csv', 'FIFA - 1994.csv', 'FIFA - 1998.csv',
    'FIFA - 2002.csv', 'FIFA - 2006.csv', 'FIFA - 2010.csv', 'FIFA - 2014.csv',
    'FIFA - 2018.csv', 'FIFA - 2022.csv'
    ]

for fileName in archivos:
    year = (fileName.split("- "))[1].split(".csv")[0]
    with open(fileName, "r") as file:
        data = pandas.read_csv(file)
        columnas = data.columns
        infoDataFrame = data.shape #(n° de filas, n° de columnas)
  