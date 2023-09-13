import pandas
import pyodbc
import getpass

def obtener_paises(archivos):
    paises = []
    for fileName in archivos:
        file = open(fileName, "r")
        df = pandas.read_csv(file)
        for i in range(len(df)):
            data = list(df.loc[i])
            country = data[1]
            if country not in paises:
                paises.append(country)
        file.close()
    return paises  

archivos = [
    'FIFA - 1930.csv', 'FIFA - 1934.csv', 'FIFA - 1938.csv', 'FIFA - 1950.csv',
    'FIFA - 1954.csv', 'FIFA - 1958.csv', 'FIFA - 1962.csv', 'FIFA - 1966.csv',
    'FIFA - 1970.csv', 'FIFA - 1974.csv', 'FIFA - 1978.csv', 'FIFA - 1982.csv',
    'FIFA - 1986.csv', 'FIFA - 1990.csv', 'FIFA - 1994.csv', 'FIFA - 1998.csv',
    'FIFA - 2002.csv', 'FIFA - 2006.csv', 'FIFA - 2010.csv', 'FIFA - 2014.csv',
    'FIFA - 2018.csv', 'FIFA - 2022.csv'
    ]
server = 'ARTEMIS\\USMDATABASE'
dataBase = 'FUT-USM'
user = str(input("User: "))
password = str(getpass.getpass())

try:
    conexion = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + dataBase +
        ';UID=' + user + ';PWD=' + password
    )

except Exception as error:
    print("Error: ", error, "\n")

paises = obtener_paises(archivos)
cursor = conexion.cursor()
for pais in paises:
    consulta = '''
    IF OBJECT_ID(N'dbo.{}', N'U') IS NULL
        CREATE TABLE {}(
            YEAR INT NOT NULL PRIMARY KEY,
            POSITION INT NOT NULL,
            GAMES_PLAYED INT NOT NULL,
            GAMES_WON INT NOT NULL,
            GAMES_TIED INT NOT NULL,
            GAMES_LOST INT NOT NULL,
            GOALS_FOR INT NOT NULL,
            GOALS_AGAINST INT NOT NULL,
            GOALS_DIFF INT NOT NULL,
            POINTS INT NOT NULL
        );'''.format(pais, pais)
    
    print(consulta + '\n')
    cursor.execute(consulta)
