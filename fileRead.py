import pandas
import pyodbc
import getpass

def leer_csv(filename):
    with open(filename, "r") as file:
        dataframe = pandas.read_csv(file, encoding_errors="")
    return dataframe

def conectar_bd():
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
        return conexion

    except Exception as error:
            print("Error: ", error, "\n")

conexion = conectar_bd()
with conexion.cursor() as cursor:
    archivo_resumen = open("FIFA - World Cup Summary.csv", "r")
    dataframe = pandas.read_csv(archivo_resumen)
    for i in range(len(dataframe)):
        line = list(dataframe.loc[i])
        year = line[0]
        anfitriones = line[1].split(", ")

        data = leer_csv("FIFA - {}.csv".format(year))
        crear_mundiales = '''
        IF OBJECT_ID(N'dbo.MUNDIALES', N'U') IS NULL
        CREATE TABLE MUNDIALES(
        YEAR INT NOT NULL PRIMARY KEY,
        HOST1 VARCHAR(25) NOT NULL,
        HOST2 VARCHAR(25),        
        );
        '''

        crear_mundial = '''
        IF OBJECT_ID(N'MUNDIAL_{}', N'U') IS NULL
        CREATE TABLE MUNDIAL_{}(
        YEAR_PLAYED INT NOT NULL FOREIGN KEY REFERENCES MUNDIALES(YEAR),
        PLACE INT IDENTITY(1,1) PRIMARY KEY,
        TEAM VARCHAR(25) NOT NULL,
        GAMES_PLAYED INT NOT NULL,
        GAMES_WON INT NOT NULL,
        GAMES_TIED INT NOT NULL,
        GAMES_LOST INT NOT NULL,
        GOALS_FOR INT NOT NULL,
        GOALS_AGAINST INT NOT NULL,
        GOAL_DIFF INT NOT NULL,
        POINTS INT NOT NULL
        );
        '''.format(year, year)
        cursor.execute(crear_mundiales)
        cursor.execute(crear_mundial)

        insertar_mundial_info = '''INSERT INTO MUNDIAL_{} (
        YEAR_PLAYED,
        TEAM,
        GAMES_PLAYED,
        GAMES_WON,
        GAMES_TIED,
        GAMES_LOST,
        GOALS_FOR,
        GOALS_AGAINST,
        GOAL_DIFF,
        POINTS) VALUES (?,?,?,?,?,?,?,?,?,?);
        '''.format(year)

        if len(anfitriones) == 1:
            cursor.execute("INSERT INTO MUNDIALES (YEAR, HOST1) VALUES (?,?)", (int(year), anfitriones[0]))
        else:
            cursor.execute("INSERT INTO MUNDIALES (YEAR, HOST1, HOST2) VALUES (?,?,?)", (int(year), anfitriones[0], anfitriones[1]))

        for j in range(len(data)):
            fila = list(data.loc[j])
            print(fila)
            cursor.execute(insertar_mundial_info,(int(year), fila[1], int(fila[2]), int(fila[3]), int(fila[4]), int(fila[5]), int(fila[6]), int(fila[7]), int(fila[8]), int(fila[9])))
        

