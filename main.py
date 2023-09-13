import pandas
import pyodbc


def leer_csv(filename):
    with open(filename, "r") as file:
        dataframe = pandas.read_csv(file, encoding_errors="")
        file.close()
    return dataframe

def conectar_bd():
    server = 'ARTEMIS\\USMDATABASE'
    dataBase = 'FUT-USM'
    user = 'PYTHON'
    password = 'python'

    try:
        conexion = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + dataBase +
            ';UID=' + user + ';PWD=' + password
            )
        print("Successful connection!\n\n")
        return conexion

    except Exception as error:
            print("Error: ", error, "\n")

def crear_tablas(conexion):
    with conexion.cursor() as cursor:
        crear_mundiales = '''
            IF OBJECT_ID(N'dbo.MUNDIALES', N'U') IS NULL
            CREATE TABLE MUNDIALES(
            YEAR INT NOT NULL PRIMARY KEY,
            HOST1 VARCHAR(25) NOT NULL,
            HOST2 VARCHAR(25),
            MATCHES_PLAYED INT NOT NULL,
            AVG_GOALS_PER_GAME INT NOT NULL
            );
            '''
        crear_mundial = '''
            IF OBJECT_ID(N'dbo.MUNDIAL',N'U') IS NULL
            CREATE TABLE MUNDIAL(
            YEAR_PLAYED INT FOREIGN KEY REFERENCES MUNDIALES(YEAR),
            PLACE INT,
            TEAM VARCHAR(20),
            GAMES_PLAYED INT NOT NULL,
            GAMES_WON INT NOT NULL,
            GAMES_TIED INT NOT NULL,
            GAMES_LOST INT NOT NULL,
            GOALS_FOR INT NOT NULL,
            GOALS_AGAINST INT NOT NULL,
            GOAL_DIFF INT NOT NULL,
            POINTS INT NOT NULL,
            PRIMARY KEY(YEAR_PLAYED, PLACE)         
            );'''
        cursor.execute(crear_mundiales)
        cursor.execute(crear_mundial)

def llenar_tablas(conexion):
    with conexion.cursor() as cursor:
        df_summary = leer_csv("FIFA - World Cup Summary.csv")
        for i in range(len(df_summary)):
            fila = list(df_summary.loc[i])
            year = int(fila[0])
            match_num = int(fila[6])
            prom_goles = int(fila[8])
            anfitriones = fila[1].split(', ')

            if len(anfitriones) == 1:
                insertar_mundiales = "INSERT INTO MUNDIALES (YEAR, HOST1, MATCHES_PLAYED, AVG_GOALS_PER_GAME) VALUES (?,?,?,?);"
                cursor.execute(insertar_mundiales, (year, anfitriones[0], match_num, prom_goles))
            else: 
                insertar_mundiales = "INSERT INTO MUNDIALES (YEAR, HOST1, HOST2, MATCHES_PLAYED, AVG_GOALS_PER_GAME) VALUES (?,?,?,?,?);"
                cursor.execute(insertar_mundiales, (year, anfitriones[0], anfitriones[1], match_num, prom_goles))
            
            df_mundial = leer_csv("FIFA - {}.csv".format(str(year)))
            for j in range(len(df_mundial)):
                fila_mundial = list(df_mundial.loc[j])
                posicion = int(fila_mundial[0])
                pais = fila_mundial[1]
                partidos_jugados = int(fila_mundial[2])
                ganados = int(fila_mundial[3])
                empates = int(fila_mundial[4])
                perdidos = int(fila_mundial[5])
                gol_favor = int(fila_mundial[6])
                gol_contra = int(fila_mundial[7])
                gol_diff = int(fila_mundial[8])
                puntos = int(fila_mundial[9])

                query = '''
                    INSERT INTO MUNDIAL (
                        YEAR_PLAYED,
                        PLACE,
                        TEAM,
                        GAMES_PLAYED,
                        GAMES_WON,
                        GAMES_TIED,
                        GAMES_LOST,
                        GOALS_FOR,
                        GOALS_AGAINST,
                        GOAL_DIFF,
                        POINTS
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?);'''
                cursor.execute(query, (year, posicion, pais, partidos_jugados, ganados, empates, perdidos, gol_favor, gol_contra, gol_diff, puntos))





def mostrar_campeones(conexion): #en desarrollo
    query = "SELECT YEAR FROM MUNDIALES JOIN"

print("Ingrese credenciales para conectar a la base de datos.\n")
connection = conectar_bd()
flag = True
while flag:
    print("Seleccione una opcion: \n\t(1) Crear tablas. \n\t(2) Insertar datos. \n\t(Otro) Salir.")
    accion = int(input("Ingrese accion: "))
    if accion == 1:
        print("Creando tablas\n")
        crear_tablas(connection)
        print("Tablas creadas con exito.\n")
    elif accion== 2:
        llenar_tablas(connection)
        print("Datos ingresados.\n")
    else:
        flag = False
        connection.close()



