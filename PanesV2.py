import pandas
import pyodbc


def leer_csv(filename):
    with open(filename, "r") as file:
        dataframe = pandas.read_csv(file, encoding_errors="")
        file.close()
    return dataframe

def conectar_bd():
#    server = 'ARTEMIS\\USMDATABASE'
#    server = 'DESKTOP-9GL51HC\SQLEXPRESS'
#    dataBase = 'FUT-USM'
    flag = True
    while(flag):
        flag = False
        server = str(input("Ingrese SERVER: "))
        dataBase = str(input("Ingrese DATABASE: "))
        user = str(input("Ingrese Username: "))
        password = str(input("Ingrese Password: "))
        try:
            conexion = pyodbc.connect(
                'DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + dataBase +
                ';UID=' + user + ';PWD=' + password
                )
            print("Successful connection!\n\n")
            return conexion

        except Exception as error:
            print("Error: ", error, "\n")
            flag = True

def delete_tables(conexion):
    with conexion.cursor() as cursor:
        drop_tables = '''
            DROP TABLE dbo.MUNDIAL, dbo.MUNDIALES ;
            '''
        cursor.execute(drop_tables)

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
            YEAR_PLAYED INT,
            TEAM VARCHAR(30),
            PLACE INT,
            GAMES_PLAYED INT NOT NULL,
            GAMES_WON INT NOT NULL,
            GAMES_TIED INT NOT NULL,
            GAMES_LOST INT NOT NULL,
            GOALS_FOR INT NOT NULL,
            GOALS_AGAINST INT NOT NULL,
            GOAL_DIFF INT NOT NULL,
            POINTS INT NOT NULL,
            PRIMARY KEY(YEAR_PLAYED, TEAM)         
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
                        TEAM,
                        PLACE,
                        GAMES_PLAYED,
                        GAMES_WON,
                        GAMES_TIED,
                        GAMES_LOST,
                        GOALS_FOR,
                        GOALS_AGAINST,
                        GOAL_DIFF,
                        POINTS
                        ) VALUES (?,?,?,?,?,?,?,?,?,?,?);'''
                cursor.execute(query, (year, pais, posicion, partidos_jugados, ganados, empates, perdidos, gol_favor, gol_contra, gol_diff, puntos))

def show_champions(conexion):
    with conexion.cursor() as cursor:
        search_champions = '''
            SELECT YEAR_PLAYED,TEAM
            FROM MUNDIAL
            WHERE PLACE = 1
            '''
        cursor.execute(search_champions)
        df = pandas.DataFrame({})
        df["Year"] = None
        df["Champion"] = None
        years = []
        champions = []
        for row in cursor.fetchall():
            years.append(row[0])
            champions.append(row[1])
        df["Year"] = years
        df["Champion"] = champions
        print(df)

def maximos_goleadores(conexion):
    with conexion.cursor() as cursor:
        query = '''SELECT TOP 5 TEAM, SUM(GOALS_FOR) AS TOTAL_GOALS
        FROM MUNDIAL
        WHERE TEAM=TEAM
        GROUP BY TEAM
        ORDER BY SUM(GOALS_FOR) DESC'''
        df = pandas.DataFrame({})
        df["Team"] = None
        df["Goals"] = None
        team = []
        goals = []

        cursor.execute(query)
        
        for row in cursor.fetchall():
            team.append(row[0])
            goals.append(row[1])
        
        df["Team"] = team
        df["Goals"] = goals
        print(df, '\n')

def most_times_third(conexion):
    with conexion.cursor() as cursor:
        query = '''SELECT TOP 5 TEAM, SUM(PLACE) AS N_Third
        FROM MUNDIAL
        WHERE PLACE=3
        GROUP BY TEAM
        ORDER BY SUM(PLACE) DESC'''

        cursor.execute(query)

        df = pandas.DataFrame({})
        df["Team"] = None
        df["Times"] = None
        
        team = []
        times = []

        for row in cursor.fetchall():
            team.append(row[0])
            times.append(row[1])
        
        df["Team"] = team
        df["Times"] = times
        print(df, '\n')

def most_goals_against(conexion):
    with conexion.cursor() as cursor:
        query = '''SELECT TOP 1 TEAM, SUM(GOALS_AGAINST) AS N_GOALS_AGAINST
        FROM MUNDIAL
        GROUP BY TEAM
        ORDER BY SUM(GOALS_AGAINST) DESC'''
        cursor.execute(query)
        
        df = pandas.DataFrame({})
        team = []
        agains_num = []
        df["Team"] = None
        df["Goals Against"] = None
        for row in cursor.fetchall():
            team.append(row[0])
            agains_num.append(row[1])
        df["Team"] = team
        df["Goals Against"] = agains_num
        print(df, '\n')

def proof(conexion,country):
    with conexion.cursor() as cursor:
        proof_work = '''
            SELECT *
            FROM MUNDIAL
            WHERE TEAM = ?
            '''
        cursor.execute(proof_work,(country))
        year = []
        place = []
        games_played = []
        games_won = []
        games_tied = []
        games_lost = []
        goals_for = []
        goals_against = []
        goal_diff = []
        points = []
        df = pandas.DataFrame({})
        df["Year"] = None
        df["Place"] = None
        df["Games Played"] = None
        df["Games Won"] = None
        df["Games Tied"] = None
        df["Games Lost"] = None
        df["Goals For"] = None
        df["Goals Against"] = None
        df["Goal Diff"] = None
        df["Points"] = None

        for row in cursor.fetchall():
            year.append(row[0])
            place.append(row[2])
            games_played.append(row[3])
            games_won.append(row[4])
            games_tied.append(row[5])
            games_lost.append(row[6])
            goals_for.append(row[7])
            goals_against.append(row[8])
            goal_diff.append(row[9])
            points.append(row[10])

        df["Year"] = year
        df["Place"] = place
        df["Games Played"] = games_played
        df["Games Won"] = games_won
        df["Games Tied"] = games_tied
        df["Games Lost"] = games_lost
        df["Goals For"] = goals_for
        df["Goals Against"] = goals_against
        df["Goal Diff"] = goal_diff
        df["Points"] = points
        print('\n',country, '\n')
        print(df, '\n')

def top_three(conexion):
    with conexion.cursor() as cursor:
        top_countries = '''
            SELECT TOP 3 TEAM AS NAME,SUM(GAMES_PLAYED) AS TOTAL_GAMES_PLAYED
            FROM MUNDIAL
            GROUP BY TEAM
            ORDER BY SUM(GAMES_PLAYED) DESC
            '''
        cursor.execute(top_countries)
        df = pandas.DataFrame({})
        df["Country"] = None
        df["Total Matches"] = None
        team = []
        matches = []
        for row in cursor.fetchall():
            team.append(row[0])
            matches.append(row[1])
        df["Country"] = team
        df["Total Matches"] = matches
        print(df, '\n')

def best_ratio(conexion):
    with conexion.cursor() as cursor:
        ratio = '''
            SELECT TOP 1 TEAM, SUM(GAMES_WON)/SUM(GAMES_LOST+GAMES_TIED)
            FROM MUNDIAL
            GROUP BY TEAM
            ORDER BY SUM(GAMES_WON)/SUM(GAMES_LOST+GAMES_TIED) DESC
            '''
        cursor.execute(ratio)
        df = pandas.DataFrame({})
        df["Team"] = None
        df["Ratio"] = None
        team = []
        ratio = []
        for row in  cursor.fetchall():
            team.append(row[0])
            ratio.append(row[1])
        df["Team"] = team
        df["Ratio"] = ratio
        print(df,"\n")

def won_on_home(conexion):
    with conexion.cursor() as cursor:
        on_home = '''
            SELECT YEAR,HOST1
            FROM MUNDIAL,MUNDIALES
            WHERE YEAR=YEAR_PLAYED AND PLACE=1 AND HOST1=TEAM
            '''
        cursor.execute(on_home)
        df = pandas.DataFrame({})
        df["Year"] = None
        df["Country"] = None
        year = []
        team = []
        for row in cursor.fetchall():
            year.append(row[0])
            team.append(row[1])
        df["Year"] = year
        df["Country"] = team
        print(df, '\n')

def mostThirdOrBetter(conexion):
    with conexion.cursor() as cursor:
        wins = '''
            SELECT TOP 1 TEAM,COUNT(PLACE) AS MOST_TIMES_BETWEEN_BEST_THREE
            FROM MUNDIAL
            WHERE PLACE<4
            GROUP BY TEAM
            ORDER BY COUNT(PLACE) DESC
            '''
        cursor.execute(wins)
        df = pandas.DataFrame({})
        df["Team"] = None
        df["Times"] = None
        team = []
        times = []

        for row in cursor.fetchall():
            team.append(row[0])
            times.append(row[1])

        df["Team"] = team
        df["Times"] = times
        print(df, '\n')
        
def rivales_historicos(conexion):
    with conexion.cursor() as cursor:
        query = '''SELECT TOP 1 winners , runnerUp, count(*) as count
            FROM (select YEAR_PLAYED as year1, team as winners from mundial where place=1) as table1,
            (select YEAR_PLAYED as year2,team as runnerUp from mundial where place=2) as table2
            WHERE year1=year2
            group by winners, runnerUp
            order by count desc'''
        
        cursor.execute(query)
        df = pandas.DataFrame({})
        df["Team 1"] = None
        df["Team 2"] = None
        df["Times"] = None
        team1 = []
        team2 = []
        times = []

        for row in cursor.fetchall():
            team1.append(row[0])
            team2.append(row[1])
            times.append(row[2])

        df["Team 1"] = team1
        df["Team 2"] = team2
        df["Times"] = times

        print(df, '\n')

print("Ingrese credenciales para conectar a la base de datos.\n")
connection = conectar_bd()

flag = True
while flag:
    print("Seleccione una opcion: \n\t",
          "(c) Crear tablas. \n\t",
          "(v) Insertar datos.\n\t",
          "(b) Borrar tablas.\n\t",
          "(x) Salir.\n\t",
          "(0) Mostrar campeones por año.\n\t",
          "(1) Mostrar top 5 goleadores historicos.\n\t",
          "(2) Top 5 más veces tercer lugar historico.\n\t",
          "(3) Pais con más goles recibidos historico.\n\t",
          "(4) Buscar país.\n\t",
          "(5) Top 3 paises con más partidas historico.\n\t",
          "(6) Mejor ratio de partidas historico.\n\t",
          "(7) Campeónes locales historicos.\n\t",
          "(8) Más veces en el podio historico.\n\t",
          "(9) Más veces rivales en la final históricos.",
          )
    accion = str(input("Ingrese accion: "))
    if accion == 'c':
        print("Creando tablas\n")
        crear_tablas(connection)
        print("Tablas creadas con exito.\n")
    elif accion== 'v':
        llenar_tablas(connection)
        print("Datos ingresados.\n")
    elif accion== 'b':
        delete_tables(connection)
        print("Tablas borradas.\n")
    elif accion== '0':
        print("\nMostrar campeones por año.\n")
        show_champions(connection)
    elif accion == "1":
        print("\nMostrar top 5 goleadores historicos.\n")
        maximos_goleadores(connection)
    elif accion == "2":
        print("\nTop 5 más veces tercer lugar historico.\n")
        most_times_third(connection)
    elif accion == "3":
        print("\nPais con más goles recibidos historico.\n")
        most_goals_against(connection)
    elif accion == "4":
        pais = str(input("Ingrese país: "))
        print("\nBuscando país: ",pais,"\n")
        proof(connection, pais)
    elif accion == "5":
        print("\nTop 3 paises con más partidas historico.\n")
        top_three(connection)
    elif accion == "6":
        print("\nMejor ratio de partidas historico.\n")
        best_ratio(connection)
    elif accion == "7":
        print("\nCampeónes locales historicos.\n")
        won_on_home(connection)
    elif accion == "8":
        print("\nMás veces en el podio historico.\n")
        mostThirdOrBetter(connection)
    elif accion == "9":
        print("\nMás veces rivales en la final históricos.\n")
        rivales_historicos(connection)
    elif accion == 'x':
        flag = False
        connection.close()
    else :
        print("\nInvalid input, try again: ")

