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
        
        for row in cursor.fetchall():
            team.append(row[0])
            goals.append(row[1])
        
        df["Team"] = team
        df["Goals"] = goals
        print(df, '\n')