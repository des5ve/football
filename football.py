import requests
import json
from requests.auth import HTTPBasicAuth
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import mysql.connector
import pymysql



timeOfPosession = "https://www.teamrankings.com/college-football/stat/average-time-of-possession-net-of-ot"
playsPerGame = "https://www.teamrankings.com/college-football/stat/plays-per-game"
pointsPerGame = "https://www.teamrankings.com/college-football/stat/points-per-game"
ypp = "https://www.teamrankings.com/college-football/stat/yards-per-play"
yppa = "https://www.teamrankings.com/college-football/stat/opponent-yards-per-play"
passPercent = "https://www.teamrankings.com/college-football/stat/passing-play-pct"
yardPerPassAttempt = "https://www.teamrankings.com/college-football/stat/yards-per-pass-attempt"
takeAways = "https://www.teamrankings.com/college-football/stat/takeaways-per-game"
giveAways = "https://www.teamrankings.com/college-football/stat/giveaways-per-game"
puntYardsPerGame = "https://www.teamrankings.com/college-football/stat/gross-punt-yards-per-game"
puntsPerGame = "https://www.teamrankings.com/college-football/stat/punt-attempts-per-game"


posessionFrame = pd.read_html(timeOfPosession)[0].add_prefix('TOP_')
playsPerGameFrame =pd.read_html(playsPerGame)[0].add_prefix('PlaysPerGame_')
pointsPerGameFrame = pd.read_html(pointsPerGame)[0].add_prefix('PointsPerGame_')
yppFrame = pd.read_html(ypp)[0].add_prefix('YPP_')
yppaFrame = pd.read_html(yppa)[0].add_prefix('YPPA_')
passPercentFrame = pd.read_html(passPercent)[0].add_prefix('PassPercent_')
yardPerPassAttemptFrame = pd.read_html(yardPerPassAttempt)[0].add_prefix('YardsPerPassAttempt_')
takeAwaysFrame = pd.read_html(takeAways)[0].add_prefix('TakeAwaysPerGame_')
giveAwaysFrame = pd.read_html(giveAways)[0].add_prefix('GiveAwaysPerGame_')
puntYardsPerGameFrame = pd.read_html(puntYardsPerGame)[0].add_prefix('PuntYardsPerGame_')
puntsPerGameFrame = pd.read_html(puntsPerGame)[0].add_prefix('PuntsPerGame_')



merged_inner = pd.merge(left = posessionFrame, right = playsPerGameFrame, left_on = 'TOP_Team', right_on= 'PlaysPerGame_Team')
merge_innerOne = pd.merge( left = yppFrame, right = yppaFrame, left_on ='YPP_Team', right_on = 'YPPA_Team')
merge_innerTwo = pd.merge( left = passPercentFrame, right = yardPerPassAttemptFrame, left_on ='PassPercent_Team', right_on = 'YardsPerPassAttempt_Team')
merge_innerThree = pd.merge( left = takeAwaysFrame, right = giveAwaysFrame, left_on ='TakeAwaysPerGame_Team', right_on = 'GiveAwaysPerGame_Team')
merge_innerFour = pd.merge( left = puntYardsPerGameFrame, right = puntsPerGameFrame, left_on ='PuntYardsPerGame_Team', right_on = 'PuntsPerGame_Team')


secondMerge = pd.merge(left = merged_inner, right = merge_innerOne, left_on = 'TOP_Team', right_on = 'YPPA_Team' )
secondMergeTwo = pd.merge(left = merge_innerTwo, right = merge_innerThree, left_on = 'PassPercent_Team', right_on = 'GiveAwaysPerGame_Team')

thirdMerge = pd.merge(left = secondMerge, right = secondMergeTwo, left_on = 'TOP_Team', right_on = 'GiveAwaysPerGame_Team')

finalMerge = pd.merge(left = thirdMerge, right = merge_innerFour, left_on = 'TOP_Team', right_on = 'PuntsPerGame_Team')


teamStats = finalMerge.drop(columns = ['PlaysPerGame_Team', 'YPP_Team', 'YPPA_Team', 'PassPercent_Team', 'YardsPerPassAttempt_Team', 'TakeAwaysPerGame_Team','GiveAwaysPerGame_Team', 'PuntYardsPerGame_Team','PuntsPerGame_Team'] ).rename(columns ={'TOP_Team': 'Team'})
print (teamStats)
columns = teamStats.columns


gameData = []
numberOfColumns = teamStats.shape[1]
columnNames =  teamStats.columns.tolist()
placeHolder = []
for i in columns:
  placeHolder.append('%s')
tupleHolder = tuple(placeHolder)
print (tupleHolder)
print ("Column Names: ", columnNames)
for i, row in teamStats.iterrows():
    dataRow = []
    iterator = 0
    while iterator < numberOfColumns:
      dataRow.append(row[columnNames[iterator]])
      iterator +=1
    gameData.append(dataRow)
print ("Data from Frame in Arrays:", gameData)


mydb = mysql.connector.connect(
  host="nfldb2.cke1iobwnywt.us-east-1.rds.amazonaws.com",
  user="des5ve",
  passwd="Cm14fcfire",
    database= "BetTrack"
)

mycursor = mydb.cursor()

truncateSQL = 'TRUNCATE TABLE college_football_teams'
mycursor.execute(truncateSQL)
sql = "INSERT INTO college_football_teams (TOP_Rank, Team, TOP_2020, TOP_Last_3, TOP_Last_1, TOP_Home, TOP_Away, TOP_2019, PlaysPerGame_Rank, PlaysPerGame_2020, PlaysPerGame_Last_3, PlaysPerGame_Last_1, PlaysPerGame_Home, PlaysPerGame_Away, PlaysPerGame_2019, YPP_Rank, YPP_2020, YPP_Last_3, YPP_Last_1, YPP_Home, YPP_Away, YPP_2019, YPPA_Rank, YPPA_2020, YPPA_Last_3, YPPA_Last_1, YPPA_Home, YPPA_Away, YPPA_2019, PassPercent_Rank, PassPercent_2020, PassPercent_Last_3, PassPercent_Last_1, PassPercent_Home, PassPercent_Away, PassPercent_2019, YardsPerPassAttempt_Rank, YardsPerPassAttempt_2020, YardsPerPassAttempt_Last_3, YardsPerPassAttempt_Last_1, YardsPerPassAttempt_Home, YardsPerPassAttempt_Away, YardsPerPassAttempt_2019, TakeAwaysPerGame_Rank, TakeAwaysPerGame_2020, TakeAwaysPerGame_Last_3, TakeAwaysPerGame_Last_1, TakeAwaysPerGame_Home, TakeAwaysPerGame_Away, TakeAwaysPerGame_2019, GiveAwaysPerGame_Rank, GiveAwaysPerGame_2020, GiveAwaysPerGame_Last_3, GiveAwaysPerGame_Last_1, GiveAwaysPerGame_Home, GiveAwaysPerGame_Away, GiveAwaysPerGame_2019, PuntYardsPerGame_Rank, PuntYardsPerGame_2020, PuntYardsPerGame_Last_3, PuntYardsPerGame_Last_1, PuntYardsPerGame_Home, PuntYardsPerGame_Away, PuntYardsPerGame_2019, PuntsPerGame_Rank, PuntsPerGame_2020, PuntsPerGame_Last_3, PuntsPerGame_Last_1, PuntsPerGame_Home, PuntsPerGame_Away, PuntsPerGame_2019) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"

mycursor.executemany(sql, gameData)
mydb.commit()
print (mycursor.rowcount, "record inserted")


