import requests
import json
from requests.auth import HTTPBasicAuth
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
import mysql.connector
import pymysql

statDictionary = {
"pointsPerGame" : "https://www.teamrankings.com/nfl/stat/points-per-game",
"ypp" : "https://www.teamrankings.com/nfl/stat/yards-per-play",
"yardsPerRush" : "https://www.teamrankings.com/nfl/stat/yards-per-rush-attempt",
"yardsPerPass" : "https://www.teamrankings.com/nfl/stat/yards-per-pass-attempt",
"ypp_Against" : "https://www.teamrankings.com/nfl/stat/opponent-yards-per-play",
"yardsPerRush_Against" : "https://www.teamrankings.com/nfl/stat/opponent-yards-per-rush-attempt",
"yardsPerPass_Against" : "https://www.teamrankings.com/nfl/stat/opponent-yards-per-pass-attempt",
"pointsPerGame_Against" : "https://www.teamrankings.com/nfl/stat/opponent-points-per-game",

}

frameArray = []
for key, value in statDictionary.items():
   frameName = key + "Frame"
   frame =  pd.read_html(value)[0].add_prefix(key)
   frameArray.append(frame)
   

mergeFrameOne = pd.merge(left = frameArray[0], right =frameArray[1] , left_on = 'pointsPerGameTeam', right_on= 'yppTeam')
mergeFrameTwo = pd.merge(left = frameArray[2], right = frameArray[3], left_on = 'yardsPerRushTeam', right_on = 'yardsPerPassTeam')
mergeFrameThree = pd.merge(left = frameArray[4], right = frameArray[5], left_on = 'ypp_AgainstTeam', right_on = 'yardsPerRush_AgainstTeam')
mergeFrameFour = pd.merge(left = frameArray[6], right = frameArray[7], left_on = 'yardsPerPass_AgainstTeam', right_on = 'pointsPerGame_AgainstTeam')

midMerge = pd.merge(left= mergeFrameOne, right = mergeFrameTwo, left_on= 'pointsPerGameTeam', right_on = 'yardsPerPassTeam')
midMergeTwo = pd.merge(left = mergeFrameThree, right = mergeFrameFour, left_on = 'ypp_AgainstTeam', right_on = 'pointsPerGame_AgainstTeam')

finalMerge = pd.merge(left = midMerge, right = midMergeTwo, left_on = 'pointsPerGameTeam', right_on = 'pointsPerGame_AgainstTeam')

print (finalMerge)

teamStats = finalMerge.drop(columns= ['yppTeam', 'yardsPerRushTeam', 'yardsPerPassTeam', 'ypp_AgainstTeam', 'yardsPerRush_AgainstTeam', 'pointsPerGameTeam', 'pointsPerGame_AgainstTeam'])
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



"ALTER TABLE BetTrack.testCreate ADD {columnName} FLOAT NULL; "


strings = []
for name in columnNames:
   startOfString = "ALTER TABLE BetTrack.nfl_team_stats ADD "
   endOfString = ' FLOAT NULL;'
   totalString = startOfString + name + endOfString
   strings.append(totalString)

print (strings)


mydb = mysql.connector.connect(
  host="nfldb2.cke1iobwnywt.us-east-1.rds.amazonaws.com",
  user="",
  password="",
  database= "BetTrack"
)

mycursor = mydb.cursor()

truncateSQL = 'TRUNCATE TABLE nfl_team_stats'
mycursor.execute(truncateSQL)


sql = "INSERT INTO nfl_team_stats (pointsPerGameRank, pointsPerGame2020, pointsPerGameLast_3, pointsPerGameLast_1, pointsPerGameHome, pointsPerGameAway, pointsPerGame2019, yppRank, ypp2020, yppLast_3, yppLast_1, yppHome, yppAway, ypp2019, yardsPerRushRank, yardsPerRush2020, yardsPerRushLast_3, yardsPerRushLast_1, yardsPerRushHome, yardsPerRushAway, yardsPerRush2019, yardsPerPassRank, yardsPerPass2020, yardsPerPassLast_3, yardsPerPassLast_1, yardsPerPassHome, yardsPerPassAway, yardsPerPass2019, ypp_AgainstRank, ypp_Against2020, ypp_AgainstLast_3, ypp_AgainstLast_1, ypp_AgainstHome, ypp_AgainstAway, ypp_Against2019, yardsPerRush_AgainstRank, yardsPerRush_Against2020, yardsPerRush_AgainstLast_3, yardsPerRush_AgainstLast_1, yardsPerRush_AgainstHome, yardsPerRush_AgainstAway, yardsPerRush_Against2019, yardsPerPass_AgainstRank, yardsPerPass_AgainstTeam, yardsPerPass_Against2020, yardsPerPass_AgainstLast_3, yardsPerPass_AgainstLast_1, yardsPerPass_AgainstHome, yardsPerPass_AgainstAway, yardsPerPass_Against2019, pointsPerGame_AgainstRank, pointsPerGame_Against2020, pointsPerGame_AgainstLast_3, pointsPerGame_AgainstLast_1, pointsPerGame_AgainstHome, pointsPerGame_AgainstAway, pointsPerGame_Against2019) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
mycursor.executemany(sql, gameData)
mydb.commit()
print (mycursor.rowcount, "record inserted")
