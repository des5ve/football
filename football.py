import requests
import json
from requests.auth import HTTPBasicAuth
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver


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