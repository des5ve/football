import requests
import json
from requests.auth import HTTPBasicAuth
import numpy as np
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver

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