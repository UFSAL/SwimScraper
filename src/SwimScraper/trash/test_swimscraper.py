#This is a test file that you can use to test whatever functions you want

from SwimScraper import getTeamPerformance

results = getTeamPerformance(team_id=117, gender="M", limit=20)
print(len(results))
print(results[0])
