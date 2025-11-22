from SwimScraper import getTeamPerformance

results = getTeamPerformance(team_id=117, gender="M", limit=20)
print(len(results))
print(results[0])
