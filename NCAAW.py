import asyncio, aiohttp, async_timeout, json, datetime as dt, itertools, copy, pickle, math, scipy
from bs4 import BeautifulSoup
from time import strptime
from statistics import mean
from scipy.stats.mstats import gmean


class Team:
	def __init__(self, teamid, longname, shortname=None, isd1=False, conference=None):
		self.teamid = teamid
		self.longname = longname
		self.shortname = shortname
		self.isd1 = isd1
		self.conference = conference
		self.games = set()
	
	def winslosses(self):
		sortedlist = [[],[]]
		for game in self.games:
			if game.winner() is self:
				sortedlist[0].append(game)
			elif game.loser() is self:
				sortedlist[1].append(game)
		return sortedlist
	
	def opponent(self, game):
		if game.hometeam is self:
			return game.awayteam
		elif game.awayteam is self:
			return game.hometeam
		else:
			print(self.longname+' did not play in game '+str(game.gameid)+'!')
	
	def D1winslosses(self):
		winslosses = self.winslosses()
		return [[win for win in winslosses[0] if self.opponent(win).isd1], [loss for loss in winslosses[1] if self.opponent(loss).isd1]]
	
	def record(self):
		winslosses = self.winslosses()
		return str(len(winslosses[0]))+'-'+str(len(winslosses[1]))
	
	def D1record(self):
		D1winslosses = self.D1winslosses()
		return str(len(D1winslosses[0]))+'-'+str(len(D1winslosses[1]))
	
	def winpct(self):
		winslosses = self.winslosses()
		wins = winslosses[0]
		losses = winslosses[1]
		if not wins and not losses:
			return 0.
		else:
			return len(wins)/(len(wins)+len(losses))
	
	def D1winpct(self):
		D1winslosses = self.D1winslosses()
		D1wins = D1winslosses[0]
		D1losses = D1winslosses[1]
		if not D1wins and not D1losses:
			return 0.
		else:
			return len(D1wins)/(len(D1wins)+len(D1losses))
	
	def D1winpctwithoutopponent(self, opponent):
		D1winslosses = self.D1winslosses()
		D1winswithoutopponent = [win for win in D1winslosses[0] if self.opponent(win) is not opponent]
		D1losseswithoutopponent = [loss for loss in D1winslosses[1] if self.opponent(loss) is not opponent]
		if not D1winswithoutopponent and not D1losseswithoutopponent:
			return 0.
		else:
			return len(D1winswithoutopponent)/(len(D1winswithoutopponent)+len(D1losseswithoutopponent))
	
	def opponents(self):
		return {self.opponent(game) for game in self.games}
	
	def D1opponents(self):
		return {self.opponent(game) for game in self.games if self.opponent(game).isd1}
	
	def OWP(self):
		return mean([opponent.D1winpctwithoutopponent(self) for opponent in self.D1opponents()])
	
	def OOWP(self):
		return mean([opponent.OWP() for opponent in self.D1opponents()])
	
	def RPI(self):
		return 0.25*self.D1winpct()+0.5*self.OWP()+0.25*self.OOWP()


class Game:
	def __init__(self, gameid, time, hometeam, homescore, awayteam, awayscore, ots=0):
		self.gameid = gameid
		self.time = time
		self.hometeam = hometeam
		self.homescore = homescore
		self.awayteam = awayteam
		self.awayscore = awayscore
		self.ots = ots
	
	def winner(self):
		if self.homescore > self.awayscore:
			return self.hometeam
		elif self.awayscore > self.homescore:
			return self.awayteam
	
	def loser(self):
		if self.homescore > self.awayscore:
			return self.awayteam
		elif self.awayscore > self.homescore:
			return self.hometeam
	
	def pointtotal(self):
		return self.homescore+self.awayscore
	
	def pointdifferential(self):
		return abs(self.homescore-self.awayscore)


def savedata(teams, games, savefile):
	saveteams = [[team.teamid, team.longname, team.shortname, team.isd1, team.conference, [game.gameid for game in team.games]] for team in teams.values()]
	savegames = [[game.gameid, game.time, game.hometeam.teamid, game.homescore, game.awayteam.teamid, game.awayscore, game.ots] for game in games.values()]
	pickle.dump([saveteams, savegames], open(savefile,'wb'))

def recalldata(savefile):
	teamsgames = pickle.load(open(savefile,'rb'))
	recallteams = {team[0] : Team(team[0], team[1], shortname=team[2], isd1=team[3], conference=team[4]) for team in teamsgames[0]}
	recallgames = {game[0] : Game(game[0], game[1], recallteams[game[2]], game[3], recallteams[game[4]], game[5], ots=game[6]) for game in teamsgames[1]}
	for game in recallgames.values():
		game.hometeam.games.add(game)
		game.awayteam.games.add(game)
	return recallteams, recallgames


async def getD1teams(session):
	teams = dict()
	async with session.get('https://www.espn.com/womens-college-basketball/teams') as resp:
		html = await resp.text()
		teamsoup = BeautifulSoup(html, 'html5lib').html.body.script
	columns = json.loads(teamsoup.text[23:-1])['page']['content']['leagueTeams']['columns']
	for column in columns:
		groups = column['groups']
		for group in groups:
			conference = group['nm']
			teamsjson = group['tms']
			for teamjson in teamsjson:
				teamid = int(teamjson['id'])
				longname = teamjson['n']
				team = Team(teamid, longname, isd1=True, conference=conference)
				teams[teamid] = team
	return teams

async def dictyieldvalues(dictionary):
	for i in dictionary.values():
		yield i

async def getD1teamschedulejson(teamid, session):
	#print('getting '+str(teamid))
	schedulejson = None
	while not schedulejson:
		async with session.get('https://www.espn.com/womens-college-basketball/team/schedule/_/id/'+str(teamid)) as resp:
			try:
				with async_timeout.timeout(5.0):
					html = await asyncio.wait_for(resp.text(), timeout=5.0)
			except asyncio.TimeoutError:
				#print('still waiting for '+str(teamid)+', restarting')
				continue
			try:
				schedulesoup = BeautifulSoup(html, 'html5lib').html.body.script
				schedulejson = json.loads(schedulesoup.text[23:-1])['page']['content']['scheduleData']
			except json.decoder.JSONDecodeError:
				#print('problem with '+str(teamid))
				pass
	#print('done getting '+str(teamid))
	return teamid, schedulejson

def processschedulejson(teams, team, schedulejson):
	games = dict()
	team.shortname = schedulejson['team']['abbrev']
	gamesjson = list(itertools.chain.from_iterable([schedule['events']['post'] for schedule in schedulejson['teamSchedule']]))
	for gamejson in gamesjson:
		gameid = int(gamejson['time']['link'].split('=')[1])
		datetime = dt.datetime.strptime(gamejson['date']['date'], '%Y-%m-%dT%H:%MZ').replace(tzinfo=dt.timezone.utc)
		
		opponentjson = gamejson['opponent']
		opponentid = int(opponentjson['id'])
		if opponentid not in teams:
			try:
				opponentshortname = opponentjson['abbrev']
			except KeyError:
				opponentshortname = None	
			opponent = Team(opponentid, opponentjson['displayName'], shortname=opponentshortname)
			teams[opponentid] = opponent
		else:
			opponent = teams[opponentid]
		vsat = opponentjson['homeAwaySymbol']
		neutralsite = opponentjson['neutralSite']
		
		result = gamejson['result']
		teamscore = int(result['currentTeamScore'])
		opponentscore = int(result['opponentTeamScore'])
		
		try:
			overtime = result['overtime']
			if overtime == 'OT':
				ots = 1
			else:
				ots = int(overtime[:-2])
		except KeyError:
			ots = 0

		if vsat == 'vs':
			if neutralsite:
				gameteams = sorted([[team, teamscore], [opponent, opponentscore]], key=lambda x: x[0].teamid)
				hometeam = gameteams[0][0]
				homescore = gameteams[0][1]
				awayteam = gameteams[1][0]
				awayscore = gameteams[1][1]
				game = Game(gameid, datetime, hometeam, homescore, awayteam, awayscore, ots=ots)
				games[gameid] = game
			else:
				hometeam = team
				homescore = teamscore
				awayteam = opponent
				awayscore = opponentscore
				game = Game(gameid, datetime, hometeam, homescore, awayteam, awayscore, ots=ots)
				games[gameid] = game
		elif vsat == '@':
			hometeam = opponent
			homescore = opponentscore
			awayteam = team
			awayscore = teamscore
			game = Game(gameid, datetime, hometeam, homescore, awayteam, awayscore, ots=ots)
			games[gameid] = game
	return games

async def getD1teamsgames(savefile=None):
	async with aiohttp.ClientSession() as session:
		teams = await getD1teams(session)
		schedulejsons = await asyncio.gather(*[getD1teamschedulejson(team.teamid, session) async for team in dictyieldvalues(teams)])
	games = dict()
	for teamid, schedulejson in schedulejsons:
		games.update(processschedulejson(teams, teams[teamid], schedulejson))
	for game in games.values():
		game.hometeam.games.add(game)
		game.awayteam.games.add(game)
	return teams, games

#KRACH rankings
def cleanexistingties(teamsin, gamesin):
	teams = {team.teamid: Team(team.teamid, team.longname, shortname=team.shortname, isd1=team.isd1, conference=team.conference) for team in teamsin.values()}
	games = {game.gameid: Game(game.gameid, game.time, teams[game.hometeam.teamid], game.homescore, teams[game.awayteam.teamid], game.awayscore) for game in gamesin.values() if game.winner()}
	for game in list(games.values()):
		game.hometeam.games.add(game)
		game.awayteam.games.add(game)
	return teams, games

def cleanbeforetime(time, teamsin, gamesin):
	teams = {team.teamid: Team(team.teamid, team.longname, shortname=team.shortname, isd1=team.isd1, conference=team.conference) for team in teamsin.values()}
	games = {game.gameid: Game(game.gameid, game.time, teams[game.hometeam.teamid], game.homescore, teams[game.awayteam.teamid], game.awayscore) for game in gamesin.values() if game.time >= time}
	for game in list(games.values()):
		game.hometeam.games.add(game)
		game.awayteam.games.add(game)
	return teams, games

def cleanaftertime(time, teamsin, gamesin):
	teams = {team.teamid: Team(team.teamid, team.longname, shortname=team.shortname, isd1=team.isd1, conference=team.conference) for team in teamsin.values()}
	games = {game.gameid: Game(game.gameid, game.time, teams[game.hometeam.teamid], game.homescore, teams[game.awayteam.teamid], game.awayscore) for game in gamesin.values() if game.time < time}
	for game in list(games.values()):
		game.hometeam.games.add(game)
		game.awayteam.games.add(game)
	return teams, games

def numplayed(team1, team2):
	return len(team1.games.intersection(team2.games))

def sos(krachratings, team):
	return gmean([krachratings[team.opponent(game).teamid] for game in team.games if team.opponent(game).isd1])

'''def sos(krachratings, team):
	return sum(krachratings[opponent.teamid]*numplayed(team, opponent)/(krachratings[opponent.teamid]+krachratings[team.teamid]) for opponent in team.D1opponents())/sum(numplayed(team, opponent)/(krachratings[opponent.teamid]+krachratings[team.teamid]) for opponent in team.D1opponents())'''

def conferencestrength(krachratings, teams, conference):
	return gmean([krachratings[team.teamid] for team in teams.values() if team.conference == conference])

def victorypoints(team, alpha):
	D1winslosses = team.D1winslosses()
	return sum([1/(1+math.exp(-game.pointdifferential()/(alpha*game.pointtotal()))) for game in D1winslosses[0]]+[1/(1+math.exp(game.pointdifferential()/(alpha*game.pointtotal()))) for game in D1winslosses[1]])

def rrwp(krachratings, team):
	teamrating = krachratings[team.teamid]
	return sum([teamrating/(teamrating+krachratings[opponentid]) for opponentid in krachratings if opponentid != team.teamid])/(len(krachratings)-1)

def rrwpkrach(krachratings, rating):
	return sum([rating/(rating+opprating) for opprating in krachratings.values()])/len(krachratings)

def krachadj(krachratings):
	return scipy.optimize.fsolve(lambda rating: rrwpkrach(krachratings, rating)-0.5, 100)[0]

def calckrachratings(teamsin, gamesin, vpalpha=5, goaldelta=1e-10, time=None, sincetime=None, savefile=None, calcteamsos=True):
	teams, games = cleanexistingties(teamsin, gamesin)
	if time:
		teams, games = cleanaftertime(time, teams, games)
	if sincetime:
		teams, games = cleanbeforetime(time, teams, games)
	D1teams = {team.teamid: team for team in teams.values() if team.isd1}
	
	krachratings = {teamid: 100. for teamid, team in D1teams.items() if team.isd1}
	
	iterations = 0
	alphaadj = vpalpha/mean([game.pointtotal() for game in games.values() if game.hometeam.isd1 and game.awayteam.isd1])
	victorypoints_dict = dict()
	D1opponents_dict = dict()
	numplayed_dict = dict()
	for team in D1teams.values():
			victorypoints_dict[team] = victorypoints(team, alphaadj)
			D1opponents = team.D1opponents()
			D1opponents_dict[team] = D1opponents
			for opponent in D1opponents:
				numplayed_dict[frozenset({team, opponent})] = numplayed(team, opponent)
	while True:
		print('Iteration '+str(iterations+1))
		newkrachratings = dict(krachratings)
		delta = 0.
		
		for team in D1teams.values():
			newkrachratings[team.teamid] = victorypoints_dict[team]/sum(numplayed_dict[frozenset({team, opponent})]/(krachratings[team.teamid]+krachratings[opponent.teamid]) for opponent in D1opponents_dict[team])
			delta += abs(newkrachratings[team.teamid]-krachratings[team.teamid])
		
		krachratings = dict(newkrachratings)
		
		print(delta)
		if delta < goaldelta*gmean(list(krachratings.values())):
			adj = krachadj(krachratings)
			krachratings = {k: v*100/adj for k, v in krachratings.items()}
			break
		
		iterations += 1
	if calcteamsos:
		teamsos = {team.teamid: sos(krachratings, team) for team in D1teams.values()}
	if savefile:
		if calcteamsos:
			pickle.dump([krachratings, teamsos], open(savefile,'wb'))
		else:
			pickle.dump(krachratings, open(savefile,'wb'))
	if calcteamsos:
		return krachratings, teamsos
	else:
		return krachratings

def calckrachratingsold(teamsin, gamesin, goaldelta=1e-10, time=None, savefile=None, calcteamsos=True):
	teams, games = cleanexistingties(teamsin, gamesin)
	if time:
		teams, games = cleanaftertime(time, teams, games)
	D1teams = {team.teamid: team for team in teams.values() if team.isd1}
	
	if [team for team in D1teams.values() if (not team.D1winslosses()[0] or not team.D1winslosses()[1])]:
		needbogusteam = True
	else:
		needbogusteam = False
	krachratings = {teamid: 100. for teamid, team in D1teams.items() if team.isd1}
	if needbogusteam:
		krachratings[-1] = 100.
	
	iterations = 0
	numwins_dict = dict()
	D1opponents_dict = dict()
	numplayed_dict = dict()
	for team in D1teams.values():
		numwins_dict[team] = len(team.D1winslosses()[0])
		D1opponents = team.D1opponents()
		D1opponents_dict[team] = D1opponents
		for opponent in D1opponents:
			numplayed_dict[frozenset({team, opponent})] = numplayed(team, opponent)
	while True:
		print('Iteration '+str(iterations+1))
		newkrachratings = dict(krachratings)
		delta = 0.
		
		if not needbogusteam:
			for team in D1teams.values():
				newkrachratings[team.teamid] = numwins_dict[team]/sum(numplayed_dict[frozenset({team, opponent})]/(krachratings[team.teamid]+krachratings[opponent.teamid]) for opponent in D1opponents_dict[team])
				delta += abs(newkrachratings[team.teamid]-krachratings[team.teamid])
		if needbogusteam:
			for team in D1teams.values():
				newkrachratings[team.teamid] = (numwins_dict[team]+0.5)/(1/(krachratings[team.teamid]+krachratings[-1])+sum(numplayed_dict[frozenset({team, opponent})]/(krachratings[team.teamid]+krachratings[opponent.teamid]) for opponent in D1opponents_dict[team]))
				delta += abs(newkrachratings[team.teamid]-krachratings[team.teamid])
			newkrachratings[-1] = (len(D1teams)/2)/sum(1/(krachratings[team.teamid]+krachratings[-1]) for team in D1teams.values())
		
		krachratings = dict(newkrachratings)
		
		print(delta)
		if delta < goaldelta:
			krachratings = {k: v*100./krachratings[-1] for k, v in krachratings.items()}
			break
		
		iterations += 1
	if calcteamsos:
		teamsos = {team.teamid: sos(krachratings, team) for team in D1teams.values()}
	if savefile:
		if calcteamsos:
			pickle.dump([krachratings, teamsos], open(savefile,'wb'))
		else:
			pickle.dump(krachratings, open(savefile,'wb'))
	if calcteamsos:
		return krachratings, teamsos
	else:
		return krachratings
