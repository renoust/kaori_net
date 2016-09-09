from tulip import *
import tulipplugins
from tulipgui import *
import py2neo
from py2neo.types import remote
import json
import os

id_neo = ""
password_neo = ""
serveur_neo = ""
url_neo = ""

from configTulip_file import *


class Main(tlp.Algorithm):
	def __init__(self, context):
		tlp.Algorithm.__init__(self, context)
		
		self.addBooleanParameter("showInvalidated","True if you want to show what has been Invalidated ?", "False")
		
		self.addStringParameter("Year Start","Set the year of the Start Date","2011")
		self.addStringParameter("Month Start","Set the month of the Start Date","4")
		self.addStringParameter("Day Start","Set the day of the Start Date","15")
		
		self.addStringParameter("Year End","Set the year of the End Date","2011")
		self.addStringParameter("Month End","Set the month of the End Date","5")
		self.addStringParameter("Day End","Set the day of the End Date","15")

	def check(self):
		parameters = self.dataSet
		launchAlgo = True
		error = ""
		if ((str(parameters["Year Start"]).isdigit() or str(parameters["Year Start"]) == "") and (str(parameters["Month Start"]).isdigit() or str(parameters["Month Start"]) == "") and (str(parameters["Day Start"]).isdigit() or str(parameters["Day Start"]) == "") and (str(parameters["Year End"]).isdigit() or str(parameters["Year End"]) == "") and (str(parameters["Month End"]).isdigit() or str(parameters["Month End"]) == "")  and (str(parameters["Day End"]).isdigit() or str(parameters["Day End"]) == "") ) is False or ( str(parameters["Year Start"]) == "" or (str(parameters["Month Start"]) == "" and str(parameters["Day Start"]) != "") or str(parameters["Year End"]) == "" or (str(parameters["Month End"]) == "" and str(parameters["Day End"]) != "") ):
				launchAlgo = False
				error = "Dates Entry must be digit"
		return (launchAlgo, error)
	def run(self):
		parameters = self.dataSet
		showInvalidated = parameters["showInvalidated"]
		dateStart = []
		YearStart = parameters["Year Start"]
		if YearStart != "":
			YearStart = str(int(YearStart))
			dateStart.append(YearStart)
		
		MonthStart = parameters["Month Start"]
		if MonthStart != "":
			MonthStart = str(int(MonthStart))
			dateStart.append(MonthStart)
		
		DayStart = parameters["Day Start"]
		if DayStart != "":
			DayStart = str(int(DayStart))
			dateStart.append(DayStart)
		
		
		dateEnd = []
		YearEnd = parameters["Year End"]
		if YearEnd != "":
			YearEnd = str(int(YearEnd))
			dateEnd.append(YearEnd)
		
		MonthEnd = parameters["Month End"]
		if MonthEnd != "":
			MonthEnd = str(int(MonthEnd))
			dateEnd.append(MonthEnd)
		
		DayEnd = parameters["Day End"]
		if DayEnd != "":
			DayEnd = str(int(DayEnd))
			dateEnd.append(DayEnd)
		
		def printStatementWhere(num):
			statementWhere = ""
			if dateStart != [] or dateEnd != []:
				statementWhere = ' ('
			if dateStart != []:
				if len(dateStart) == 1:
					statementWhere += 'year' + num + '.name >= ' + dateStart[0]
				elif len(dateStart) > 1:
					statementWhere += 'year' + num + '.name > ' + dateStart[0]
					if len(dateStart) == 2:
						statementWhere += ' OR (year' + num + '.name = ' + dateStart[0] + ' AND month' + num + '.name >= ' + dateStart[1] +')'
					elif len(dateStart) > 2:
						statementWhere += ' OR (year' + num + '.name = ' + dateStart[0] + ' AND month' + num + '.name > ' + dateStart[1] +')'
						statementWhere += ' OR (year' + num + '.name = ' + dateStart[0] + ' AND month' + num + '.name = ' + dateStart[1] + ' AND day' + num + '.name >= ' + dateStart[2] + ')'
				statementWhere += ')'
				if dateEnd != []:
					statementWhere += ' AND ('
			if dateEnd != []:
				if len(dateEnd) == 1:
					statementWhere += 'year' + num + '.name <= ' + dateEnd[0]
				elif len(dateEnd) > 1:
					statementWhere += 'year' + num + '.name < ' + dateEnd[0]
					if len(dateEnd) == 2:
						statementWhere += ' OR (year' + num + '.name = ' + dateEnd[0] + ' AND month' + num + '.name <= ' + dateEnd[1] +')'
					elif len(dateEnd) > 2:
						statementWhere += ' OR (year' + num + '.name = ' + dateEnd[0] + ' AND month' + num + '.name < ' + dateEnd[1] +')'
						statementWhere += ' OR (year' + num + '.name = ' + dateEnd[0] + ' AND month' + num + '.name = ' + dateEnd[1] + ' AND day' + num + '.name <= ' + dateEnd[2] + ')'
				statementWhere += ')'
			return statementWhere
		statementWhere = printStatementWhere("")
		
		py2neo.authenticate(serveur_neo, id_neo, password_neo)
		graphNeo4j = py2neo.Graph(url_neo)
		tx = graphNeo4j.begin()
		
		dict_cluster = {}
		#We load what it is labeled
		statement = ' MATCH (lab:Label)<-[:LABEL]-(clus:User_Defined_Cluster)-[r:CLUSTERED]->(tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE ' + statementWhere
		if not showInvalidated:
			statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND NOT (clus)-[:INVALIDATED_BY]->() '
		statement += 'WITH lab AS l,clus AS c, tracks AS tr, day AS d,month AS m,year AS y '\
							' RETURN DISTINCT [l,tr,d,m,y] '\
							'UNION '\
							'MATCH (lab:Label)<-[:LABEL]-(clus:Cluster)-[r:CLUSTERED]->(tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE '\
							+ statementWhere + ' AND NOT( (clus)<-[:CLUSTERED]-()) '
		if not showInvalidated:
				statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND NOT (clus)-[:INVALIDATED_BY]->() '
		statement += 'WITH lab AS l,clus AS c, tracks AS tr, day AS d,month AS m,year AS y '\
						' RETURN DISTINCT [l,tr,d,m,y] '\
							
		cursorClusterLabeled = tx.run(statement)
		for record in cursorClusterLabeled:
			label = str(dict(record[0][0])["name"])
			day = int(dict(record[0][2])["name"])
			month = int(dict(record[0][3])["name"])
			year = int(dict(record[0][4])["name"])
			num_track = int(dict(record[0][1])["num_track"])
			num_shot = int(dict(record[0][1])["num_shot"])
			
			#infos = year + "_" + month + "_" + day + "_" + num_shot + "_" + num_track
			infos = "%04d_%02d_%02d_19_00-shot%02d_%d-Track%d"%(year,month,day,day,num_shot,num_track)
			dict_cluster[infos] = label
		
		
		
		#We load what it is not labeled
		statement = ' MATCH (clus:User_Defined_Cluster)-[r:CLUSTERED]->(tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE ' + statementWhere + ' AND NOT ((:Label)<-[:LABEL]-(clus)) '
		if not showInvalidated:
			statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND NOT (clus)-[:INVALIDATED_BY]->() '
		statement += 'WITH clus AS c, tracks AS tr, day AS d,month AS m,year AS y '\
							' RETURN DISTINCT [ID(c),tr,d,m,y] '\
							'UNION '\
							'MATCH (clus:Cluster)-[r:CLUSTERED]->(tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE '\
							+ statementWhere + ' AND NOT( (clus)<-[:CLUSTERED]-()) AND NOT ((:Label)<-[:LABEL]-(clus)) '
		if not showInvalidated:
				statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND NOT (clus)-[:INVALIDATED_BY]->() '
		statement += 'WITH clus AS c, tracks AS tr, day AS d,month AS m,year AS y '\
						' RETURN DISTINCT [ID(c),tr,d,m,y] '\
		
		cursorClusterNotLabeled = tx.run(statement)
		dict_listCluster = {}
		nb_cluster = 0
		for record in cursorClusterNotLabeled:
			idClus = str(record[0][0])
			if idClus not in dict_listCluster:
				nb_cluster += 1
				dict_listCluster[idClus] = nb_cluster
			num_clus = dict_listCluster[idClus]
			
			day = int(dict(record[0][2])["name"])
			month = int(dict(record[0][3])["name"])
			year = int(dict(record[0][4])["name"])
			num_track = int(dict(record[0][1])["num_track"])
			num_shot = int(dict(record[0][1])["num_shot"])
			
			#infos = year + "_" + month + "_" + day + "_" + num_shot + "_" + num_track
			infos = "%04d_%02d_%02d_19_00-shot%02d_%d-Track%d"%(year,month,day,day,num_shot,num_track)
			
			dict_cluster[infos] = "Cluster " + str(num_clus)
		
		
		namefile = ""
		for d in dateStart:
			namefile += d + "_"
		namefile += "to_"
		for d in dateEnd:
			namefile += d + "_"
		namefile = namefile[0:-1]
		with open('Resultat_cluster/' + namefile + '.json', 'w') as f:
			json.dump(dict_cluster, f, indent=4,sort_keys=True)
		f.close()
		
		
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "neo4j Clusters to file", "Adrien", "07/09/2016", "", "1.0")
