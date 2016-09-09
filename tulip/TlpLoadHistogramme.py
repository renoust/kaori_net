from tulip import *
from tulipgui import *
import tulipplugins
import json
import os
from py2neo.types import remote
import py2neo

texture_path = ""
DEFAULT_FRAMERATE = ""

id_neo = ""
password_neo = ""
serveur_neo = ""
url_neo = ""

from configTulip_file import *

class Main(tlp.Algorithm):
	def __init__(self, context):
		tlp.Algorithm.__init__(self, context)
		# You can add parameters to the plugin here through the following syntax:
		# self.add<Type>Parameter("<paramName>", "<paramDoc>", "<paramDefaultValue>")
		# (see the documentation of class tlp.WithParameter to see what parameter types are supported).
		self.addBooleanParameter("Search_by_Time","Do you want a research between two dates or not ?", "True")
		self.addBooleanParameter("showInvalidated","True if you want to show what has been Invalidated ?", "False")
		
		self.addStringParameter("Year Start","Set the year of the Start Date","2012")
		self.addStringParameter("Month Start","Set the month of the Start Date","7")
		self.addStringParameter("Day Start","Set the day of the Start Date","27")
		
		self.addStringParameter("Year End","Set the year of the End Date","2012")
		self.addStringParameter("Month End","Set the month of the End Date","8")
		self.addStringParameter("Day End","Set the day of the End Date","12")

	def check(self):
		parameters = self.dataSet
		launchAlgo = True
		error = ""
		if parameters["Search_by_Time"] is True:
			if ((str(parameters["Year Start"]).isdigit() or str(parameters["Year Start"]) == "") and (str(parameters["Month Start"]).isdigit() or str(parameters["Month Start"]) == "") and (str(parameters["Day Start"]).isdigit() or str(parameters["Day Start"]) == "") and (str(parameters["Year End"]).isdigit() or str(parameters["Year End"]) == "") and (str(parameters["Month End"]).isdigit() or str(parameters["Month End"]) == "")  and (str(parameters["Day End"]).isdigit() or str(parameters["Day End"]) == "") ) is False or ( str(parameters["Year Start"]) == "" or (str(parameters["Month Start"]) == "" and str(parameters["Day Start"]) != "") or str(parameters["Year End"]) == "" or (str(parameters["Month End"]) == "" and str(parameters["Day End"]) != "") ):
					launchAlgo = False
					error = "Dates Entry must be digit"
		return (launchAlgo, error)

	def run(self):
		parameters = self.dataSet
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
		
		Search_by_Time = parameters["Search_by_Time"]
		showInvalidated = parameters["showInvalidated"]
		Rootgraph = self.graph.getRoot()
		statementWhere = ""
		if dateStart != [] or dateEnd != []:
			statementWhere = ' ('
		if dateStart != []:
			if len(dateStart) == 1:
				statementWhere += 'year.name >= ' + dateStart[0]
			elif len(dateStart) > 1:
				statementWhere += 'year.name > ' + dateStart[0]
				if len(dateStart) == 2:
					statementWhere += ' OR (year.name = ' + dateStart[0] + ' AND month.name >= ' + dateStart[1] +')'
				elif len(dateStart) > 2:
					statementWhere += ' OR (year.name = ' + dateStart[0] + ' AND month.name > ' + dateStart[1] +')'
					statementWhere += ' OR (year.name = ' + dateStart[0] + ' AND month.name = ' + dateStart[1] + ' AND day.name >= ' + dateStart[2] + ')'
			statementWhere += ')'
			if dateEnd != []:
				statementWhere += ' AND ('
		if dateEnd != []:
			if len(dateEnd) == 1:
				statementWhere += 'year.name <= ' + dateEnd[0]
			elif len(dateEnd) > 1:
				statementWhere += 'year.name < ' + dateEnd[0]
				if len(dateEnd) == 2:
					statementWhere += ' OR (year.name = ' + dateEnd[0] + ' AND month.name <= ' + dateEnd[1] +')'
				elif len(dateEnd) > 2:
					statementWhere += ' OR (year.name = ' + dateEnd[0] + ' AND month.name < ' + dateEnd[1] +')'
					statementWhere += ' OR (year.name = ' + dateEnd[0] + ' AND month.name = ' + dateEnd[1] + ' AND day.name <= ' + dateEnd[2] + ')'
			statementWhere += ')'
		
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		NumIdSubgraph = self.graph.getDoubleProperty("Num Id Subgraph")
		IdNeo4jMeta = self.graph.getStringProperty("Neo4jId")
		AirtimeMeta = self.graph.getDoubleProperty("Airtime")
		NumClusterMeta = self.graph.getStringProperty("Num Cluster")
		for Metanode in viewSelection.getNodesEqualTo(True):
			if self.graph.isMetaNode(Metanode):
				idMetaNode = IdNeo4jMeta[Metanode]
				subgraph = self.graph.getNodeMetaInfo(Metanode)
				subgraph.clear()
				TypeNode = subgraph.getStringProperty("Type Node")
				Namesubgraph = subgraph.getStringProperty("Name")
				
				viewBorderColor = subgraph.getColorProperty("viewBorderColor")
				ColorBorderLigne = tlp.Color(20,0,0) #darkRed
				
				
				viewSize = subgraph.getSizeProperty("viewSize")
				SizeTracks = tlp.Size(50,50,30)
				SizeYear = tlp.Size(80,40,30)
				
				viewTexture = subgraph.getStringProperty("viewTexture")
				
				viewShape = subgraph.getIntegerProperty("viewShape")
				ShapeTracks = tlp.NodeShape.Square
				ShapeYear = tlp.NodeShape.Square
				
				viewColor = subgraph.getColorProperty("viewColor")
				ColorTracks = tlp.Color(255,255,255) #white
				ColorYear = tlp.Color(255,255,255)
				
				viewLayout =  subgraph.getLayoutProperty("viewLayout")
				IdNeo4j = subgraph.getStringProperty("Neo4jId")
				
				viewLabel = subgraph.getStringProperty("viewLabel")
			
				viewLabelColor = subgraph.getColorProperty("viewLabelColor")
				ColorLabelPerson = tlp.Color(181,181,181)
				
				viewBorderColor = subgraph.getColorProperty("viewBorderColor")
				ColorBorderUnknow = tlp.Color(127,127,127)
				ColorBorderValide = tlp.Color(0,86,27)
				ColorBorderInvalide = tlp.Color(187,11,11)
				
				viewBorderWidth = subgraph.getDoubleProperty("viewBorderWidth")
				BorderWidthTrack = 10
				
				SizePicture = 50.0
				SizeInterPicture = 4.0
				SizeInterYear = 40.0
				
				py2neo.authenticate(serveur_neo, id_neo, password_neo)
				graphNeo4j = py2neo.Graph(url_neo)
				tx = graphNeo4j.begin()
				
				id_cluster = IdNeo4jMeta[Metanode]
				statement = 'MATCH (clus) '\
							'WHERE ID(clus) =' + str(id_cluster) + \
							' MATCH (clus)-[r1:CLUSTERED]->(:Track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) '
				if (not showInvalidated) or Search_by_Time:
					statement += 'WHERE '
				if not showInvalidated:
					statement += '(r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") '
				if (not showInvalidated) and Search_by_Time:
					statement += 'AND '
				if Search_by_Time:
					statement += statementWhere
				statement += ' RETURN r1,day.name,month.name,year.name'
				print statement
				cursor = tx.run(statement)
				
				dict_track = {}
				airtime = 0
				for record in cursor:
					print record
					#record is like : (u'r1': (cb7abbc)-[:CLUSTERED {isValidated:"Unknow"}]->(b2fd780), u'day': (afb2cbf:Day {name:25}), u'month': (fa8dfb0:Month {name:2}), u'year': (add61c4:Year {name:2009}))
					#"Unknow" = dict(record[0]).values()[0]
					statut_of_track = str(dict(record[0]).values()[0])
					infoTrack = []
					TrackNeo4j = record[0].end_node()
					#dict(record[0].end_node()) is like {u'last_frame': 12138, u'first_frame': 12065, u'num_track': 1, u'num_shot': 49}
					print TrackNeo4j
					infos = dict(TrackNeo4j)
					airtime += infos["last_frame"] - infos["first_frame"]
					infoTrack.append(str(infos["num_shot"]))
					infoTrack.append(str(infos["num_track"]))
					infoTrack.append(str(record[1]))
					infoTrack.append(str(record[2]))
					infoTrack.append(str(record[3]))
					infoTrack.append(remote(TrackNeo4j)._id)
					infoTrack.append(str(dict(record[0])["isValidated"]))
					#infoTrack is like : ['76', '1', '27', '9', '2008',1876239,Unknow]numshot,numtrack,day,month,year,id,state
					if infoTrack[4] not in dict_track:
						dict_track[infoTrack[4]] = []
					dict_track[infoTrack[4]].append(infoTrack)
				
				
				#nb_year = len(dict_track)
				
				tab_tracks = []
				for year, tracks in dict_track.iteritems():
					list_year_tracks = [year]
					tracks.sort(key=lambda track: [int(track[3]),int(track[2])])
					list_year_tracks.append(tracks)
					tab_tracks.append(list_year_tracks)
				tab_tracks.sort(key=lambda year: int(year[0]))
				#We have an array tab_tracks with all the tracks sorted by year, month, day
			
			
				ReferencePositionX = - SizePicture - SizeInterPicture
				ReferencePositionY = 0.0
				
				nb_tracks = 0
				for year_tracks in tab_tracks:
					ReferencePositionDebutYearX = ReferencePositionX
					ReferencePositionDebutYearY = ReferencePositionY
					HauteurTrack = 0
					MaxHauteur = 1
					
					year = year_tracks[0]
					tracks = year_tracks[1]
					
					f = open(texture_path + "image_track_" + str(year) +".json")
					json_texture_year = json.load(f) #we load the annotation data.
					f.close()
					
					
					previous_day = "0"
					previous_month = "0"
					for track in tracks:
						nb_tracks += 1
						if track[2] == previous_day and track[3] == previous_month:
							HauteurTrack += 1
							ReferencePositionY += SizePicture + SizeInterPicture
						else:
							if HauteurTrack > MaxHauteur:
								MaxHauteur = HauteurTrack
							HauteurTrack = 1
							
							ReferencePositionX += SizePicture + SizeInterPicture
							ReferencePositionY = ReferencePositionDebutYearY
						previous_day = track[2]
						previous_month = track[3]
						
						node_track = Rootgraph.addNode()
						subgraph.addNode(node_track)
						viewSize[node_track] = SizeTracks
						viewShape[node_track] = ShapeTracks
						viewColor[node_track] = ColorTracks
						IdNeo4j[node_track] = str(track[5])

						TypeNode[node_track] = "Track"
						viewBorderWidth[node_track] = BorderWidthTrack
						if track[6] == "Unknow":
							viewBorderColor[node_track] = ColorBorderUnknow
						elif track[6] == "Valide":
							viewBorderColor[node_track] = ColorBorderValide
						elif track[6] == "NotValide":
							viewBorderColor[node_track] = ColorBorderInvalide
						
						
						day = track[2]
						if len(day) == 1:
							day = "0" + day
							
						month = track[3]
						if len(month) == 1:
							month = "0" + month
							
						num_shot = track[0]
						num_track = track[1]
						Namesubgraph[node_track] = day + "/" + month + "/" + year
						
						nb_cluster = NumClusterMeta[Metanode]
						
						image_str = json_texture_year[month + "_" + day][num_shot + "_" + num_track]
						image_file = open("temp_image/image_Hist_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(nb_cluster) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg","wb")
						image_file.write(image_str.decode('base64'))
						image_file.close()
						
						path_image = "temp_image/image_Hist_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(nb_cluster) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg"
						viewTexture[node_track] = path_image
						viewLayout[node_track] = tlp.Coord(ReferencePositionX + float(SizePicture / 2.0),ReferencePositionY + float(SizePicture / 2.0),0)
						
					node_date = Rootgraph.addNode()
					subgraph.addNode(node_date)
					viewSize[node_date] = SizeYear
					viewShape[node_date] = ShapeYear
					viewColor[node_date] = ColorYear
					viewLabel[node_date] = year
					viewLayout[node_date] = tlp.Coord((ReferencePositionX + SizePicture + ReferencePositionDebutYearX)/2.0 + 30.0, ReferencePositionDebutYearY - 80,0)
					
					ReferencePositionX = 0.0
					ReferencePositionY = ReferencePositionDebutYearY + float(MaxHauteur)*(float(SizePicture) + float(SizeInterPicture)) + 115.0
				AirtimeMeta[Metanode] = airtime
	
		
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jLoadHistogramme", "Adrien", "24/08/2016", "Neo4JCluster", "1.0")
