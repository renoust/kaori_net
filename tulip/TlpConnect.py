from tulip import *
import tulipplugins
from tulipgui import *
import py2neo
from py2neo.types import remote
import json
import os
from sets import ImmutableSet

# -*-coding:Utf-8 -*

id_neo = ""
password_neo = ""
serveur_neo = ""
url_neo = ""

texture_path = ""

from configTulip_file import *

class Main(tlp.Algorithm):
	def __init__(self, context):
		tlp.Algorithm.__init__(self, context)
		# You can add parameters to the plugin here through the following syntax:
		# self.add<Type>Parameter("<paramName>", "<paramDoc>", "<paramDefaultValue>")
		# (see the documentation of class tlp.WithParameter to see what parameter types are supported).
		self.addBooleanParameter("Search_by_Time","Do you want a research between two dates or not ?", "True")
		self.addStringParameter("Name","If Search_by_Time is False, choose someone", "Barak OBAMA")
		self.addIntegerParameter("Creation of a Metanode", "The Number of Tracks until we must create a Metanode. Usefull if Search_by_Time is False","5")
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
		if parameters["Search_by_Time"] is False:
			if parameters["Name"] == "":
				launchAlgo = False
				error = "Missing name"
		return (launchAlgo, error)
	
	def run(self):
		parameters = self.dataSet
		os.system("rm temp_image/*")
		
		name_cluster = parameters["Name"]
		
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
		dict_texture = {}
		def create_meta_node_cluster(graphNeo4j, graphTlp, main_sub, clusterNeo4j,showInvalidated,nb_cluster):
			tx = graphNeo4j.begin()
			statement = 'MATCH (clus) '\
							'WHERE ID(clus)=' + str(remote(clusterNeo4j)._id) + \
							' MATCH (clus)-[:LABEL]->(lab:Label) '\
							'RETURN lab'
			cursor = tx.run(statement)
			label = ""
			for record in cursor:
				if record is not None:
					label = str(dict(record[0])["name"])
	
			#We count the number of tracks
			statement = 'MATCH (clus) '\
							'WHERE ID(clus) =' + str(remote(clusterNeo4j)._id) + \
							' MATCH (clus)-[r1:CLUSTERED]->(track:Track) '
			if Search_by_Time:
				statement += 'MATCH (track:Track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) '

			if Search_by_Time or (not showInvalidated):
						statement += 'WHERE '
			if not showInvalidated:
						statement += ' (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") '
			if Search_by_Time and (not showInvalidated):
						statement += 'AND '
			if Search_by_Time:
						statement += statementWhere
			statement += ' RETURN count(DISTINCT track) '
	
			record = graphNeo4j.run(statement)
			nb_tracks = record.evaluate(0)
			
			statement = 'MATCH (clus) '\
							'WHERE ID(clus) =' + str(remote(clusterNeo4j)._id) + \
							' RETURN clus.texture'
			image_str = graphNeo4j.run(statement).evaluate(0)
			
			
			subgraph = graphTlp.addSubGraph(str(nb_cluster))				
				
			#The subgraph becomes a metanode
			meta_node = main_sub.createMetaNode(subgraph)
			nodeId = meta_node.id
			
			image_file = open("temp_image/image_Texture_" + str(nodeId) + ".jpg","wb")
			image_file.write(image_str.decode('base64'))
			image_file.close()
			path_image = "temp_image/image_Texture_" + str(nodeId) + ".jpg"
			
			Nb_Track[meta_node] = nb_tracks
			if label != "":
				viewLabel[meta_node] = label
				ShapeMetanode = ShapeMetanodeAnnote
			else:
				ShapeMetanode = ShapeMetanodeNotAnnote
				
			viewTexture[meta_node] = path_image
			
			viewLabelPosition[meta_node] = LabelPositionMetanode

			viewShape[meta_node] = ShapeMetanode
			tx.commit()
			return meta_node
		

		self.graph.clear()
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
		
		nb_cluster = 0
		
		main_sub = self.graph.addSubGraph("Main")
		tlpgui.closeViewsRelatedToGraph(self.graph)
		viewNodeLinkDiagram = tlpgui.createView("Node Link Diagram view", main_sub)
		renderingParameters = viewNodeLinkDiagram.getRenderingParameters()
		renderingParameters.setLabelScaled(True)
		renderingParameters.setDisplayMetaNodes(True)
		renderingParameters.setEdgeSizeInterpolate(False)
		viewNodeLinkDiagram.setRenderingParameters(renderingParameters)
		viewNodeLinkDiagram.centerView()
		
		viewTexture = main_sub.getStringProperty("viewTexture")
		viewSize = main_sub.getSizeProperty("viewSize")
		viewSizeTemp = main_sub.getSizeProperty("viewSizeTemp")
		viewLabel = main_sub.getStringProperty("viewLabel")
		
		viewLabelPosition = main_sub.getIntegerProperty("viewLabelPosition")
		LabelPositionMetanode = tlp.LabelPosition.Bottom
		
		viewColor = main_sub.getColorProperty("viewColor")
		ColorMetanode = tlp.Color(255,255,255,255)
		ColorEdge_over = tlp.Color(156,9,126)
		ColorEdge_tracks = tlp.Color(104,163,222)
		ColorTimeline = tlp.Color(6, 119, 144)
		ColorYear = tlp.Color(136, 77, 167)
		ColorMonth = tlp.Color(0, 142, 142)
		ColorDay = tlp.Color(44,117,255)
		ColorTopic = tlp.Color(247, 35, 12)
		ColorDiamond = tlp.Color(255,140,0)
		
		
		#viewIcon = main_sub.getStringProperty("viewFontAwesomeIcon")
		#IconTrack = tlp.TulipFontAwesome.Map
		
		viewShape = main_sub.getIntegerProperty("viewShape")
		ShapeMetanodeAnnote = tlp.NodeShape.Square
		ShapeMetanodeNotAnnote = tlp.NodeShape.Circle
		ShapeTracks = tlp.NodeShape.Square
		ShapeDiamond = tlp.NodeShape.Diamond
		
		viewBorderColor = main_sub.getColorProperty("viewBorderColor")
		ColorBorderUnknow = tlp.Color(127,127,127)
		ColorBorderValide = tlp.Color(0,86,27)
		ColorBorderInvalide = tlp.Color(187,11,11)
		
		viewBorderWidth = main_sub.getDoubleProperty("viewBorderWidth")
		BorderWidthCluster = 10
		
		viewLayout =  main_sub.getLayoutProperty("viewLayout")
		
		Airtime = main_sub.getDoubleProperty("Airtime")
		Nb_Track = main_sub.getDoubleProperty("Nb Tracks")
		IdNeo4j = main_sub.getStringProperty("Neo4jId")
		RootIdNeo4j = self.graph.getStringProperty("Neo4jId")
		NbOverlap = main_sub.getDoubleProperty("Nb Overlap")
		TypeNode = main_sub.getStringProperty("Type Node")
		ListTrackInMetaNode = main_sub.getStringVectorProperty("List Track")
		
		list_cluster_to_display = []
		list_tracks_to_display=[]
		dict_Neo4j_Tlp = {}
		
		if Search_by_Time :
			statement = ' MATCH (clus:User_Defined_Cluster)-[r:CLUSTERED]->(tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE ' + statementWhere
			if not showInvalidated:
					statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND NOT (clus)-[:INVALIDATED_BY]->() '
			statement += 'WITH clus AS c, top AS tops, day AS d,month AS m,year AS y '\
							' RETURN DISTINCT [c,tops,d,m,y] '\
							'UNION '\
							'MATCH (clus:Cluster)-[r:CLUSTERED]->(tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE '\
							+ statementWhere + ' AND NOT( (clus)<-[:CLUSTERED]-()) '
			if not showInvalidated:
					statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND NOT (clus)-[:INVALIDATED_BY]->() '
			statement += 'WITH clus AS c, top AS tops, day AS d,month AS m,year AS y '\
								'RETURN DISTINCT [c,tops,d,m,y]'
			cursorCluster = tx.run(statement)
			
			statement = ' MATCH (track:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE ' + statementWhere + ' AND NOT( (track)<-[:CLUSTERED]-() ) AND (top)-[:TRACK]->(:Track)<-[:CLUSTERED]-() ' \
								'WITH DISTINCT track AS tracks, top AS tops, day AS d,month AS m,year AS y '\
								'RETURN DISTINCT [tracks,tops,d,m,y] '
			cursor_Track_Time = tx.run(statement)
			
			
			list_topicNeo4j = []
			list_dayNeo4j = []
			list_track = []
			#We create day,topics and tracks when there are tracks near a cluster
			for record in cursor_Track_Time:
				topic = record[0][1]
				day = record[0][2]
				track = record[0][0]
				month = record[0][3]
				year = record[0][4]
				list_track.append(track)
				if day not in list_dayNeo4j:
					date = dict(day)["date"]
					list_dayNeo4j.append(day)
					day_node = main_sub.addNode()
					viewColor[day_node] = ColorDay
					TypeNode[day_node] = "Day"
					Nb_Track[day_node] = 3
					viewLabel[day_node] = str(date)
					IdNeo4j[day_node] = str(remote(day)._id)
					dict_Neo4j_Tlp[str(remote(day)._id)] = day_node
				else:
					day_node = dict_Neo4j_Tlp[str(remote(day)._id)]
				if topic not in list_topicNeo4j:
					num_top = dict(topic)["name"]
					list_topicNeo4j.append(topic)
					topic_node =  main_sub.addNode()
					viewColor[topic_node] = ColorTopic
					TypeNode[topic_node] = "Topic"
					IdNeo4j[topic_node] = str(remote(topic)._id)
					Nb_Track[topic_node] = 2
					viewLabel[topic_node] = "Topic : " + str(num_top)
					dict_Neo4j_Tlp[str(remote(topic)._id)] = topic_node
					edge_day = main_sub.addEdge(day_node,topic_node)
					Nb_Track[edge_day] = 1
				else:
					topic_node = dict_Neo4j_Tlp[str(remote(topic)._id)]
				track_node = main_sub.addNode()
				TypeNode[track_node] = "Track"
				IdNeo4j[track_node] = str(remote(track)._id)
				Nb_Track[track_node] = 1
				viewShape[track_node] = ShapeTracks
				list_track.append(track)
				dict_Neo4j_Tlp[str(remote(track)._id)] = day_node
				edge = main_sub.addEdge(topic_node,track_node)
				NbOverlap[edge] = 0.0
				Nb_Track[edge] = 1.0
				viewColor[track_node] = ColorMetanode
				
				year = str(dict(year)["name"])
				month = str(dict(month)["name"])
				day = str(dict(day)["name"])
				if len(day) == 1:
					day = "0" + day
				if len(month) == 1:
					month = "0" + month
				
				if year not in dict_texture:
					f = open(texture_path + "image_track_" + str(year) +".json")
					json_texture_year = json.load(f) 
					f.close()
					dict_texture[year] = json_texture_year
				else:
					json_texture_year = dict_texture[year]
				info_track = dict(track)
				num_shot = str(info_track["num_shot"])
				num_track = str(info_track["num_track"])
				
				image_str = json_texture_year[month + "_" + day][num_shot + "_" + num_track]
				image_file = open("temp_image/image_Connect_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg","wb")
				image_file.write(image_str.decode('base64'))
				image_file.close()
				
				path_image = "temp_image/image_Connect_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg"
				viewTexture[track_node] = path_image
			
			
			#We create day,topic,cluster
			for infoCluster in cursorCluster:
				clusterNeo4j = infoCluster[0][0]
				if clusterNeo4j not in list_cluster_to_display:
					list_cluster_to_display.append(clusterNeo4j)
					nb_cluster += 1
					meta_node = create_meta_node_cluster(graphNeo4j, self.graph, main_sub, clusterNeo4j,showInvalidated,nb_cluster)
					dict_Neo4j_Tlp[str(remote(clusterNeo4j)._id)] = meta_node
					if nb_cluster ==1:
						tlpMain_Node = meta_node
					nb_track = Nb_Track[meta_node]
					IdNeo4j[meta_node] = str(remote(clusterNeo4j)._id)
					RootIdNeo4j[meta_node] = str(remote(clusterNeo4j)._id)
					TypeNode[meta_node] = "Cluster"
					viewColor[meta_node] = ColorMetanode
	
					#Contour du node
					statement = 'MATCH (clus) ' \
									'WHERE ID(clus)=' + str(remote(clusterNeo4j)._id) + \
									' MATCH (clus)-[r:VALIDATED_BY|INVALIDATED_BY]-() '\
									'RETURN r'
					cursor = tx.run(statement)
					record = cursor.evaluate(0)
					viewBorderWidth[meta_node] = BorderWidthCluster
					if record is None:
						viewBorderColor[meta_node] = ColorBorderUnknow
					else:
						label = record.type()
						if label == "VALIDATED_BY":
							viewBorderColor[meta_node] = ColorBorderValide
						elif label == "INVALIDATED_BY":
							viewBorderColor[meta_node] = ColorBorderInvalide
				else:
					meta_node = dict_Neo4j_Tlp[str(remote(clusterNeo4j)._id)]
				
				topic = infoCluster[0][1]
				day = infoCluster[0][2]
				month = infoCluster[0][3]
				year = infoCluster[0][4]
				
				if day not in list_dayNeo4j:
					date = dict(day)["date"]
					list_dayNeo4j.append(day)
					day_node = main_sub.addNode()
					viewColor[day_node] = ColorDay
					TypeNode[day_node] = "Day"
					Nb_Track[day_node] = 3
					viewLabel[day_node] = str(date)
					IdNeo4j[day_node] = str(remote(day)._id)
					dict_Neo4j_Tlp[str(remote(day)._id)] = day_node
				else:
					day_node = dict_Neo4j_Tlp[str(remote(day)._id)]
				if topic not in list_topicNeo4j:
					num_top = dict(topic)["name"]
					list_topicNeo4j.append(topic)
					topic_node =  main_sub.addNode()
					viewColor[topic_node] = ColorTopic
					TypeNode[topic_node] = "Topic"
					IdNeo4j[topic_node] = str(remote(topic)._id)
					Nb_Track[topic_node] = 2
					viewLabel[topic_node] = "Topic : " + str(num_top)
					dict_Neo4j_Tlp[str(remote(topic)._id)] = topic_node
					edge_day = main_sub.addEdge(day_node,topic_node)
					Nb_Track[edge_day] = 1
				else:
					topic_node = dict_Neo4j_Tlp[str(remote(topic)._id)]
				edge = main_sub.addEdge(topic_node,meta_node)
				NbOverlap[edge] = 0
				Nb_Track[edge] = 1
			
			statement = 'MATCH (day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) WHERE ' + statementWhere + \
							' RETURN DISTINCT [day,month,year] '
			cursorTimeline = tx.run(statement)
			
			
			Timeline_node =  main_sub.addNode()
			TypeNode[Timeline_node] = "Timeline"
			Nb_Track[Timeline_node] = 8
			viewLabel[Timeline_node] = "Timeline"
			viewColor[Timeline_node] = ColorTimeline
			
			list_monthNeo4j = []
			list_yearNeo4j = []
			for coupleTime in cursorTimeline:
				day = coupleTime[0][0]
				month = coupleTime[0][1]
				year = coupleTime[0][2]
				if day not in list_dayNeo4j:
					if year not in list_yearNeo4j:
						list_yearNeo4j.append(year)
						date = dict(year)["name"]
						year_node = main_sub.addNode()
						viewColor[year_node] = ColorYear
						TypeNode[year_node] = "Year"
						Nb_Track[year_node] = 6
						viewLabel[year_node] = str(date)
						IdNeo4j[year_node] = str(remote(year)._id)
						dict_Neo4j_Tlp[str(remote(year)._id)] = year_node
						edge_year = main_sub.addEdge(year_node,Timeline_node)
						Nb_Track[edge_year] = 1
					else:
						year_node = dict_Neo4j_Tlp[str(remote(year)._id)]
						
					if month not in list_monthNeo4j:
						list_monthNeo4j.append(month)
						date = dict(month)["name"]
						month_node = main_sub.addNode()
						viewColor[month_node] = ColorMonth
						TypeNode[month_node] = "Month"
						Nb_Track[month_node] = 4
						viewLabel[month_node] = str(date)
						IdNeo4j[month_node] = str(remote(month)._id)
						dict_Neo4j_Tlp[str(remote(month)._id)] = month_node
						edge_month = main_sub.addEdge(year_node,month_node)
						Nb_Track[edge_month] = 1
					else:
						month_node = dict_Neo4j_Tlp[str(remote(month)._id)]
						
					date = dict(day)["date"]
					list_dayNeo4j.append(day)
					day_node = main_sub.addNode()
					viewColor[day_node] = ColorDay
					TypeNode[day_node] = "Day"
					Nb_Track[day_node] = 3
					viewLabel[day_node] = str(date)
					IdNeo4j[day_node] = str(remote(day)._id)
					dict_Neo4j_Tlp[str(remote(day)._id)] = day_node
					edge_day = main_sub.addEdge(month_node,day_node)
					Nb_Track[edge_day] = 1
				
			
			
			
			statementWhere1 = printStatementWhere("1")
			statementWhere2 = printStatementWhere("2")	
			
			#this create the links between cluster
			for clusterNeo4j_1 in list_cluster_to_display:
				for clusterNeo4j_2 in list_cluster_to_display:
					if list_cluster_to_display.index(clusterNeo4j_1) < list_cluster_to_display.index(clusterNeo4j_2):
						#We count how many overlap there is between this 2 clusters
						statement = 'MATCH (clus1) '\
										'WHERE ID(clus1)=' + str(remote(clusterNeo4j_1)._id) + \
										' MATCH (clus2) '\
										'WHERE ID(clus2)=' + str(remote(clusterNeo4j_2)._id) + \
										' MATCH (clus1)-[r1:CLUSTERED]->(tracks1:Track)-[overlap_rel:OVERLAP]-(tracks2:Track)<-[r2:CLUSTERED]-(clus2) '\
										'MATCH (tracks1:Track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day1:Day)<-[:DAY]-(month1:Month)<-[:MONTH]-(year1:Year) '\
										'MATCH (tracks2:Track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day2:Day)<-[:DAY]-(month2:Month)<-[:MONTH]-(year2:Year) '
						if (not showInvalidated):
							statement += 'WHERE (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") AND (r2.isValidated ="' + 'Unknow' + '" OR r2.isValidated ="' + 'Valide' + '") AND ' + statementWhere1 + ' AND ' + statementWhere2
						else:
							statement += 'WHERE ' + statementWhere1 + ' AND ' + statementWhere2
						statement += ' WITH count(DISTINCT overlap_rel) AS nb_overlap '\
											'RETURN nb_overlap'
						
						
						record = graphNeo4j.run(statement)
						nb_rel_over = record.evaluate(0)
						
						#We count how may tracks there is between the 2 clusters
						statement = 'MATCH (clus1) '\
										'WHERE ID(clus1)=' + str(remote(clusterNeo4j_1)._id) + \
										' MATCH (clus2) '\
										'WHERE ID(clus2)=' + str(remote(clusterNeo4j_2)._id) + \
										' MATCH (clus1)-[r1:CLUSTERED]->(tracks1:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(tracks2:Track)<-[r2:CLUSTERED]-(clus2) '\
										'MATCH (tracks1:Track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day1:Day)<-[:DAY]-(month1:Month)<-[:MONTH]-(year1:Year) '\
										'MATCH (tracks2:Track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day2:Day)<-[:DAY]-(month2:Month)<-[:MONTH]-(year2:Year) '
						if (not showInvalidated):
							statement += 'WHERE (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") AND (r2.isValidated ="' + 'Unknow' + '" OR r2.isValidated ="' + 'Valide' + '") AND ' + statementWhere1 + ' AND ' + statementWhere2
						else:
							statement += 'WHERE ' + statementWhere1 + ' AND ' + statementWhere2
						statement += 'WITH count(DISTINCT tracks1) AS nb1, count(DISTINCT tracks2) AS nb2 '\
										'RETURN [nb1,nb2]'
						record = graphNeo4j.run(statement)
						nb_info = record.evaluate(0)
						nb_track = nb_info[0] + nb_info[1]
						
						
						#We search the metanode
						for metanode in main_sub.getNodes():
							if IdNeo4j[metanode] == str(remote(clusterNeo4j_1)._id):
								meta1 = metanode
							elif IdNeo4j[metanode] == str(remote(clusterNeo4j_2)._id):
								meta2 = metanode					
						
						
						#We create edge between the two clusters
						if nb_track != 0:
							edge_tracks = main_sub.addEdge(meta1,meta2)
							NbOverlap[edge_tracks] = nb_rel_over
							Nb_Track[edge_tracks] = nb_track
		else:
			statement = 'MATCH (label:Label {name:"' + name_cluster + '"}) '\
				'RETURN label'
			cursor = tx.run(statement)
			record = cursor.evaluate()
			#We look if there is such a label
			if record is None:
				print "There is no such label"
			else:
				#First we look if it is a User_Defined_Cluster which is bound to this label. If it is a cluster, we look if there a user_cluster linked to this cluster. If not we take the cluster
				statement = 'MATCH (:Label {name:"' + name_cluster + '"})<-[:LABEL]-(clus:User_Defined_Cluster) '\
							'RETURN clus '\
							'UNION '\
							'MATCH (:Label {name:"' + name_cluster + '"})<-[:LABEL]-(:Cluster)<-[:CLUSTERED]-(clus:User_Defined_Cluster) '\
							'RETURN clus '\
							'UNION '\
							'MATCH (:Label {name:"' + name_cluster + '"})<-[:LABEL]-(clus:Cluster) '\
							'WHERE NOT (clus)<-[:CLUSTERED]-() '\
							'RETURN clus'
				cursor = tx.run(statement)
				neo4jNode_cluster = cursor.evaluate()

				list_cluster_to_display.append(neo4jNode_cluster)
				#We search every user_Cluster linked to our cluster
				#clus->track->track->user_clus
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(neo4jNode_cluster)._id) + \
								' MATCH (clus)-[r1:CLUSTERED]->(:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(:Track)<-[r2:CLUSTERED]-(cluster:User_Defined_Cluster) '
				if not showInvalidated:
					statement += 'WHERE (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") AND (r2.isValidated ="' + 'Unknow' + '" OR r2.isValidated ="' + 'Valide' + '") '
				
				#clus->track->track->cluster
				statement += 'RETURN DISTINCT cluster '\
								'UNION '\
								'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(neo4jNode_cluster)._id) + \
								' MATCH (clus)-[r1:CLUSTERED]->(:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(:Track)<-[r2:CLUSTERED]-(cluster:Cluster) '\
								'WHERE NOT (cluster)<-[:CLUSTERED]-() '
				if not showInvalidated:
					statement += 'AND (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") AND (r2.isValidated ="' + 'Unknow' + '" OR r2.isValidated ="' + 'Valide' + '") '
				statement += 'RETURN DISTINCT cluster '
				
				cursor = tx.run(statement)
				for record in cursor:
					if record[0] not in list_cluster_to_display:
						list_cluster_to_display.append(record[0])
				
				#we search the tracks around the main_cluster
				"""
				#First when we have an overlapping
				tracks_Main_Overlap = []
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(neo4jNode_cluster)._id) + \
								' MATCH (clus)-[r:CLUSTERED]->(:Track)-[:OVERLAP]-(tracks:Track) '\
								'WHERE NOT((tracks)<-[:CLUSTERED]-()) '
				if not showInvalidated:
						statement += 'AND (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") '
				statement += 'RETURN DISTINCT tracks'
				cursor = tx.run(statement)
				for record in cursor:
					tracks_Main_Overlap.append(record[0])
				"""
				tracks_Main = []
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(neo4jNode_cluster)._id) + \
								' MATCH (clus)-[r:CLUSTERED]->(:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(tracks:Track) '\
								'WHERE NOT((tracks)<-[:CLUSTERED]-()) '
				if not showInvalidated:
						statement += 'AND (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") '
				statement += 'RETURN DISTINCT tracks'
				cursor = tx.run(statement)
				for record in cursor:
					#if record[0] not in tracks_Main_Overlap:
					tracks_Main.append(record[0])
				
				#We create the nodes
				for clusterNeo_to_display in list_cluster_to_display:
					meta_node = create_meta_node_cluster(graphNeo4j, self.graph, main_sub, clusterNeo_to_display,showInvalidated,nb_cluster)
					dict_Neo4j_Tlp[str(remote(clusterNeo_to_display)._id)] = meta_node
					nb_cluster += 1
					if nb_cluster ==1:
						tlpMain_Node = meta_node
					nb_track = Nb_Track[meta_node]
					IdNeo4j[meta_node] = str(remote(clusterNeo_to_display)._id)
					RootIdNeo4j[meta_node] = str(remote(clusterNeo_to_display)._id)
					TypeNode[meta_node] = "Cluster"
					viewColor[meta_node] = ColorMetanode
					
					#Contour du node
					statement = 'MATCH (clus) ' \
									'WHERE ID(clus)=' + str(remote(clusterNeo_to_display)._id) + \
									' MATCH (clus)-[r:VALIDATED_BY|INVALIDATED_BY]-() '\
									'RETURN r'
					cursor = tx.run(statement)
					record = cursor.evaluate(0)
					viewBorderWidth[meta_node] = BorderWidthCluster
					if record is None:
						viewBorderColor[meta_node] = ColorBorderUnknow
					else:
						label = record.type()
						if label == "VALIDATED_BY":
							viewBorderColor[meta_node] = ColorBorderValide
						elif label == "INVALIDATED_BY":
							viewBorderColor[meta_node] = ColorBorderInvalide
					
				#this create the links between cluster
				for clusterNeo4j_1 in list_cluster_to_display:
					for clusterNeo4j_2 in list_cluster_to_display:
						if list_cluster_to_display.index(clusterNeo4j_1) < list_cluster_to_display.index(clusterNeo4j_2):
							#We count how many overlap there is between this 2 clusters
							statement = 'MATCH (clus1) '\
											'WHERE ID(clus1)=' + str(remote(clusterNeo4j_1)._id) + \
											' MATCH (clus2) '\
											'WHERE ID(clus2)=' + str(remote(clusterNeo4j_2)._id) + \
											' MATCH (clus1)-[r1:CLUSTERED]->(tracks1:Track)-[overlap_rel:OVERLAP]-(tracks2:Track)<-[r2:CLUSTERED]-(clus2) '
							if (not showInvalidated):
								statement += 'WHERE (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") AND (r2.isValidated ="' + 'Unknow' + '" OR r2.isValidated ="' + 'Valide' + '") '
							statement += ' WITH count(DISTINCT overlap_rel) AS nb_overlap '\
												'RETURN nb_overlap'
							
							
							record = graphNeo4j.run(statement)
							nb_rel_over = record.evaluate(0)
							
							#We count how may tracks there is between the 2 clusters
							statement = 'MATCH (clus1) '\
											'WHERE ID(clus1)=' + str(remote(clusterNeo4j_1)._id) + \
											' MATCH (clus2) '\
											'WHERE ID(clus2)=' + str(remote(clusterNeo4j_2)._id) + \
											' MATCH (clus1)-[r1:CLUSTERED]->(tracks1:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(tracks2:Track)<-[r2:CLUSTERED]-(clus2) '
							if (not showInvalidated):
								statement += 'WHERE (r1.isValidated ="' + 'Unknow' + '" OR r1.isValidated ="' + 'Valide' + '") AND (r2.isValidated ="' + 'Unknow' + '" OR r2.isValidated ="' + 'Valide' + '") '
							statement += 'WITH count(DISTINCT tracks1) AS nb1, count(DISTINCT tracks2) AS nb2 '\
											'RETURN [nb1,nb2]'
							record = graphNeo4j.run(statement)
							nb_info = record.evaluate(0)
							nb_track = nb_info[0] + nb_info[1]
							
							
							#We search the metanode
							for metanode in main_sub.getNodes():
								if IdNeo4j[metanode] == str(remote(clusterNeo4j_1)._id):
									meta1 = metanode
								elif IdNeo4j[metanode] == str(remote(clusterNeo4j_2)._id):
									meta2 = metanode					
							
							
							#We create edge between the two clusters
							if nb_track != 0:
								edge_tracks = main_sub.addEdge(meta1,meta2)
								NbOverlap[edge_tracks] = nb_rel_over
								Nb_Track[edge_tracks] = nb_track
					
				"""
				for track in tracks_Main_Overlap:
					node_track = main_sub.addNode()
					edge_track = main_sub.addEdge(node_track,tlpMain_Node)
					NbOverlap[edge_track] = 1
					Nb_Track[edge_track] = 1
					Nb_Track[node_track] = 1
					TypeNode[node_track] = "Track"
				"""
				dict_hashSet_Set = {}
				dict_hashSet_Facetracks = {}
				for track in tracks_Main:
					#We search which clusters there is near this track
					statement = 'MATCH (track:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(:Track)<-[r:CLUSTERED]-(clus:User_Defined_Cluster) '\
									'WHERE ID(track)=' + str(remote(track)._id)
					if (not showInvalidated):
								statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") '
					statement += 'RETURN DISTINCT clus '\
									'UNION '\
									'MATCH (track:Track)<-[:TRACK]-(:Topic)-[:TRACK]->(:Track)<-[r:CLUSTERED]-(clus:Cluster) '\
									'WHERE ID(track)=' + str(remote(track)._id) + ' AND NOT (clus)<-[:CLUSTERED]-() '
					if (not showInvalidated):
								statement += ' AND  (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") AND (r.isValidated ="' + 'Unknow' + '" OR r.isValidated ="' + 'Valide' + '") '
					statement += 'RETURN DISTINCT clus'
					cursor_clusVoisin = graphNeo4j.run(statement)
					setVoisin = ImmutableSet([record[0] for record in cursor_clusVoisin ])
					#for record in cursor_clusVoisin:
					#	setVoisin.add(record[0])
					hashOfSet = hash(setVoisin)
					if hashOfSet not in dict_hashSet_Set:
						dict_hashSet_Set[hashOfSet] = setVoisin
						dict_hashSet_Facetracks[hashOfSet] = [track]
					dict_hashSet_Facetracks[hashOfSet].append(track)

				#Now we look what set we have
				minCreationMetanode = parameters["Creation of a Metanode"]
				for hashset,list_facetrack in dict_hashSet_Facetracks.iteritems():
					if len(list_facetrack) <= minCreationMetanode:
						for trackNeo4j in list_facetrack:
							#We need information on the track to load a Texture
							statement = 'MATCH (track:Track) '\
											'WHERE ID(track)=' + str(remote(trackNeo4j)._id) + \
											' MATCH (track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day) ' \
											'RETURN day.date'
							cursor = graphNeo4j.run(statement)
							date = cursor.evaluate(0)
							date = str(date).split("/")
							day = date[0]
							month = date[1]
							year = date[2]
							
							if len(day) == 1:
								day = "0" + day
							if len(month) == 1:
								month = "0" + month
							
							if year not in dict_texture:
								f = open(texture_path + "image_track_" + str(year) +".json")
								json_texture_year = json.load(f) 
								f.close()
								dict_texture[year] = json_texture_year
							else:
								json_texture_year = dict_texture[year]
							info_track = dict(trackNeo4j)
							num_shot = str(info_track["num_shot"])
							num_track = str(info_track["num_track"])
							
							image_str = json_texture_year[month + "_" + day][num_shot + "_" + num_track]
							image_file = open("temp_image/image_Connect_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg","wb")
							image_file.write(image_str.decode('base64'))
							image_file.close()
							
							path_image = "temp_image/image_Connect_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg"
							
							node_track = main_sub.addNode()
							viewTexture[node_track] = path_image
							viewColor[node_track] = ColorMetanode
							viewShape[node_track] = ShapeTracks
							IdNeo4j[node_track] = str(remote(trackNeo4j)._id)
							Nb_Track[node_track] = 1
							TypeNode[node_track] = "Track"
							listVoisin = dict_hashSet_Set[hashset]
							for VoisinNeo in listVoisin:
								VoisinTlp = dict_Neo4j_Tlp[str(remote(VoisinNeo)._id)]
								edge_track = main_sub.addEdge(node_track,VoisinTlp)
								NbOverlap[edge_track] = 0
					else:
						subgraph = self.graph.getRoot().addSubGraph()
						metanode = main_sub.createMetaNode(subgraph)
						Nb_Track[metanode] = len(list_facetrack)
						viewShape[metanode] = ShapeDiamond
						viewColor[metanode] = ColorDiamond
						listVoisin = dict_hashSet_Set[hashset]
						for VoisinNeo in listVoisin:
							VoisinTlp = dict_Neo4j_Tlp[str(remote(VoisinNeo)._id)]
							edge_track = main_sub.addEdge(metanode,VoisinTlp)
							NbOverlap[edge_track] = 0
						#We set set the vector property with the list of all the tracks
						ListTrackInMetaNode[metanode] = [str(remote(trackNeo4j)._id) for trackNeo4j in list_facetrack ]
						


		parameter_dict = tlp.getDefaultPluginParameters("FM^3 (OGDF)")
		#parameter_dict["Unit edge length"] = 60
		main_sub.applyLayoutAlgorithm("FM^3 (OGDF)", viewLayout, parameter_dict)
		 
		e2Color = {e:viewColor[e]for e in main_sub.getNodes()}
		parameter_dict = {}
		parameter_dict["target"] = "edges"
		parameter_dict["input property"] = NbOverlap
		main_sub.applyColorAlgorithm("Color Mapping", viewColor,parameter_dict)
		for e in main_sub.getNodes():
			viewColor[e] = e2Color[e]
		
		e2size = {e:viewSize[e] for e in main_sub.getEdges()}
		parameter_dict = tlp.getDefaultPluginParameters('Size Mapping')
		parameter_dict["result"] = viewSize
		parameter_dict["input"] = viewSize
		parameter_dict["property"] = Nb_Track
		parameter_dict["min size"] = 3.0
		parameter_dict["max size"] = 9.0
		parameter_dict["node/edge"] = True
		parameter_dict["area proportional"] = "Area Proportional"
		main_sub.applySizeAlgorithm("Size Mapping",viewSize,parameter_dict)
		for e in main_sub.getEdges():
			viewSize[e] = e2size[e]
			
		n2size = {n:viewSize[n] for n in main_sub.getNodes()}
			
		parameter_dict = tlp.getDefaultPluginParameters('Size Mapping')
		parameter_dict["result"] = viewSize
		parameter_dict["input"] = viewSize
		parameter_dict["property"] = Nb_Track
		parameter_dict["node/edge"] = False
		parameter_dict["min size"] = 0.2
		parameter_dict["max size"] = 3.0
		parameter_dict["area proportional"] = "Area Proportional"
		main_sub.applySizeAlgorithm("Size Mapping",viewSize,parameter_dict)
		for n in main_sub.getNodes():
			viewSize[n] = n2size[n]

		parameter_dict = tlp.getDefaultPluginParameters('Fast Overlap Removal')
		parameter_dict["x border"] = 5
		parameter_dict["y border"] = 5
		main_sub.applyLayoutAlgorithm("Fast Overlap Removal", viewLayout,parameter_dict)
	
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jConnect", "Adrien", "16/08/2016", "Neo4JCluster", "1.0")
