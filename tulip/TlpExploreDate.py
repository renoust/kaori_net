from tulip import *
import tulipplugins
import py2neo
from py2neo.types import remote
from py2neo import Node
import json

texture_path = ""


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

	def check(self):
		# This method is called before applying the algorithm on the input graph.
		# You can perform some precondition checks here.
		# See comments in the run method to know how to have access to the input graph.

		# Must return a tuple (Boolean, string). First member indicates if the algorithm can be applied
		# and the second one can be used to provide an error message.
		return (True, "")

	def run(self):
		py2neo.authenticate(serveur_neo, id_neo, password_neo)
		graphNeo4j = py2neo.Graph(url_neo)
		tx = graphNeo4j.begin()
		
		
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		TypeNode = self.graph.getStringProperty("Type Node")
		IdNeo4j = self.graph.getStringProperty("Neo4jId")
		
		
		Nb_Track = self.graph.getDoubleProperty("Nb Tracks")
		viewShape = self.graph.getIntegerProperty("viewShape")
		ShapeTracks = tlp.NodeShape.Square
		viewColor = self.graph.getColorProperty("viewColor")
		ColorTopic = tlp.Color(247, 35, 12)
		ColorTransparent = tlp.Color(255,255,255,255)
		viewLabel = self.graph.getStringProperty("viewLabel")
		NbOverlap = self.graph.getDoubleProperty("Nb Overlap")
		
		viewTexture = self.graph.getStringProperty("viewTexture")
		viewSize = self.graph.getSizeProperty("viewSize")
		viewLayoutMain = self.graph.getLayoutProperty("viewLayout")
		dict_Neo4j_Tlp = {}
		dict_texture = {}
		
		for NodeTlp in viewSelection.getNodesEqualTo(True):
			if TypeNode[NodeTlp] == "Day":
				initialLayout = viewLayoutMain[NodeTlp]
				subgraphAlgoLayout = self.graph.addSubGraph()
				subgraphAlgoLayout.addNode(NodeTlp)
				viewLayout = subgraphAlgoLayout.getLayoutProperty("viewLayout")
				
				topics = self.graph.getOutNodes(NodeTlp)
				list_top = []
				for topic in topics:
					list_top.append(IdNeo4j[topic])
				idNeo4Day = IdNeo4j[NodeTlp]
				
				date = str(viewLabel[NodeTlp]).split("/")
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
				
				#We search every topics and track linked to this date
				statement = 'MATCH (day) '\
								'WHERE ID(day)=' + idNeo4Day + \
								' MATCH (tracks:Track)<-[:TRACK]-(top)<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day) '\
								'RETURN DISTINCT [top,tracks] '
				cursorTracks = tx.run(statement)
				list_topicNeo4j = []
				for coupleTopTrack in cursorTracks:
					topic= coupleTopTrack[0][0]
					track = coupleTopTrack[0][1]
					
					#Si le topic n'a pas deja ete cree avec la requete Connect
					if str(remote(topic)._id) not in list_top:
						#Si le topic n'a pas deja ete fait:
						if topic not in list_topicNeo4j:
							num_top = dict(topic)["name"]
							list_topicNeo4j.append(topic)
							topic_node = self.graph.addNode()
							viewSize[topic_node] = tlp.Size(7,7,0)
							subgraphAlgoLayout.addNode(topic_node)
							viewColor[topic_node] = ColorTopic
							TypeNode[topic_node] = "Topic"
							IdNeo4j[topic_node] = str(remote(topic)._id)
							Nb_Track[topic_node] = 2
							viewLabel[topic_node] = "Topic : " + str(num_top)
							dict_Neo4j_Tlp[str(remote(topic)._id)] = topic_node
							edge_day = self.graph.addEdge(NodeTlp,topic_node)
							subgraphAlgoLayout.addEdge(edge_day)
							Nb_Track[edge_day] = 1
						else:
							topic_node = dict_Neo4j_Tlp[str(remote(topic)._id)]
						
						track_node = self.graph.addNode()
						viewSize[track_node] = tlp.Size(3,3,0)
						subgraphAlgoLayout.addNode(track_node)
						TypeNode[track_node] = "Track"
						IdNeo4j[track_node] = str(remote(track)._id)
						Nb_Track[track_node] = 1
						viewShape[track_node] = ShapeTracks
						dict_Neo4j_Tlp[str(remote(track)._id)] = track_node
						edge = self.graph.addEdge(topic_node,track_node)
						subgraphAlgoLayout.addEdge(edge)
						NbOverlap[edge] = 0.0
						Nb_Track[edge] = 1.0
						viewColor[track_node] = ColorTransparent
						info_track = dict(track)
						num_shot = str(info_track["num_shot"])
						num_track = str(info_track["num_track"])

						image_str = json_texture_year[month + "_" + day][num_shot + "_" + num_track]
						image_file = open("temp_image/image_ExploreDate_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg","wb")
						image_file.write(image_str.decode('base64'))
						image_file.close()
						
						path_image = "temp_image/image_ExploreDate_" + str(year) + "_" + str(month) + "_" + str(day) + "_" + str(num_shot) + "_" + str(num_track) + ".jpg"
						viewTexture[track_node] = path_image
						
		
		
				parameter_dict = tlp.getDefaultPluginParameters('FM^3 (OGDF)')
				parameter_dict["Unit edge length"] = 3
				subgraphAlgoLayout.applyLayoutAlgorithm("FM^3 (OGDF)", viewLayout,parameter_dict)
				Correction = viewLayout[NodeTlp] - initialLayout
				for node_to_modify in subgraphAlgoLayout.getNodes():
					viewLayoutMain[node_to_modify] = tlp.Coord(viewLayout[node_to_modify][0] - Correction[0],viewLayout[node_to_modify][1] - Correction[1],viewLayout[node_to_modify][2] - Correction[2])
				print viewLayout[NodeTlp]
				self.graph.delSubGraph(subgraphAlgoLayout)
				
		parameter_dict = tlp.getDefaultPluginParameters('Fast Overlap Removal')
		parameter_dict["x border"] = 1
		parameter_dict["y border"] = 1
		self.graph.applyLayoutAlgorithm("Fast Overlap Removal", viewLayout,parameter_dict)
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4j Explore Date", "Adrien", "02/09/2016", "", "1.0")
