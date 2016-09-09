from tulip import *
import tulipplugins
import py2neo
from py2neo.types import remote
from py2neo import Node

import sys
sys.path.insert(0, 'Extraction_Interaction_server')
import neo4j_commit

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
		graphRoot = self.graph.getRoot()
		
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		RootviewSelection = graphRoot.getBooleanProperty("viewSelection")
		
		TypeNode = self.graph.getStringProperty("Type Node")
		RootTypeNode = graphRoot.getStringProperty("Type Node")
		
		IdNeo4j = self.graph.getStringProperty("Neo4jId")
		RootIdNeo4j = graphRoot.getStringProperty("Neo4jId")

		viewBorderWidth = self.graph.getDoubleProperty("viewBorderWidth")
		RootviewBorderWidth = graphRoot.getDoubleProperty("viewBorderWidth")
		BorderWidthCluster = 10
		
		viewLayout =  self.graph.getLayoutProperty("viewLayout")
		RootviewLayout =  graphRoot.getLayoutProperty("viewLayout")
		
		viewBorderColor = self.graph.getColorProperty("viewBorderColor")
		RootviewBorderColor = graphRoot.getColorProperty("viewBorderColor")
		ColorBorderUnknow = tlp.Color(127,127,127)
		ColorBorderValide = tlp.Color(0,86,27)
		ColorBorderInvalide = tlp.Color(187,11,11)
		
		viewColor = self.graph.getColorProperty("viewColor")
		RootviewColor = graphRoot.getColorProperty("viewColor")
		ColorCluster = tlp.Color(255,255,255,255)
		
		viewSize = self.graph.getSizeProperty("viewSize")
		RootviewSize = graphRoot.getSizeProperty("viewSize")
		
		Nb_Track = self.graph.getDoubleProperty("Nb Tracks")
		RootNb_Track = graphRoot.getDoubleProperty("Nb Tracks")
		
		viewTexture = self.graph.getStringProperty("viewTexture")
		RootviewTexture = graphRoot.getStringProperty("viewTexture")
		
		viewLabelPosition = self.graph.getIntegerProperty("viewLabelPosition")
		LabelPositionMetanode = tlp.LabelPosition.Bottom
		
		viewLabel = self.graph.getStringProperty("viewLabel")
		viewShape = self.graph.getIntegerProperty("viewShape")
		ShapeMetanodeAnnote = tlp.NodeShape.Square
		ShapeMetanodeNotAnnote = tlp.NodeShape.Circle
		list_Cluster_to_merge = []
		list_track_to_merge = []
		
		PosXNewNode = 0.0
		PosYNewNode = 0.0
		nb_Node_to_merge = 0
		
		Nb_Total_Track = 0
		list_NodeTlp_to_link = []
		nameClus = ""
		for NodeTlp in viewSelection.getNodesEqualTo(True):
			if TypeNode[NodeTlp] == "Cluster":
				list_Cluster_to_merge.append(IdNeo4j[NodeTlp])
				Node_to_link = graphRoot.getInOutNodes(NodeTlp)
				path_image = viewTexture[NodeTlp]
				for Node in Node_to_link:
					if Node not in list_NodeTlp_to_link:
						list_NodeTlp_to_link.append(Node)
				graphRoot.delEdges(graphRoot.getInOutEdges(NodeTlp))
				Nb_Total_Track += Nb_Track[NodeTlp]
				
				PosXNewNode += RootviewLayout[NodeTlp][0]
				PosYNewNode += RootviewLayout[NodeTlp][1]
				nb_Node_to_merge += 1
				nameClus = viewLabel[NodeTlp]
				graphRoot.delNode(NodeTlp)
				
			elif TypeNode[NodeTlp] == "Track":
				Nb_Total_Track += 1
				list_track_to_merge.append(IdNeo4j[NodeTlp])
				Node_to_link = self.graph.getInOutNodes(NodeTlp)
				for Node in Node_to_link:
					if Node not in list_NodeTlp_to_link:
						list_NodeTlp_to_link.append(Node)
				self.graph.delEdges(self.graph.getInOutEdges(NodeTlp))
				self.graph.delNode(NodeTlp)

				PosXNewNode += viewLayout[NodeTlp][0]
				PosYNewNode += viewLayout[NodeTlp][1]
				nb_Node_to_merge += 1

		
		#First we merge the cluster
		not_finish = True
		Main_clus_is_defined = False
		while not_finish:
			not_finish = False
			if len(list_Cluster_to_merge) >= 2:
				clus1 = list_Cluster_to_merge[0]
				clus2 = list_Cluster_to_merge[1]
				#not_finish = True
				
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + clus1 + \
								' RETURN clus'
				cursor = graphNeo4j.run(statement)
				clusNeo4j1 = cursor.evaluate(0)
				for lab in clusNeo4j1.labels():
					if str(lab) == 'User_Defined_Cluster':
						type1 = "user"
					else:
						type1 = "cluster"
						
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + clus2 + \
								' RETURN clus'
				cursor = graphNeo4j.run(statement)
				clusNeo4j2 = cursor.evaluate(0)
				for lab in clusNeo4j2.labels():
					if str(lab) == 'User_Defined_Cluster':
						type2 = "user"
					else:
						type2 = "cluster"
						
				if type1 == "user" and type2 == "user":
					new_clus = neo4j_commit.merge_userCluster_userCluster(graphNeo4j,clusNeo4j1,clusNeo4j2)
				elif type1 == "cluster" and type2 == "cluster":
					new_clus = neo4j_commit.merge_Cluster_Cluster(graphNeo4j,clusNeo4j1,clusNeo4j2)
				elif type1 == "user" and type2 == "cluster":
					new_clus = neo4j_commit.merge_userCluster_Cluster(graphNeo4j,clusNeo4j1,clusNeo4j2)
				elif type2 == "user" and type1 == "cluster":
					new_clus = neo4j_commit.merge_userCluster_Cluster(graphNeo4j,clusNeo4j2,clusNeo4j1)
				
				list_Cluster_to_merge.remove(clus1)
				list_Cluster_to_merge.remove(clus2)
				list_Cluster_to_merge.insert(0,new_clus)
		
		if len(list_Cluster_to_merge) >= 1:
			Main_clus = str(list_Cluster_to_merge[0])
			Main_clus_is_defined = True
				
		
		#Now We merge the tracks to the cluster. If there is none we have to create one.
		LoadTexture = False
		if len(list_track_to_merge) >= 1 and Main_clus_is_defined is False:
			statement = 'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
				'CREATE (user_defined:User_Defined_Cluster) '\
				'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
				'RETURN user_defined'
			cursor = graphNeo4j.run(statement)
			clusNeo4j = cursor.evaluate(0)
			Main_clus = str(remote(clusNeo4j)._id)
			Main_clus_is_defined = True
			LoadTexture = True
		
		for track in list_track_to_merge:
			statement = 'MATCH (clus) '\
							'WHERE ID(clus)=' + Main_clus + \
							' Match (track:Track) '\
							'WHERE ID(track)=' + track + \
							' CREATE (clus)-[:CLUSTERED {isValidated:"Valide"} ]->(track)'
			graphNeo4j.run(statement)
		
		if LoadTexture:
			print Main_clus
			texture = neo4j_commit.load_texture(graphNeo4j,Main_clus)
		
		if Main_clus:
			
			subgraph = graphRoot.addSubGraph()
			NewNode = self.graph.createMetaNode(subgraph)
			PosXNewNode = PosXNewNode / float(nb_Node_to_merge)
			PosYNewNode = PosYNewNode / float(nb_Node_to_merge)
			viewLayout[NewNode] = tlp.Coord(PosXNewNode,PosYNewNode)
			for node_to_link in list_NodeTlp_to_link:
				edge = self.graph.addEdge(node_to_link,NewNode)
			
			IdNeo4j[NewNode] = str(Main_clus)
			TypeNode[NewNode] = "Cluster"
			viewBorderWidth[NewNode] = BorderWidthCluster
			Nb_Track[NewNode] = Nb_Total_Track
			if LoadTexture:
				image_file = open("temp_image/image_Texture_New" + str(Main_clus) + ".jpg","wb")
				image_file.write(texture.decode('base64'))
				image_file.close()
				path_image = "temp_image/image_Texture_New" + str(Main_clus) + ".jpg"
			viewTexture[NewNode] = path_image
			
			statement = 'MATCH (clus) ' \
				'WHERE ID(clus)=' + str(Main_clus) + \
				' MATCH (clus)-[r:VALIDATED_BY|INVALIDATED_BY]-() '\
				'RETURN r'
			cursor = tx.run(statement)
			record = cursor.evaluate(0)
			if record is None:
				viewBorderColor[NewNode] = ColorBorderUnknow
			else:
				label = str(record.type())
				print label
				if label == "VALIDATED_BY":
					viewBorderColor[NewNode] = ColorBorderValide
				elif label == "INVALIDATED_BY":
					viewBorderColor[NewNode] = ColorBorderInvalide
					
			viewBorderColor[NewNode] = ColorBorderUnknow
			viewSize[NewNode] = tlp.Size(10,10,10)
			viewColor[NewNode] = ColorCluster
			if str(nameClus) != "":
				viewLabel[NewNode] = str(nameClus)
				viewLabelPosition[NewNode] = LabelPositionMetanode
				viewShape[NewNode]= ShapeMetanodeAnnote
			else:
				viewShape[NewNode]= ShapeMetanodeNotAnnote
			
		parameter_dict = tlp.getDefaultPluginParameters('Fast Overlap Removal')
		parameter_dict["x border"] = 5
		parameter_dict["y border"] = 5
		self.graph.applyLayoutAlgorithm("Fast Overlap Removal", viewLayout,parameter_dict)
		
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jMerge", "Adrien", "26/08/2016", "Neo4JCluster", "1.0")
