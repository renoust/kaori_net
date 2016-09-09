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
		
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		TypeNode = self.graph.getStringProperty("Type Node")
		IdNeo4j = self.graph.getStringProperty("Neo4jId")
		viewTexture = self.graph.getStringProperty("viewTexture")
		SomethingHasChanged = False
		
		#If we are in a metaNode
		if str(self.graph.getName()).isdigit() is True or str(self.graph.getName())== "unnamed":
			graphRoot = self.graph.getRoot()
			
			for subgraph in graphRoot.getSubGraphs():
				if subgraph.getName() == "Main":
					graphMain = subgraph
			
			MainviewLayout =  graphMain.getLayoutProperty("viewLayout")
			MainTypeNode = graphMain.getStringProperty("Type Node")
			MainIdNeo4j = graphMain.getStringProperty("Neo4jId")
			MainviewColor = graphMain.getColorProperty("viewColor")
			MainColorCluster = tlp.Color(255,255,255,255)
			MainviewSize = graphMain.getSizeProperty("viewSize")
			MainNb_Track = graphMain.getDoubleProperty("Nb Tracks")
			MainviewTexture = graphMain.getStringProperty("viewTexture")
			MainviewShape = graphMain.getIntegerProperty("viewShape")

			ShapeTrack = tlp.NodeShape.Square
			IdNeo4jOfMetaNode = graphMain.getStringProperty("Neo4jId")
			
			
			for Metanodes in graphRoot.getNodes():
				if graphRoot.isMetaNode(Metanodes):
					if graphRoot.getNodeMetaInfo(Metanodes) == self.graph:
						MetaCluster = Metanodes
			idNeo4jCluster = IdNeo4jOfMetaNode[MetaCluster]
			print idNeo4jCluster
			ClusterIsFound = False
			
			list_node_connect_to_Metacluster = []
			for node in graphMain.getInOutNodes(MetaCluster):
				list_node_connect_to_Metacluster.append(node)
			
			
			
			for NodeTlp in viewSelection.getNodesEqualTo(True):
				if TypeNode[NodeTlp] == "Track":
					SomethingHasChanged = True
					if ClusterIsFound is False:
						ClusterIsFound = True
						#We have to know if it is a user_defined_cluster or a cluster
						statement = 'MATCH (clus) '\
										'WHERE ID(clus)=' + str(idNeo4jCluster) + \
										' RETURN clus '
						cursor = graphNeo4j.run(statement)
						clusNeo4j = cursor.evaluate(0)
						for lab in clusNeo4j.labels():
							labelCluster = str(lab)
						#If it is a simple cluster. We have to create a new user_defined_cluster. We have to connect every track from the cluster to the clusNeo4j
						if labelCluster == "Cluster":
							statement = 'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
											'MATCH (clus) '\
											'WHERE ID(clus)=' + str(idNeo4jCluster) + \
											' CREATE (user_defined:User_Defined_Cluster) ' \
											'CREATE (user_defined)-[:CLUSTERED]->(clus) '\
											'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
											'RETURN user_defined '
							cursor = graphNeo4j.run(statement)
							clusNeo4j = cursor.evaluate(0)
							IdNeo4jOfMetaNode[MetaCluster] = str(remote(clusNeo4j)._id)

							#We connect to a label if there is one
							statement = 'MATCH (clus) '\
											'WHERE ID(clus)=' + str(idNeo4jCluster) + \
											' MATCH (clus)-[:LABEL]-(label) '\
											'RETURN label'
							cursor = graphNeo4j.run(statement)
							label = cursor.evaluate(0)
							if label is not None:
								statement = 'MATCH (clusNeo4j) '\
											'WHERE ID(clusNeo4j)=' + str(remote(clusNeo4j)._id) + \
											' MATCH (label) '\
											'WHERE ID(label)=' + str(remote(label)._id) + \
											' CREATE (clusNeo4j)-[:LABEL]->(label)'
								graphNeo4j.run(statement)
							
							statement = 'MATCH (clus) '\
											'WHERE ID(clus)=' + str(idNeo4jCluster) + \
											' MATCH (clus)-[r:CLUSTERED]->(tr:Track) '\
											'WITH COLLECT (DISTINCT [r,tr]) AS list_rel '\
											'RETURN list_rel'
							cursor = graphNeo4j.run(statement)
							list_rel = cursor.evaluate(0)
							for infoRelation in list_rel:
								state = str(dict(infoRelation[0])["isValidated"])
								track = infoRelation[1]
								statement = 'MATCH (clusNeo4j) '\
											'WHERE ID(clusNeo4j)=' + str(remote(clusNeo4j)._id) + \
											' MATCH (track) '\
											'WHERE ID(track)=' + str(remote(track)._id) + \
											' MERGE (clusNeo4j)-[r:CLUSTERED]->(track) '\
											'SET r.isValidated = "' + state + '"'
								graphNeo4j.run(statement)
							texture = neo4j_commit.load_texture(graphNeo4j,clusNeo4j)
					#Now we have our cluster. We have juste to remove the tracks that we want
					trackId = IdNeo4j[NodeTlp]
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
									' MATCH (track) '\
									'WHERE ID(track)=' + str(trackId) + \
									' MATCH (clus)-[r:CLUSTERED]->(track) '\
									'DELETE r'
					graphNeo4j.run(statement)
					
					#We create a new track in the Main graph
					newTrack = graphMain.addNode()
					for node_to_connect in list_node_connect_to_Metacluster:
						graphMain.addEdge(node_to_connect,newTrack)
						

					MainviewLayout[newTrack] = MainviewLayout[MetaCluster]
					MainTypeNode[newTrack] = "Track"
					MainIdNeo4j[newTrack] = str(trackId)
					MainviewColor[newTrack] = MainColorCluster
					MainviewSize[newTrack] = tlp.Size(3,3,3)
					MainNb_Track[newTrack] = 1
					MainviewShape[newTrack] = ShapeTrack
					
					MainviewTexture[newTrack] = viewTexture[NodeTlp]
					
					self.graph.getRoot().delNode(NodeTlp)
					
					
			#We have to supress the cluster if there is no tracks left.
			if SomethingHasChanged:
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
								' MATCH (clus)-[:CLUSTERED]->(track:Track) '\
								'RETURN count(track) '
				cursor = graphNeo4j.run(statement)
				nb_track = cursor.evaluate(0)
				print nb_track 
				#S'il n'y a plus de track dans le cluster
				if nb_track == 0:
					statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
								' MATCH (clus)-[:LABEL]->(label) '\
								'RETURN label'
					cursor = graphNeo4j.run(statement)
					lab = cursor.evaluate(0)
					if lab is not None:
						statement = 'MATCH (label) '\
										'WHERE ID(label)=' + str(remote(lab)._id) + \
										' MATCH (label)<-[r:LABEL]-()'\
										'RETURN count(r)'
						cursor = graphNeo4j.run(statement)
						nb_rel = cursor.evaluate(0)
						statement = 'MATCH (clus) '\
										'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
										' MATCH (clus)-[r:LABEL]->(label) '\
										'MATCH (label)-[allRel]-() '\
										'DELETE r '
						if nb_rel == 1:
							statement += 'DELETE allRel '\
											'DELETE label'
						graphNeo4j.run(statement)
					statement = 'MATCH (clus) '\
								'WHERE ID(clus)='+ str(remote(clusNeo4j)._id) + \
								' MATCH (clus)-[r]-() '\
								'DELETE r '\
								'DELETE clus'
					graphNeo4j.run(statement)
					
					#We suprress the MetaCluster if there is no track left
					graphRoot.delNode(MetaCluster)
				
				parameter_dict = tlp.getDefaultPluginParameters('Fast Overlap Removal')
				parameter_dict["x border"] = 5
				parameter_dict["y border"] = 5
				graphMain.applyLayoutAlgorithm("Fast Overlap Removal", MainviewLayout,parameter_dict)
		
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jRemoveFromCluster", "Adrien", "29/08/2016", "", "1.0")
