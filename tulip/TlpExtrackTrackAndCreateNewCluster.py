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
		
		IdNeo4jOfMetaNode = self.graph.getStringProperty("Id Neo4j of the MetaNode")
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		TypeNode = self.graph.getStringProperty("Type Node")
		IdNeo4j = self.graph.getStringProperty("Neo4jId")
		
		SomethingHasChanged = False
		

		
		if str(self.graph.getName()).isdigit() is True or str(self.graph.getName())== "unnamed":
			graphRoot = self.graph.getRoot()
			IdNeo4jOfMetaNode = graphRoot.getStringProperty("Neo4jId")
			#We search the Main subgraph
			
			for subgraph in graphRoot.getSubGraphs():
				if subgraph.getName() == "Main":
					graphMain = subgraph
			
			MainviewLayout =  graphMain.getLayoutProperty("viewLayout")
			MainTypeNode = graphMain.getStringProperty("Type Node")
			MainIdNeo4j = graphMain.getStringProperty("Neo4jId")
			MainviewBorderColor = graphMain.getColorProperty("viewBorderColor")
			MainColorBorderUnknow = tlp.Color(127,127,127)
			MainColorBorderValide = tlp.Color(0,86,27)
			MainColorBorderInvalide = tlp.Color(187,11,11)
			MainviewColor = graphMain.getColorProperty("viewColor")
			MainColorCluster = tlp.Color(255,255,255,255)
			MainviewSize = graphMain.getSizeProperty("viewSize")
			MainNb_Track = graphMain.getDoubleProperty("Nb Tracks")
			MainviewTexture = graphMain.getStringProperty("viewTexture")
			MainviewShape = graphMain.getIntegerProperty("viewShape")
			MainviewBorderWidth = graphMain.getDoubleProperty("viewBorderWidth")
			MainBorderWidthCluster = 10
			ShapeMetanodeNotAnnote = tlp.NodeShape.Circle
			
			
			for Metanodes in graphRoot.getNodes():
				if graphRoot.isMetaNode(Metanodes):
					if graphRoot.getNodeMetaInfo(Metanodes) == self.graph:
						MetaCluster = Metanodes
						print Metanodes
			idNeo4jCluster = IdNeo4jOfMetaNode[MetaCluster]
			
			CoordMeta = MainviewLayout[MetaCluster]
			
			list_node_to_connect = []
			for node_adjacent in graphMain.getInOutNodes(MetaCluster):
				list_node_to_connect.append(node_adjacent)
			
			

			#We create a new cluster
			statement = 'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
							' CREATE (user_defined:User_Defined_Cluster) ' \
							'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
							'RETURN user_defined '
			cursor = graphNeo4j.run(statement)
			Newclus = cursor.evaluate(0)
			
			ClusterIsFound = False
			
			nb_track_to_remove = 0
			for NodeTlp in viewSelection.getNodesEqualTo(True):
				if TypeNode[NodeTlp] == "Track":
					nb_track_to_remove += 1
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

							idNeo4jCluster = str(remote(clusNeo4j)._id)
							IdNeo4jOfMetaNode[MetaCluster] = idNeo4jCluster
							
					#Now we have our cluster. We have juste to remove the tracks that we want and we link the tracks to the new cluster
					trackId = IdNeo4j[NodeTlp]
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
									' MATCH (track) '\
									'WHERE ID(track)=' + str(trackId) + \
									' MATCH (clus)-[r:CLUSTERED]->(track) '\
									'DELETE r '

					graphNeo4j.run(statement)
					
					statement = ' MATCH (newclus) '\
									'WHERE ID(newclus)=' + str(remote(Newclus)._id) + \
									' MATCH (track) '\
									'WHERE ID(track)=' + str(trackId) + \
									' CREATE (newclus)-[:CLUSTERED {isValidated:"Valide"}]->(track) '
					graphNeo4j.run(statement)


					graphRoot.delNode(NodeTlp)
			
			#We have to supress the cluster if there is no tracks left.
			if SomethingHasChanged:
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
								' MATCH (clus)-[:CLUSTERED]->(track:Track) '\
								'RETURN count(track) '
				cursor = graphNeo4j.run(statement)
				nb_track = cursor.evaluate(0)
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
					graphRoot.delNode(MetaCluster)
				else:
					#We update the texture of the old Cluster
					Oldtexture = neo4j_commit.load_texture(graphNeo4j,clusNeo4j)
					image_file = open("temp_image/image_Texture_OldCluster" + str(remote(clusNeo4j)._id) + ".jpg","wb")
					image_file.write(Oldtexture.decode('base64'))
					image_file.close()
					path_image = "temp_image/image_Texture_OldCluster" + str(remote(clusNeo4j)._id) + ".jpg"
					MainviewTexture[MetaCluster] = path_image
					
				Newtexture = neo4j_commit.load_texture(graphNeo4j,Newclus)
				
				#Now we create the new cluster in Tulip
				NewMetanodeSubgraph = graphRoot.addSubGraph()
				NewMetaNode = graphMain.createMetaNode(NewMetanodeSubgraph)
				MainviewLayout[NewMetaNode] = CoordMeta
				MainTypeNode[NewMetaNode] = "Cluster"
				MainIdNeo4j[NewMetaNode] = str(remote(Newclus)._id)
				MainviewBorderColor[NewMetaNode] = MainColorBorderValide
				MainviewColor[NewMetaNode] = MainColorCluster
				MainviewSize[NewMetaNode] = tlp.Size(10,10,10)
				MainNb_Track[NewMetaNode] = nb_track_to_remove
				MainviewShape[NewMetaNode] = ShapeMetanodeNotAnnote
				MainviewBorderWidth[NewMetaNode] = MainBorderWidthCluster
				
				image_file = open("temp_image/image_Texture_NewCluster" + str(remote(Newclus)._id) + ".jpg","wb")
				image_file.write(Newtexture.decode('base64'))
				image_file.close()
				path_image = "temp_image/image_Texture_NewCluster" + str(remote(Newclus)._id) + ".jpg"
				
				MainviewTexture[NewMetaNode] = path_image
				
				
				
				
				
				for node_adjacent in list_node_to_connect:
					graphMain.addEdge(NewMetaNode,node_adjacent)
				
				parameter_dict = tlp.getDefaultPluginParameters('Fast Overlap Removal')
				parameter_dict["x border"] = 5
				parameter_dict["y border"] = 5
				graphMain.applyLayoutAlgorithm("Fast Overlap Removal", MainviewLayout,parameter_dict)
		
	
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4j Extract Tracks And Create New Cluster", "Adrien", "31/08/2016", "", "1.0")
