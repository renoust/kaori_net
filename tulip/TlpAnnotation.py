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
		
		self.addStringParameter("Name","the name of the seclection you want to give", "")

	def check(self):
		parameters = self.dataSet
		launchAlgo = True
		error = ""
		name = parameters["Name"]
		if name == "":
			launchAlgo = False
			error = "Please set a name for your selection"
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		TypeNode = self.graph.getStringProperty("Type Node")
		nb_node = 0
		for node in viewSelection.getNodesEqualTo(True):
			if TypeNode[node] == "Cluster" or TypeNode[node] == "Track":
				nb_node += 1
		if nb_node == 0:
			launchAlgo = False
			error = "Please Select a node you want to annotate"
		return (launchAlgo, error)

	def run(self):
		parameters = self.dataSet
		name = parameters["Name"]
		py2neo.authenticate(serveur_neo, id_neo, password_neo)
		graphNeo4j = py2neo.Graph(url_neo)
		tx = graphNeo4j.begin()
		
		viewLabel = self.graph.getStringProperty("viewLabel")
		viewSelection = self.graph.getBooleanProperty("viewSelection")
		TypeNode = self.graph.getStringProperty("Type Node")
		IdNeo4j = self.graph.getStringProperty("Neo4jId")
		
		viewShape = self.graph.getIntegerProperty("viewShape")
		ShapeSquare = tlp.NodeShape.Square
		ShapeCircle = tlp.NodeShape.Circle
		
		viewLayout =  self.graph.getLayoutProperty("viewLayout")
		
		Nb_Track = self.graph.getDoubleProperty("Nb Tracks")
		
		viewBorderWidth = self.graph.getDoubleProperty("viewBorderWidth")
		BorderWidthCluster = 10
		
		viewBorderColor = self.graph.getColorProperty("viewBorderColor")
		ColorBorderUnknow = tlp.Color(127,127,127)
		
		viewSize = self.graph.getSizeProperty("viewSize")
		viewColor = self.graph.getColorProperty("viewColor")
		ColorMetanode = tlp.Color(255,255,255,255)
		
		viewLabelPosition = self.graph.getIntegerProperty("viewLabelPosition")
		LabelPositionCluster = tlp.LabelPosition.Bottom
		
		viewTexture = self.graph.getStringProperty("viewTexture")
		
		list_Cluster_to_annotate = []
		list_track_to_annotate = []
		list_newNode = []
		#this is is to remeber us that we will merge in Tulip after
		for NodeTlp in viewSelection.getNodesEqualTo(True):
			if TypeNode[NodeTlp] == "Cluster":
				list_Cluster_to_annotate.append(NodeTlp)
				viewShape[NodeTlp] = ShapeSquare
				viewLabelPosition[NodeTlp] = LabelPositionCluster
				viewLabel[NodeTlp] = name
				list_newNode.append(NodeTlp)
			if TypeNode[NodeTlp] == "Track":
				list_track_to_annotate.append(NodeTlp)
				list_newNode.append(NodeTlp)

		for clusterTlp in list_Cluster_to_annotate:
			clusNeo4jId = IdNeo4j[clusterTlp]
			#First we look if the label already exist:
			statement = 'MATCH (lab:Label {name:"' + name + '"}) '\
							'RETURN lab'
			cursor = graphNeo4j.run(statement)
			labelNeo4j = cursor.evaluate(0)
			#Si le label n'existe pas, on le cree.
			if labelNeo4j is None:
				print 'newLabel'
				statement = ' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
								'CREATE (lab:Label {name:"' + name + '"}) '\
								'CREATE (lab)-[:IDENTIFY_BY]->(algo) '\
								'RETURN lab '
				
				cursor = graphNeo4j.run(statement)
				NewLabel = cursor.evaluate(0)
				#We look if it is a user_def or a cluster
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(clusNeo4jId) +\
								' RETURN (clus) '
				cursor = graphNeo4j.run(statement)
				clus = cursor.evaluate(0)
				for label in clus.labels():
					lab = label
				#Si c'etait un user_defined. On cree un lien entre le user_clus et le label. On supprime le lien de l'ancien. Si l'ancien sert plus a rien, on le supprime
				if str(lab) == 'User_Defined_Cluster':
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(remote(clus)._id) + \
									' MATCH (clus)-[r:LABEL]->(lab) '\
									'DELETE r '\
									'RETURN lab'
					cursor = graphNeo4j.run(statement)
					ancienLabel = cursor.evaluate(0)
					#On supprime le label s'il ne sert plus a rien
					if ancienLabel is not None:
						statement = 'MATCH (label) '\
										'WHERE ID(label)=' + str(remote(ancienLabel)._id) + \
										' MATCH (label)<-[r:LABEL]-()'\
										'RETURN count(r)'
						cursor = graphNeo4j.run(statement)
						nb_rel = cursor.evaluate(0)
						print nb_rel
						if nb_rel == 0:
							statement = 'MATCH (label) '\
											'WHERE ID(label)=' + str(remote(ancienLabel)._id) + \
											'MATCH (label)-[r]-() '\
											'DELETE r '\
											'DELETE label'
							graphNeo4j.run(statement)
					#On cree le lien entre le nouveau label et le user_clus
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(remote(clus)._id) + \
									' MATCH (newlab) '\
									'WHERE ID(newlab)=' + str(remote(NewLabel)._id) + \
									' CREATE (clus)-[:LABEL]->(newlab) '
					graphNeo4j.run(statement)
				else:
					#Si nous avions un simple cluster. On cree un nouveau user_clus qui reprend les tracks du clus et on le relie au nouveau label
					statement = 'MATCH (clus) '\
							'WHERE ID(clus)=' + str(remote(clus)._id) + \
							' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
							'MATCH (label) '\
							'WHERE ID(label)=' + str(remote(NewLabel)._id) + \
							' CREATE (user_defined:User_Defined_Cluster) '\
							'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
							'CREATE (user_defined)-[:CLUSTERED]->(clus) '\
							'CREATE (user_defined)-[:LABEL]->(label) '\
							'RETURN user_defined'
					cursor = graphNeo4j.run(statement)
					newUserClus = cursor.evaluate(0)
					#We change the id in Tulip of the cluster
					IdNeo4j[clusterTlp] = str(remote(newUserClus)._id)
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(remote(clus)._id) + \
									' MATCH (clus)-[:CLUSTERED]->(track) '\
									'WITH COLLECT(DISTINCT track) AS tracks '\
									'RETURN tracks '
					cursor = graphNeo4j.run(statement)
					listTracks = cursor.evaluate()
					#On connect tous les tracks au nouveau user_clus
					for track in listTracks:
						statement = 'MATCH (user_clus) '\
										'WHERE ID(user_clus)=' + str(remote(newUserClus)._id) + \
										' MATCH (track) '\
										'WHERE ID(track)=' + str(remote(track)._id) + \
										' CREATE (user_clus)-[:CLUSTERED]->(track) '
						graphNeo4j.run(statement)
			else:
				#Si le label existe deja
				print "label already exist"
				statement = 'MATCH (clus) '\
								'WHERE ID(clus)=' + str(clusNeo4jId) +\
								' RETURN (clus) '
				cursor = graphNeo4j.run(statement)
				clus = cursor.evaluate(0)
				#On regarde le type du cluster
				for label in clus.labels():
					lab = label
				#Si c'est un user_clus
				if str(lab) == 'User_Defined_Cluster': 
					#On ne fait rien si le label a pas change
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(clusNeo4jId) +\
									' MATCH (clus)-[:LABEL]->(label)' \
									'RETURN label'
					cursor = graphNeo4j.run(statement)
					OldLabel = cursor.evaluate(0)
					#Si on avait deja un label sur ce user_clus
					if OldLabel is not None:
						#Si le label est different de celui d'avant
						if dict(OldLabel)["name"] != name:
							#On supprime le lien de l'ancien et on supprime le label s'il est devenu inutile.
							statement = 'MATCH (clus) '\
											'WHERE ID(clus)=' + str(clusNeo4jId) +\
											' MATCH (clus)-[r:LABEL]->()' \
											'DELETE r '
							graphNeo4j.run(statement)

							statement = 'MATCH (label) '\
										'WHERE ID(label)=' + str(remote(OldLabel)._id) + \
										' MATCH (label)<-[r:LABEL]-()'\
										'RETURN count(r)'
							cursor = graphNeo4j.run(statement)
							nb_rel = cursor.evaluate(0)

							if nb_rel == 0:
								statement = 'MATCH (label) '\
												'WHERE ID(label)=' + str(remote(OldLabel)._id) + \
												' MATCH (label)-[r]-() '\
												'DELETE r '
								graphNeo4j.run(statement)
								statement = 'MATCH (label) '\
												'WHERE ID(label)=' + str(remote(OldLabel)._id) + \
												' DELETE label'
								graphNeo4j.run(statement)
							
					#On regarde si un user_clus est relie au label deja existant
					statement = 'MATCH (label) '\
									'WHERE ID(label)=' + str(remote(labelNeo4j)._id) + \
									' MATCH (label)<-[:LABEL]-(user_clus:User_Defined_Cluster) '\
									'RETURN user_clus'
					cursor = graphNeo4j.run(statement)
					user_clus_to_merge = cursor.evaluate(0)
					
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(clusNeo4jId) +\
									' RETURN clus'
					cursor = graphNeo4j.run(statement)
					clus_to_labeled = cursor.evaluate(0)
					if user_clus_to_merge is not None:
						print "le bout est user_clus"
						#Si on a bien le user_clus a faire merger

						oldclus = clus_to_labeled
						new_clus_id = neo4j_commit.merge_userCluster_userCluster(graphNeo4j,user_clus_to_merge,oldclus)
						
						IdNeo4j[clusterTlp] = new_clus_id
					else:
						print "Le bout est cluster"
						#Si on a un cluster a faire merger.
						statement = 'MATCH (label) '\
									'WHERE ID(label)=' + str(remote(labelNeo4j)._id) + \
									' MATCH (label)<-[:LABEL]-(clus:Cluster) '\
									'RETURN clus'
						cursor = graphNeo4j.run(statement)
						clus_to_merge = cursor.evaluate(0)
						if clus_to_merge is not None:
							oldclus = clus_to_labeled
								
							new_clus_id = neo4j_commit.merge_userCluster_Cluster(graphNeo4j,oldclus,clus_to_merge)
							IdNeo4j[clusterTlp] = new_clus_id
				#Si on a un cluster
				if str(lab) == 'Cluster':
					#On ne fait rien si le label a pas change
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(clusNeo4jId) +\
									' MATCH (clus)-[:LABEL]->(label)' \
									'RETURN label'
					cursor = graphNeo4j.run(statement)
					OldLabel = cursor.evaluate(0)
					if OldLabel is None or (OldLabel is not None and dict(OldLabel)["name"] != name):
						#We create a new user_clus
						statement = 'MATCH (clus) '\
							'WHERE ID(clus)=' + str(remote(clusNeo4jId)._id) + \
							' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
							' CREATE (user_defined:User_Defined_Cluster) '\
							'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
							'CREATE (user_defined)-[:CLUSTERED]->(clus) '\
							'RETURN user_defined'
						cursor = graphNeo4j.run(statement)
						newUserClus = cursor.evaluate(0)
						#We change the id in Tulip of the cluster
						IdNeo4j[clusterTlp] = str(remote(newUserClus)._id)
						statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(remote(clus)._id) + \
									' MATCH (clus)-[:CLUSTERED]->(track) '\
									'WITH COLLECT(DISTINCT track) AS tracks '\
									'RETURN tracks '
						cursor = graphNeo4j.run(statement)
						listTracks = cursor.evaluate()
						#On connect tous les tracks au nouveau user_clus
						for track in listTracks:
							statement = 'MATCH (user_clus) '\
											'WHERE ID(user_clus)=' + str(remote(newUserClus)._id) + \
											' MATCH (track) '\
											'WHERE ID(track)=' + str(remote(track)._id) + \
											' CREATE (user_clus)-[:CLUSTERED]->(track) '
							graphNeo4j.run(statement)
						
						#We look if the label is already connected to a user cluster
						statement = 'MATCH (label) '\
									'WHERE ID(label)=' + str(remote(labelNeo4j)._id) + \
									' MATCH (label)<-[:LABEL]-(user_clus:User_Defined_Cluster) '\
									'RETURN user_clus'
						cursor = graphNeo4j.run(statement)
						user_clus_to_merge = cursor.evaluate(0)
						
						clus_to_labeled = newUserClus
						
						if user_clus_to_merge is not None:
							print "le bout est user_clus"
							#Si on a bien le user_clus a faire merger
	
							oldclus = clus_to_labeled
							new_clus_id = neo4j_commit.merge_userCluster_userCluster(graphNeo4j,user_clus_to_merge,oldclus)
							
							IdNeo4j[clusterTlp] = new_clus_id
						else:
							print "Le bout est cluster"
							#Si on a un cluster a faire merger.
							statement = 'MATCH (label) '\
										'WHERE ID(label)=' + str(remote(labelNeo4j)._id) + \
										' MATCH (label)<-[:LABEL]-(clus:Cluster) '\
										'RETURN clus'
							cursor = graphNeo4j.run(statement)
							clus_to_merge = cursor.evaluate(0)
							if clus_to_merge is not None:
								oldclus = clus_to_labeled
									
								new_clus_id = neo4j_commit.merge_userCluster_Cluster(graphNeo4j,oldclus,clus_to_merge)
								IdNeo4j[clusterTlp] = new_clus_id
						
			
		#Now : Track Annotation
		for trackTlp in list_track_to_annotate:
			trackNeo4jId = IdNeo4j[trackTlp]
			#We look if the label allready exist
			statement = 'MATCH (lab:Label {name:"' + name + '"}) '\
							'RETURN lab'
			cursor = graphNeo4j.run(statement)
			labelNeo4j = cursor.evaluate(0)
			#Si le label n'existe pas, on le cree. On cree aussi un user_cluster linked to it
			if labelNeo4j is None:
				print 'newLabel'
				statement = ' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
								'CREATE (lab:Label {name:"' + name + '"}) '\
								'CREATE (lab)-[:IDENTIFY_BY]->(algo) '\
								'CREATE (user_defined:User_Defined_Cluster) '\
								'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
								'CREATE (user_defined)-[:LABEL]->(lab) '\
								'RETURN user_defined'
				cursor = graphNeo4j.run(statement)
				clusNeo4j = cursor.evaluate(0)
			else:
				#Otherwise we look if there a user_clus linked to this label
				statement = 'MATCH (lab:Label {name:"' + name + '"}) '\
								'MATCH (user_defined:User_Defined_Cluster)-[:LABEL]->(lab) '\
								'RETURN user_defined'
				cursor = graphNeo4j.run(statement)
				clusNeo4j = cursor.evaluate(0)
				#If there is no user_clus we search a clus and we create a user_clus
				if clusNeo4j is None:
					statement = 'MATCH (lab:Label {name:"' + name + '"}) '\
									'MATCH (clus:Cluster)-[:LABEL]->(lab) '\
									' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
									' CREATE (user_defined:User_Defined_Cluster) '\
									'CREATE (user_defined)-[:GENERATED_BY]->(algo) '\
									'CREATE (user_defined)-[:CLUSTERED]->(clus) '\
									'CREATE (user_defined)-[:LABEL]->(lab) '\
									'RETURN user_defined'
					cursor = graphNeo4j.run(statement)
					clusNeo4j = cursor.evaluate(0)
					statement = 'MATCH (lab:Label {name:"' + name + '"}) '\
									'MATCH (clus:Cluster)-[:LABEL]->(lab) '\
									' MATCH (clus)-[:CLUSTERED]->(track) '\
									'WITH COLLECT(DISTINCT track) AS tracks '\
									'RETURN tracks '
					cursor = graphNeo4j.run(statement)
					listTracks = cursor.evaluate()
					#On connect tous les tracks au nouveau user_clus
					for track in listTracks:
						statement = 'MATCH (user_clus) '\
										'WHERE ID(user_clus)=' + str(remote(clusNeo4j)._id) + \
										' MATCH (track) '\
										'WHERE ID(track)=' + str(remote(track)._id) + \
										' CREATE (user_clus)-[:CLUSTERED]->(track) '
						graphNeo4j.run(statement)
					
					
			#Normally clusNeo4j is not None
			if clusNeo4j is None:
				print "ERROR with the label : " + name + ". It seems that this label is linked to nothing."
						
			#We have our cluster. Maybe the track is already connected to it. We set the link to "Valide"
			statement = 'MATCH (clus) '\
							'WHERE ID(clus)=' + str(remote(clusNeo4j)._id) + \
							' MATCH (track) '\
							'WHERE ID(track)=' + trackNeo4jId + \
							' MERGE (clus)-[r:CLUSTERED]->(track) '\
							' SET r.isValidated = "Valide" '
			graphNeo4j.run(statement)
		
		#Now we load a texture for the new user_clus
		statement = 'MATCH (lab:Label {name:"' + name + '"}) '\
						'MATCH (user_clus:User_Defined_Cluster)-[:LABEL]->(lab) '\
						'RETURN user_clus'
		cursor = graphNeo4j.run(statement)
		NewNeo4jClus = cursor.evaluate(0)
		if NewNeo4jClus is not None:
			texture = neo4j_commit.load_texture(graphNeo4j,NewNeo4jClus)
		else:
			print "Error"
		
		#now we have to merge all the node of Tulip together
		#first we check if other nodes have the same label in Tulip
		for NodeTlp in self.graph.getNodes():
			if TypeNode[NodeTlp] == "Cluster" and viewLabel[NodeTlp] == name:
				if NodeTlp not in list_newNode:
					list_newNode.append(NodeTlp)
		
		PosXNewNode = 0.0
		PosYNewNode = 0.0
		nb_node = 0
		list_node_to_connect = []
		Nb_Total_Track = 0
		for NodeTlp in list_newNode:
			nb_node += 1
			PosXNewNode += viewLayout[NodeTlp][0]
			PosYNewNode += viewLayout[NodeTlp][1]
			for node_to_connect in self.graph.getInOutNodes(NodeTlp):
				if node_to_connect not in list_node_to_connect:
					list_node_to_connect.append(node_to_connect)
			Nb_Total_Track  += Nb_Track[NodeTlp]
			self.graph.delNode(NodeTlp)
		
		subgraph = self.graph.getRoot().addSubGraph()
		newClus = self.graph.createMetaNode(subgraph)
		PosXNewNode = PosXNewNode / float(nb_node)
		PosYNewNode = PosYNewNode / float(nb_node)
		viewLayout[newClus] = tlp.Coord(PosXNewNode,PosYNewNode)
		viewShape[newClus] = ShapeSquare
		viewLabel[newClus] = name
		viewLabelPosition[newClus] = LabelPositionCluster
		viewSize[newClus] = tlp.Size(10,10,10)
		viewColor[newClus] = ColorMetanode
		TypeNode[newClus] = "Cluster"
		IdNeo4j[newClus] = str(remote(NewNeo4jClus)._id)
		viewBorderWidth[newClus] = BorderWidthCluster
		Nb_Track[newClus] = Nb_Total_Track
		
		image_file = open("temp_image/image_Texture_NewAnnotation" + str(name) + str(Nb_Total_Track) + ".jpg","wb")
		image_file.write(texture.decode('base64'))
		image_file.close()
		path_image = "temp_image/image_Texture_NewAnnotation" + str(name) + str(Nb_Total_Track) + ".jpg"
		viewTexture[newClus] = path_image
		viewBorderColor[newClus] = ColorBorderUnknow
		
		for node_to_connect in list_node_to_connect:
			self.graph.addEdge(node_to_connect,newClus)
		
		
		parameter_dict = tlp.getDefaultPluginParameters('Fast Overlap Removal')
		parameter_dict["x border"] = 5
		parameter_dict["y border"] = 5
		self.graph.applyLayoutAlgorithm("Fast Overlap Removal", viewLayout,parameter_dict)
		
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jAnnotation", "Adrien", "26/08/2016", "Neo4JCluster", "1.0")
