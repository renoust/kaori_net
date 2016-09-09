from tulip import *
import tulipplugins
import py2neo
from py2neo.types import remote
from py2neo import Node


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
		IdNeo4jOfMetaNode = self.graph.getStringProperty("Id Neo4j of the MetaNode")
		IdNeo4j = self.graph.getStringProperty("Neo4jId")

		viewBorderColor = self.graph.getColorProperty("viewBorderColor")
		ColorBorderInvalide = tlp.Color(187,11,11)

		#If we are in a metanode
		if str(self.graph.getName()).isdigit() is True:
			for NodeTlp in viewSelection.getNodesEqualTo(True):
				if TypeNode[NodeTlp] == "Track":
					idMeta = IdNeo4jOfMetaNode[NodeTlp]
					idTrack = IdNeo4j[NodeTlp]
					viewBorderColor[NodeTlp] = ColorBorderInvalide
					statement = 'MATCH (clus) '\
									'WHERE ID(clus)=' + str(idMeta) + \
									' MATCH (track) '\
									'WHERE ID(track)=' + str(idTrack) + \
									' MATCH (clus)-[r:CLUSTERED]->(track) '\
									'SET r.isValidated = "NotValide"'
					graphNeo4j.run(statement)
		else:
			#Sinon c'est qu'on est sur le graph Main. Seul les cluster peuvent etre valides
			for NodeTlp in viewSelection.getNodesEqualTo(True):
				if TypeNode[NodeTlp] == "Cluster":
					idCluster = IdNeo4j[NodeTlp]
					viewBorderColor[NodeTlp] = ColorBorderInvalide
					statement = 'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
									' MATCH (clus) '\
									'WHERE ID(clus)=' + str(idCluster) + \
									' OPTIONAL MATCH (clus)-[r:VALIDATED_BY]->(algo) '\
									'DELETE r '\
									'MERGE (clus)-[:INVALIDATED_BY]->(algo)'
					print statement
					graphNeo4j.run(statement)
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jInvalidate", "Adrien", "29/08/2016", "", "1.0")
