from tulip import *
import tulipplugins
import py2neo

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
		self.addBooleanParameter("Delete All Modification","Are you sure you want to delete all modification ?", "False")

	def check(self):
		# This method is called before applying the algorithm on the input graph.
		# You can perform some precondition checks here.
		# See comments in the run method to know how to have access to the input graph.

		# Must return a tuple (Boolean, string). First member indicates if the algorithm can be applied
		# and the second one can be used to provide an error message.
		return (True, "")

	def run(self):
		parameters = self.dataSet
		ClearAll = parameters["Delete All Modification"]
		if ClearAll:
			py2neo.authenticate(serveur_neo, id_neo, password_neo)
			graphNeo4j = py2neo.Graph(url_neo)
			tx = graphNeo4j.begin()
			
			statement = ' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
							'MATCH (algo)<-[r]->() '\
							'DELETE r'
	
			tx.run(statement)

			statement = ' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
				'MATCH (algo)<-[:IDENTIFY_BY]-(lab) '\
				'WHERE NOT (lab)<-[:LABEL]-(:Cluster) '\
				'MATCH (lab)-[r]-() '\
				'DELETE r '\
				'DELETE lab '
			tx.run(statement)
			statement = 'MATCH (lab:Label) '\
							'WHERE NOT (lab)-[]-() '\
							'DELETE lab'
			tx.run(statement)
			
			statement = 'MATCH (user_defined:User_Defined_Cluster) '\
							'MATCH (user_defined)-[r]-() '\
							'DELETE r '
			tx.run(statement)
			
			statement = 'MATCH (user_defined:User_Defined_Cluster) '\
							'DELETE user_defined'
			tx.run(statement)
			
			statement = 'MATCH ()-[r:CLUSTERED]->() '\
							'SET r.isValidated = "Unknow" '
			tx.run(statement)
			
			statement = 'MATCH (n) '\
							'WHERE size(labels(n)) = 0 '\
							'DELETE n'
			tx.run(statement)
			
			
			tx.commit()
			neo4j_commit.correct_name(graphNeo4j)
			neo4j_commit.merge_overlap_cluster(graphNeo4j)
			neo4j_commit.load_all_texture(graphNeo4j)
		return True

# The line below does the magic to register the plugin into the plugin database
# and updates the GUI to make it accessible through the menus.
tulipplugins.registerPlugin("Main", "Neo4jClearAllModification", "Adrien", "29/08/2016", "", "1.0")
