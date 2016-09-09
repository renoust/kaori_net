#!/usr/bin/python
# -*-coding:Utf-8 -*

from py2neo import Graph, authenticate
from neokit import GraphServer
from data_format import *
from py2neo.types import remote

#Function which creates a graph in neo4j from the data located on the serveur.
""" 
Structure of the graph
Nodes for the time_tree: Timeline, n Year, n*12 Month, n*12*31 Day.
Relations for the time tree : YEAR (between Timeline and Year), MONTH (between Year and Month), DAY (between Month and Day)

Nodes for the data : one Jt per Day, n Topic per Jt, n Track per Topic, n Person per Track, One Detection per dection type, Cluster, Algo (name of the algorithm used for the clusters)
Relations for the data : BROADCAST_ON (between Jt and Day), COMPOSED_OF (between Jt and Topic), TRACK (between Topic and Track), ClUSTERED (between Cluster and Track), GENERATED_BY (between Algo and Cluster)
OVERLAP (between two tracks where there is an overlaping),DETECTION (between Track and Detection)
SUPERPERSON (between Person and Superperson)
"""

# HTTP logging is disabled. HTTP logging can be enabled by setting this property to 'true'.
#org.neo4j.server.http.log.enabled=false 
#Put here your id and password for your neo4j account :
id_neo = ""
password_neo = ""
#The serveur you want to use and the url :
serveur_neo = ""
url_neo = ""
emplacement_neo = ""
#Don't forget to launch neo4j. Otherwise it will not works. Ex : ./neo4j-community-3.0.3/bin/neo4j console.

#If there the error heap space occurs : 
#Open /neo4j-community-3.0.3/conf/neo4j-wrapper.conf
#Set those lignes like this : (without #)
#dbms.memory.heap.initial_size=512
#dbms.memory.heap.max_size=2048
#This Root path has to be set correctly
path_Root = ""

topic_seg_path_local = path_Root + ""
track_path_local = path_Root + ""
cluster_annoted_path_local = path_Root + ""
cluster_unannoted_path_local = path_Root + ""
texture_path_local = path_Root + ""

from config_file import *

#Construct the time tree graph with the segmentation_topic file.
def update_topic_graph(graph):
	print ""
	print "Suppression of the graph..."
	tx = graph.begin() #tx will be our query object.
	statement = 'MATCH (n) OPTIONAL MATCH (n)-[r]-() '\
			'DELETE n,r'
	tx.run(statement)
	tx.process()
	print "Done"
	print "Creation of the time tree graph with the topics..."
	tmps1 = time.time()
	f = open(topic_seg_path_local)
	json_data = json.load(f) #we load the segmentation_topic data.
	f.close()
	tx.run('MERGE (:Timeline {name:\"Data_tree\"})') #The first Node of our graph
	compt = 0
	for date_str, topics in json_data.iteritems():
		#Topics is like : {u'1': {u'begin_sec': 104, u'end_sec': 291, u'end_frame': 8721, u'nb_lines': 3, u'begin_frame': 3116}, u'3': {u'begin_sec': 635, u'end_sec': 672, u'end_frame': 20139, u'nb_lines': 3, u'begin_frame': 19030}, u'2': {u'begin_sec': 303, u'end_sec': 596, u'end_frame': 17862, u'nb_lines': 15, u'begin_frame': 9080}}
		date = date_str.split("_") #date_str is like : 2004_11_14_19_00
		date[0] = int(date[0])
		date[1] = int(date[1])
		date[2] = int(date[2])
		date[3] = int(date[3])
		date[4] = int(date[4])
		statement = 'MERGE (t:Timeline {name:"Data_tree"}) ' \
				'MERGE (t)-[:YEAR]->(year:Year {name:' + str(date[0]) + '}) '\
				'MERGE (year)-[:MONTH]->(month:Month {name:' + str(date[1]) + '}) '\
				'MERGE (month)-[:DAY]->(day:Day {name:' + str(date[2]) + ',date:"' + str(date[2]) + '/' + str(date[1]) + '/' + str(date[0]) + '"}) '\
				'MERGE (day)<-[:BROADCAST_ON]-(jt:Jt {name:"JT"}) '
		#if date[0] == 2010 : # For testing only a part of the graph.
		nb_topics = len(topics)
		for num_topic, info_topic in topics.iteritems():
			#We create Topic nodes
			statement += 'CREATE (jt)-[:COMPOSED_OF]->(:Topic {name:' + str(num_topic) + \
					', begin_sec:' + str(info_topic["begin_sec"]) + ', end_sec:' + str(info_topic["end_sec"]) + \
					', begin_frame:' + str(info_topic["begin_frame"]) + ', end_frame:' + str(info_topic["end_frame"]) + '}) '
		#We create the first and the last Intertopic nodes.
		statement += 'WITH jt ' \
				'MATCH (jt)-[:COMPOSED_OF]->(top_first:Topic {name:1}) ' + \
				'MATCH (jt)-[:COMPOSED_OF]->(top_last:Topic {name:' + str(nb_topics) + '}) ' + \
				'CREATE (inter1:InterTopic {name:0.5,begin_sec:0, end_sec:top_first.begin_sec - 1, begin_frame:0, end_frame:top_first.begin_frame - 1}) ' + \
				'CREATE (jt)-[:COMPOSED_OF]-> (inter1) ' + \
				'CREATE (inter2:InterTopic {name:' + str(nb_topics + 0.5) + ', begin_sec:top_last.end_sec + 1, end_sec:250000, begin_frame : top_last.end_frame + 1, end_frame:250000}) ' + \
				'CREATE (jt)-[:COMPOSED_OF]->(inter2) '
		#We create the other Intertopic nodes.
		for i in range(1,nb_topics):
			statement += 'WITH jt ' + \
						'MATCH (jt)-[:COMPOSED_OF]->(topic_debut:Topic {name:' + str(i) + '}),(jt)-[:COMPOSED_OF]->(topic_fin:Topic {name:' + str(i+1) + '}) '\
						'CREATE (inter:InterTopic {name:' + str(i+0.5) + ',begin_sec:topic_debut.end_sec+1, end_sec:topic_fin.begin_sec - 1, begin_frame:topic_debut.end_frame +1, end_frame:topic_fin.begin_frame - 1}) '\
						'CREATE (jt)-[:COMPOSED_OF]->(inter)'
		compt += 1
		tx.run(statement)
		tx.process()
		if compt%10 == 0:
			tx.commit()
			tx = graph.begin()
		print compt
	tx.commit()
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."



#Construct the tracks nodes and add them to our graph.
def update_track_graph(graph):
	tx = graph.begin()
	print ""
	print "Suppression of Track and Person nodes"
	statement = 'MATCH ()-[r1:CLUSTERED]-() '\
			'DELETE r1'
	tx.run(statement)
	statement = 'MATCH ()-[r2:TRACK]-() '\
			'DELETE r2'
	tx.run(statement)
	statement = 'MATCH ()-[r3:LABEL]-() '\
			'DELETE r3'
	tx.run(statement)
	statement = 'MATCH ()-[r4:GENERATED_BY]-() '\
			'DELETE r4'
	tx.run(statement)
	statement = 'MATCH ()-[r5:VALIDATED_BY]-() '\
			'DELETE r5'
	tx.run(statement)
	statement = 'MATCH ()-[r6:INVALIDATED_BY]-() '\
			'DELETE r6'
	tx.run(statement)
	statement = 'MATCH ()-[r7:IDENTIFY_BY]-() '\
			'DELETE r7'
	tx.run(statement)
	statement = 'MATCH (n1:CLuster) '\
			'DELETE n1'
	tx.run(statement)
	statement = 'MATCH (n2:Track) '\
			'DELETE n2'
	tx.run(statement)
	statement = 'MATCH (n3:Label) '\
			'DELETE n3'
	tx.run(statement)
	statement = 'MATCH (n4:Algo) '\
			'DELETE n4'
	tx.run(statement)
	statement = 'MATCH (n5:User_Defined_Cluster) '\
			'DELETE n5'
	tx.run(statement)
	tx.commit()
	print "Done"

	tmps1 = time.time()

	tx = graph.begin()

	print ""
	print "Creation of the Track nodes..."
	f = open(track_path_local + "/tracks_data.json")
	json_data = json.load(f) #we load the tracks data file.
	f.close()
	compt_year = 0
	tx = graph.begin()
	for year, value in json_data.iteritems():
	#if int(year) == 2009: #For testing only a part of the graph.
		compt_year += 1
		compt_day = 0
		print year
		tx = graph.begin()
		for day_month, info in value.iteritems():
			day = day_month.split("_")[1]
			month = day_month.split("_")[0]
			#We create a topic nammed 0 if there is no topic.
			statement = 'MERGE (t:Timeline {name:"Data_tree"}) ' \
			'MERGE (t)-[:YEAR]->(year:Year {name:' + str(int(year)) + '}) '\
			'MERGE (year)-[:MONTH]->(month:Month {name:' + str(int(month)) + '}) '\
			'MERGE (month)-[:DAY]->(day:Day {name:' + str(int(day)) + '}) '\
			'MERGE (day)<-[:BROADCAST_ON]-(jt:Jt {name:"JT"}) ' \
			'WITH jt ' \
			'OPTIONAL MATCH (jt)-[:COMPOSED_OF]->(top:Topic) ' \
			'FOREACH (x in CASE WHEN top IS NULL THEN [1] ELSE [] END | ' \
			'CREATE (jt)-[:COMPOSED_OF]->(:Topic {name:0,begin_sec:0, end_sec:250000, begin_frame:0, end_frame:250000})) '
			#if day == "06" and month == "11": #For testing only a part of the graph.
			for num_track_shot, info_frames in info.iteritems():
				num_track_shot = num_track_shot.split("_") # num_track is like : 21_2
				shot_num = num_track_shot[0]
				track_num = num_track_shot[1]
				first_frame = info_frames["first_frame"]
				last_frame = info_frames["last_frame"]
				if first_frame != -1: #We don't add the tracks node where informations are missing.
					statement += 'WITH jt ' \
							'MATCH (jt)-[:COMPOSED_OF]->(topics_track) ' \
							'WHERE (topics_track.begin_frame <=  ' + first_frame + ' AND ' + first_frame + ' <= topics_track.end_frame) OR (topics_track.begin_frame <=  ' + last_frame + ' AND ' + last_frame + ' <= topics_track.end_frame) ' \
							'WITH COLLECT (DISTINCT topics_track) AS topic_track, jt ' \
							'CREATE (track:Track {num_shot:' + shot_num + ', num_track:' + track_num + ', first_frame:' + first_frame + ', last_frame:' + last_frame + '}) ' \
							'FOREACH (topic_node in topic_track | ' \
							'CREATE (topic_node)-[:TRACK]->(track)) '
			compt_day += 1
			tx.run(statement)
			tx.process()		
			if compt_day%10 == 0:
				tx.commit()
				tx = graph.begin()
			print "Year number " + str(compt_year) + ". Year : " + year + ", Day : " + str(compt_day)

	tx.commit()	
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."


def update_track_overlap(graph):
	tmps1 = time.time()
	print ""
	print "Suppression of Overlap ralationships..."
	tx = graph.begin()
	statement = 'MATCH ()-[r:OVERLAP]-() '\
			'DELETE r'
	tx.run(statement)
	tx.process()
	print "Done"
	print "Creation of OVERLAP relationships..."
	compt = 0
	f = open(track_path_local + "/tracks_data.json")
	json_data = json.load(f) #we load the tracks data file.
	f.close()
	for year, value in json_data.iteritems():
		#if year == "2009": #For testing only a part of the graph.
		for day_month, info in value.iteritems():
			day = day_month.split("_")[1]
			month = day_month.split("_")[0]
			for current_shot, current_frames in info.iteritems():
				num_current_shot = current_shot.split("_")
				for compare_shot, compare_frames in info.iteritems():
					num_compare_shot = compare_shot.split("_")
					if ((int(num_current_shot[0]) != int(num_compare_shot[0])) or (int(num_current_shot[1]) != int(num_compare_shot[1]))) and ((int(compare_frames["first_frame"]) <= int(current_frames["first_frame"]) and int(current_frames["first_frame"]) <= int(compare_frames["last_frame"])) or (int(compare_frames["first_frame"]) <= int(current_frames["last_frame"]) and int(current_frames["last_frame"]) <= int(compare_frames["last_frame"]))):
						statement = 'MATCH (t:Timeline {name:"Data_tree"}) ' \
								'MATCH (t)-[:YEAR]->(year:Year {name:' + str(int(year)) + '}) '\
								'MATCH (year)-[:MONTH]->(month:Month {name:' + str(int(month)) + '}) '\
								'MATCH (month)-[:DAY]->(day:Day {name:' + str(int(day)) + '}) '\
								'MATCH (day)<-[:BROADCAST_ON]-(jt:Jt {name:"JT"}) ' \
								'MATCH (jt)-[:COMPOSED_OF]->()-[:TRACK]->(t1:Track {num_shot:' + str(int(num_current_shot[0])) + ',num_track:' + str(int(num_current_shot[1])) + '}) ' \
								'MATCH (jt)-[:COMPOSED_OF]->()-[:TRACK]->(t2:Track {num_shot:' + str(int(num_compare_shot[0])) + ',num_track:' + str(int(num_compare_shot[1])) + '}) ' \
								'MERGE (t1)-[:OVERLAP]-(t2)'
			
			compt += 1
			tx.run(statement)
			tx.process()		
			if compt%10 == 0:
				tx.commit()
				tx = graph.begin()
			print compt
	tx.commit()	
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."



#Some names can be misspelled so we create a superPerson wich regroup surname
def correctPersonName(person):

		#Here we load the name correction table maybe we will 
		# want that in a separate file
		modified = False
		correction = {
			'Shin\\\\': "Shinichi TAKEDA", 
			'Shin': "Shinichi TAKEDA",
			"Shin\\'ichi TAKEDA": "Shinichi TAKEDA", 
			'Kim Yong Il': "Kim Jong Il", 
			'Kim Jong-Il': "Kim Jong Il",
			'ASA SYO RYU': "Akinori ASASHORYU",
			'Hillary CLINTON':'Hilary CLINTON',
			'Jyun': "Jyunichi YAMAMOTO",
			'Michael Jackson': "Michael JACKSON",
			'MICHAEL Jackson': "Michael JACKSON",
			'YOICHI Masuzoe': "Yoichi MASUZOE",
			'NAKARAI Sae': 'Sae NAKARAI',
			'Shin': "Shinichi TAKEDA"
		}

		if person in correction:
			person = correction[person]
			modified = True

		return (person, modified)


#Construct the Cluster nodes who are labeled and add them to our graph. If the name is corrected add a USER_DEFINED_CLUSTER.
def update_cluster_annoted(graph):
	print ""
	print "Supression of the Cluster nodes..."
	tx = graph.begin()
	statement = 'MATCH ()-[r1:CLUSTERED]-() '\
			'DELETE r1'
	tx.run(statement)
	statement = 'MATCH ()-[r3:LABEL]-() '\
			'DELETE r3'
	tx.run(statement)
	statement = 'MATCH ()-[r4:GENERATED_BY]-() '\
			'DELETE r4'
	tx.run(statement)
	statement = 'MATCH ()-[r5:VALIDATED_BY]-() '\
			'DELETE r5'
	tx.run(statement)
	statement = 'MATCH ()-[r6:INVALIDATED_BY]-() '\
			'DELETE r6'
	tx.run(statement)
	statement = 'MATCH ()-[r7:IDENTIFY_BY]-() '\
			'DELETE r7'
	tx.run(statement)
	statement = 'MATCH (n1:Cluster) '\
			'DELETE n1'
	tx.run(statement)
	statement = 'MATCH (n3:Label) '\
			'DELETE n3'
	tx.run(statement)
	statement = 'MATCH (n4:Algo) '\
			'DELETE n4'
	tx.run(statement)
	statement = 'MATCH (n5:User_Defined_Cluster) '\
			'DELETE n5'
	tx.run(statement)
	tx.commit()
	print "Done"

	tx = graph.begin()
	statement = 'CREATE CONSTRAINT ON (a:Algo) ASSERT a.name IS UNIQUE'
	tx.run(statement)
	statement = 'CREATE CONSTRAINT ON (b:Label) ASSERT b.name IS UNIQUE'
	tx.run(statement)
	tx.commit()

	tmps1 = time.time()
	f = open(cluster_annoted_path_local)
	json_data = json.load(f) #we load the annotation data.
	f.close()
	compt = 0

	tx = graph.begin()
	statement = 'CREATE (algo:Algo {name:"' + 'user_defined' + '"})'
	tx.run(statement)
	for date, name in json_data.iteritems():
		date = date.split("_")
		year = str(int(date[0]))
		month = str(int(date[1]))
		day = str(int(date[2]))
		shot_num = str(int(date[3]))
		track_num = str(int(date[4]))
		name = str(name)
		# if year == "2010" and month == "10" and day == "30": #For testing only a part of the graph.
		#The name of the algo is "annotation_clustering"
		statement = 'MATCH (t:Timeline {name:"Data_tree"}) ' \
				'MATCH (t)-[:YEAR]->(year:Year {name:' + year + '}) '\
				'MATCH (year)-[:MONTH]->(month:Month {name:' + month + '}) '\
				'MATCH (month)-[:DAY]->(day:Day {name:' + day + '}) '\
				'MATCH (day)<-[:BROADCAST_ON]-(jt:Jt {name:"JT"}) '\
				'MATCH (jt)-[:COMPOSED_OF]->(topics:Topic) '\
				'MATCH (topics)-[:TRACK]->(track_node:Track) '\
				'WHERE track_node.num_shot = ' + shot_num + ' AND track_node.num_track = ' + track_num + \
				' MERGE (lab:Label {name:"' + name + '"}) '\
				'MERGE (clus:Cluster)-[:LABEL]->(lab) '\
				'MERGE (algo:Algo {name:"' + 'annotation_clustering' +'"}) ' \
				'MERGE (clus)-[:GENERATED_BY]->(algo) ' \
				'MERGE (lab)-[:IDENTIFY_BY]->(algo) '\
				'MERGE (clus)-[:CLUSTERED {isValidated:"' + 'Unknow' + '"}]->(track_node)'
		
		compt += 1
		tx.run(statement)
		tx.process()
		print compt
		if compt%10 == 0:
			tx.commit()
			tx = graph.begin()

	tx.commit()

	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."

def correct_name(graph):
	tmps1 = time.time()
	statement = 'MATCH (labels:Label) '\
				'RETURN labels'
	tx = graph.begin()
	cursor = tx.run(statement)
	for record in cursor:
		name = str(dict(record[0])["name"])
		(user_defined_name, modified) = correctPersonName(name)
		if modified is True:
			statement = 'MATCH (clus_lab:Label) '\
						'WHERE ID(clus_lab)=' + str(remote(record[0])._id) + \
						' MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
						'MATCH (clus:Cluster)-[:LABEL]->(clus_lab) '\
						'MERGE (user_lab:Label {name:"' + user_defined_name + '"}) '\
						'MERGE (user_lab)-[:IDENTIFY_BY]->(algo) '\
						'CREATE (user_clus:User_Defined_Cluster) '\
						'CREATE (user_clus)-[:LABEL]->(user_lab) '\
						'CREATE (user_clus)-[:CLUSTERED]->(clus) '\
						'CREATE (user_clus)-[:GENERATED_BY]->(algo) '\
						'RETURN user_clus,clus '
			cursor2 = tx.run(statement)
			for record2 in cursor2:
				statement = 'MATCH (clus:Cluster) '\
							'WHERE ID(clus)=' + str(remote(record2[1])._id) + \
							' MATCH (clus)-[:CLUSTERED]->(tracks:Track) '\
							'RETURN tracks'
				cursor3 = tx.run(statement)
				for track in cursor3:
					statement = 'MATCH (user_clus:User_Defined_Cluster) '\
								'WHERE ID(user_clus)=' + str(remote(record2[0])._id) + \
								' MATCH (track:Track) '\
								'WHERE ID(track) =' + str(remote(track[0])._id) + \
								' CREATE (user_clus)-[:CLUSTERED]->(track)'
					tx.run(statement)

	tx.commit()
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."


def update_cluster_unannoted(graph):
	tmps1 = time.time()
	f = open(cluster_unannoted_path_local)
	json_data = json.load(f) #we load clusters
	f.close()
	compt = 0
	tx = graph.begin()
	for algo, clusters in json_data.iteritems():
		statement = 'MERGE (algo:Algo {name:"' + algo + '"}) '
		tx.run(statement)
		tx.process()
		for cluster_id, list_track in clusters.iteritems():
			if len(list_track) != 0:
				statement = 'MATCH (algo:Algo {name:"' + algo + '"}) '\
							'CREATE (algo)<-[:GENERATED_BY]-(clus:Cluster {name:"' + str(int(cluster_id)) + '"}) '
				tx.run(statement)
				tx.process()		
				for track in list_track:
					infotrack = track.split("_")
					statement = 'MATCH (clus:Cluster {name:"' + str(int(cluster_id)) + '"})-[:GENERATED_BY]->(:Algo {name:"' + algo + '"}) '\
								'MATCH (t:Timeline {name:"Data_tree"}) ' \
								'MATCH (t)-[:YEAR]->(year:Year {name:' + str(int(infotrack[0])) + '}) '\
								'MATCH (year)-[:MONTH]->(month:Month {name:' + str(int(infotrack[1])) + '}) '\
								'MATCH (month)-[:DAY]->(day:Day {name:' + str(int(infotrack[2])) + '}) '\
								'MATCH (day)<-[:BROADCAST_ON]-(jt:Jt {name:"JT"}) '\
								'MATCH (jt)-[:COMPOSED_OF]->(:Topic)-[:TRACK]->(track_node:Track) '\
								'WHERE track_node.num_shot = ' + str(int(infotrack[3])) + ' AND track_node.num_track = ' + str(int(infotrack[4])) + \
								' MERGE (clus)-[:CLUSTERED {isValidated:"' + 'Unknow' + '"}]-(track_node)'
					tx.run(statement)
					tx.process()
				compt += 1
				print compt	
				if compt%5 == 0:
					tx.commit()
					tx = graph.begin()
	tx.commit()
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."


def merge_userCluster_userCluster(graph,user_cluster1,user_cluster2):
	tx = graph.begin()
	statement = 'MATCH (user_clus2:User_Defined_Cluster) '\
				'WHERE ID(user_clus2) =' + str(remote(user_cluster2)._id) + \
				' OPTIONAL MATCH (user_clus2)-[:LABEL]->(lab2:Label) '\
				'WITH lab2 '\
				'MATCH (user_clus1:User_Defined_Cluster) '\
				'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
				' OPTIONAL MATCH (user_clus1)-[:LABEL]->(lab1:Label) '\
				'RETURN lab2.name,lab1.name'
	cursor = tx.run(statement)
	#We have to set the right label
	label_clus2 = ""
	label_clus1 = ""
	for record in cursor:
		if record[0] is not None:
			label_clus2 = str(record[0])
		if record[1] is not None:
			label_clus1 = str(record[1])
	label = ""
	if not(label_clus1 == "" and label_clus2 == ""):
		if label_clus1 == "":
			label = label_clus2
		elif label_clus2 == "":
			label = label_clus1
		elif label_clus1 == label_clus2:
			label = label_clus1
		else:
			label = label_clus1 + "/" + label_clus2
		#On verifie si le user_clus est relié à un label
		statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
					'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
					' MATCH (user_clus1)-[r:LABEL]->(:Label) '\
					'RETURN r'

		cursor = tx.run(statement)
		for record in cursor:
			if label != "":
				#S'il n'y avait pas de label à user_clus1 : On en crée un avec le nouveau label.
				if record[0] is None:
					statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
								'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
								'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
								' MERGE (lab:Label {name:"' + label + '"}) '\
								'MERGE (lab)-[:IDENTIFY_BY]->(algo) '\
								'CREATE (user_clus1)-[:LABEL]->(lab)'
					tx.run(statement)
				else:
					#On supprime le label de user_clus1 si ce label n'était relié qu'à un seul noeud
					record = graph.run('MATCH (user_clus1:User_Defined_Cluster) '\
								'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
								' MATCH (user_clus1)-[:LABEL]->(:Label) '\
								'OPTIONAL MATCH (label)<-[r2:LABEL]-() '\
								'RETURN count(r2)')
					if record.evaluate(0) == 1:
						statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
									'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
									' MATCH (user_clus1)-[:LABEL]->(lab_to_suppr:Label) '\
									'MATCH (lab_to_suppr)-[r]-() '\
									'DELETE r '\
									'DELETE lab_to_suppr'
						tx.run(statement)
					else:
						#Sinon on supprime juste la relation
						statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
									'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
									' MATCH (user_clus1)-[rel_to_suppr:LABEL]->(:Label) '\
									'DELETE rel_to_suppr'
						tx.run(statement)
					#Maintenant user_clus1 n'a plus de label et on a supprimer le label que si celui ci n'était utile que à user_clus1
					#On peut mettre le nouveau label
					statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
								'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
								'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
								' MERGE (lab:Label {name:"' + label + '"}) '\
								'MERGE (lab)-[:IDENTIFY_BY]->(algo) '\
								'CREATE (user_clus1)-[:LABEL]->(lab)'
					tx.run(statement)

	tx.commit()
	tx = graph.begin()


	#We supress the label is no other cluster is linked to it
	record = graph.run('MATCH (user_clus2:User_Defined_Cluster) '\
				'WHERE ID(user_clus2) =' + str(remote(user_cluster2)._id) + \
				' MATCH (user_clus2)-[:LABEL]->(label:Label) '\
				'OPTIONAL MATCH (label)<-[r2:LABEL]-() '\
				'RETURN count(r2)')
	if record.evaluate(0) == 1:
		statement = 'MATCH (user_clus2:User_Defined_Cluster) '\
					'WHERE ID(user_clus2) =' + str(remote(user_cluster2)._id) + \
					' MATCH (user_clus2)-[r1:LABEL]->(label:Label) '\
					'MATCH (label)-[r2:IDENTIFY_BY]-() '\
					'DELETE r1 '\
					'DELETE r2 '\
					'DELETE label'
		tx.run(statement)
	tx.commit()
	tx = graph.begin()

	#We give the nodes to connect to the cluster1 and the state of the relationship
	statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
				'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
				' MATCH (user_clus2:User_Defined_Cluster) '\
				'WHERE ID(user_clus2) =' + str(remote(user_cluster2)._id) + \
				' MATCH (user_clus2)-[r1:CLUSTERED]->(nodes_to_link) '\
				'OPTIONAL MATCH (user_clus2)-[r2:GENERATED_BY]-() '\
				'OPTIONAL MATCH (user_clus2)-[r3:INVALIDATED_BY]-() '\
				'OPTIONAL MATCH (user_clus2)-[r4:VALIDATED_BY]-() '\
				'DELETE r2 '\
				'DELETE r3 '\
				'DELETE r4 '\
				'RETURN nodes_to_link,r1.isValidated '

	#We have to determine which relationship_state to take NotValide > Valide > Unknow
	cursor = tx.run(statement)
	for record in cursor:
		state_rel_second = str(record[1])
		#We look if the user_clus1 is already connected to one of the tracks of the user_clus2
		statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
					'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
					' MATCH (node_to_link) '\
					'WHERE ID(node_to_link) =' + str(remote(record[0])._id) + \
					' OPTIONAL MATCH (user_clus1)-[r:CLUSTERED]->(node_to_link) '\
					'RETURN r.isValidated '
		cursor2 = tx.run(statement)
		state_rel_first = ""
		for record2 in cursor2:
			if record2[0] is not None:
				state_rel_first = str(record2[0])
		if state_rel_first == "":
			state_rel = state_rel_second
		elif state_rel_first == "NotValide" or state_rel_second == "NotValide":
			state_rel = state_rel_first
		elif state_rel_first == "Valide" or state_rel_second == "Valide":
			state_rel = state_rel_first
		else:
			state_rel = "Unknow"

		statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
					'WHERE ID(user_clus1) =' + str(remote(user_cluster1)._id) + \
					' MATCH (node_to_link) '\
					'WHERE ID(node_to_link) =' + str(remote(record[0])._id) + \
					' OPTIONAL MATCH (user_clus1)-[r:CLUSTERED]->(node_to_link) '\
					' DELETE r '\
					'CREATE (user_clus1)-[:CLUSTERED {isValidated:"' + state_rel + '"}]->(node_to_link) '
		tx.run(statement)
	

	statement = ' MATCH (user_clus2:User_Defined_Cluster) '\
				'WHERE ID(user_clus2) =' + str(remote(user_cluster2)._id) + \
				' MATCH (user_clus2)-[r]->() '\
				'DELETE r '\
				'DELETE (user_clus2)'
	tx.run(statement)
	tx.commit()
	return str(remote(user_cluster1)._id)




def merge_userCluster_Cluster(graph,user_cluster,cluster):
	tx = graph.begin()
	#If the user_clus a label we don't touch it
	statement = 'MATCH (user_clus:User_Defined_Cluster) '\
				'WHERE ID(user_clus) =' + str(remote(user_cluster)._id) + \
				' OPTIONAL MATCH (user_clus)-[:LABEL]->(lab:Label) '\
				'RETURN lab'
	cursor = tx.run(statement)
	record = cursor.evaluate()
	#If the user_clus has no label we look if the cluster has one.
	if record is None:
		statement = 'MATCH (clus:Cluster) '\
				'WHERE ID(clus) =' + str(remote(cluster)._id) + \
				' OPTIONAL MATCH (clus)-[:LABEL]->(lab:Label) '\
				'RETURN lab'
		cursor = tx.run(statement)
		record = cursor.evaluate()
		#If the cluster has one we linked this label to the user_clus
		if record is not None:
			statement = 'MATCH (user_clus:User_Defined_Cluster) '\
						'WHERE ID(user_clus) =' + str(remote(user_cluster)._id) + \
						' MATCH (clus:Cluster) '\
						'WHERE ID(clus) =' + str(remote(cluster)._id) + \
						' MATCH (clus)-[:LABEL]->(lab:Label) '\
						'CREATE (user_clus)-[:LABEL]->(lab)'
			tx.run(statement)
	#Now we link the user_clus and the clus. And we link the tracks from the clus to the user_clus
	statement = 'MATCH (user_clus:User_Defined_Cluster) '\
				'WHERE ID(user_clus) =' + str(remote(user_cluster)._id) + \
				' MATCH (clus:Cluster) '\
				'WHERE ID(clus) =' + str(remote(cluster)._id) + \
				' MATCH (clus)-[r:CLUSTERED]->(tracks_to_link:Track) '\
				'MERGE (user_clus)-[rel:CLUSTERED]->(clus) '\
				'SET rel.isValidated = "Valide" '\
				'RETURN tracks_to_link,r.isValidated'
	cursor = tx.run(statement)
	for record in cursor:
		state_rel_second = str(record[1])
		#We look if the user_clus is already connected to one of the tracks of the clus
		statement = 'MATCH (user_clus:User_Defined_Cluster) '\
					'WHERE ID(user_clus) =' + str(remote(user_cluster)._id) + \
					' MATCH (node_to_link) '\
					'WHERE ID(node_to_link) =' + str(remote(record[0])._id) + \
					' OPTIONAL MATCH (user_clus)-[r:CLUSTERED]->(node_to_link) '\
					'RETURN r.isValidated '
		cursor2 = tx.run(statement)
		record2 = cursor2.evaluate()
		state_rel_first = ""
		if record2 is not None:
			state_rel_first = str(record2)
		#We define which state to give to the relationship 
		if state_rel_first == "":
			state_rel = state_rel_second
		elif state_rel_first == "NotValide" or state_rel_second == "NotValide":
			state_rel = state_rel_first
		elif state_rel_first == "Valide" or state_rel_second == "Valide":
			state_rel = state_rel_first
		else:
			state_rel = "Unknow"
		statement = 'MATCH (user_clus:User_Defined_Cluster) '\
					'WHERE ID(user_clus) =' + str(remote(user_cluster)._id) + \
					' MATCH (node_to_link) '\
					'WHERE ID(node_to_link) =' + str(remote(record[0])._id) + \
					' OPTIONAL MATCH (user_clus)-[r:CLUSTERED]->(node_to_link) '\
					'DELETE r '\
					' CREATE (user_clus)-[:CLUSTERED {isValidated:"' + state_rel + '"}]->(node_to_link)'
		tx.run(statement)
	tx.commit()
	return str(remote(user_cluster)._id)



	

def merge_Cluster_Cluster(graph,cluster1,cluster2):
	tx = graph.begin()
	statement = 'MATCH (clus1:Cluster) '\
				'WHERE ID(clus1) =' + str(remote(cluster1)._id) + \
				' OPTIONAL MATCH (clus1)-[:LABEL]->(lab1:Label) '\
				'WITH lab1 '\
				'MATCH (clus2:Cluster) '\
				'WHERE ID(clus2) =' + str(remote(cluster2)._id) + \
				' OPTIONAL MATCH (clus2)-[:LABEL]->(lab2:Label) '\
				'RETURN lab1.name,lab2.name'
	cursor = tx.run(statement)
	#We have to set the right label
	label_clus2 = ""
	label_clus1 = ""
	for record in cursor:
		if record[0] is not None:
			label_clus1 = str(record[0])
		if record[1] is not None:
			label_clus2 = str(record[1])
	label = ""
	if not(label_clus1 == "" and label_clus2 == ""):
		if label_clus1 == "":
			label = label_clus2
		elif label_clus2 == "":
			label = label_clus1
		elif label_clus1 == label_clus2:
			label = label_clus1
		else:
			label = label_clus1 + "/" + label_clus2

	#We create a new user_cluster and we link it to the 2 cluster
	statement = 'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
				'CREATE (user_defined:User_Defined_Cluster) '\
				'CREATE (user_defined)-[:GENERATED_BY]->(algo)'
	if label != "":
		statement += 'WITH user_defined '\
					'MERGE (lab:Label {name:"' + label + '"}) '\
					'WITH user_defined,lab '\
					'MATCH (algo:Algo {name:"' + 'user_defined' + '"}) '\
					'OPTIONAL MATCH (lab)-[r:IDENTIFY_BY]-() '\
					'FOREACH (x in CASE WHEN r IS NULL THEN [1] ELSE [] END | ' \
						'CREATE (lab)-[:IDENTIFY_BY]->(algo)) '\
					'CREATE (user_defined)-[:LABEL]->(lab) '
	statement += 'WITH user_defined '\
				'MATCH (clus1:Cluster) '\
				'WHERE ID(clus1) =' + str(remote(cluster1)._id) + \
				' MATCH (clus2:Cluster) '\
				'WHERE ID(clus2) =' + str(remote(cluster2)._id) + \
				' CREATE (user_defined)-[:CLUSTERED]->(clus1) '\
				'CREATE (user_defined)-[:CLUSTERED]->(clus2) '\
				'RETURN user_defined'
	cursor = tx.run(statement)
	user_node = cursor.evaluate()
	#We link the user_clus to the tracks of the first cluster
	statement = 'MATCH (clus1:Cluster) '\
				'WHERE ID(clus1) =' + str(remote(cluster1)._id) + \
				' MATCH (clus1)-[r:CLUSTERED]->(tracks:Track) '\
				'RETURN tracks,r.isValidated'
	cursor = tx.run(statement)
	for record in cursor:
		track_id = str(remote(record[0])._id)
		state_rel = str(record[1])
		statement = 'MATCH (user_clus:User_Defined_Cluster) '\
					'WHERE ID(user_clus) =' + str(remote(user_node)._id) + \
					' MATCH (track:Track) '\
					'WHERE ID(track) =' + track_id + \
					' WITH user_clus,track '\
					'CREATE (user_clus)-[:CLUSTERED {isValidated:"' + state_rel + '"}]->(track)'
		tx.run(statement)
	
	#We link the user_clus to the tracks of the second cluster
	statement = 'MATCH (clus2:Cluster) '\
				'WHERE ID(clus2) =' + str(remote(cluster2)._id) + \
				' MATCH (clus2)-[r:CLUSTERED]->(tracks:Track) '\
				'RETURN tracks,r.isValidated'
	cursor = tx.run(statement)
	for record in cursor:
		state_rel1 = str(record[1])
		statement = 'MATCH (user_clus:User_Defined_Cluster) '\
					'WHERE ID(user_clus) =' + str(remote(user_node)._id) + \
					' OPTIONAL MATCH (user_clus)-[r:CLUSTERED]->(tr:Track) '\
					'WHERE ID(tr) =' + str(remote(record[0])._id) + \
					' RETURN r.isValidated'
		cursor2 = tx.run(statement)
		record2 = cursor2.evaluate()
		state_rel2 = ""
		if record2 is not None:
			state_rel2 = str(record2)

		if state_rel1 == "":
			state_rel = state_rel2
		elif state_rel1 == "NotValide" or state_rel2 == "NotValide":
			state_rel = state_rel1
		elif state_rel1 == "Valide" or state_rel2 == "Valide":
			state_rel = state_rel1
		else:
			state_rel = "Unknow"

		statement = 'MATCH (user_clus:User_Defined_Cluster) '\
					'WHERE ID(user_clus) =' + str(remote(user_node)._id) + \
					' MATCH (track:Track) '\
					'WHERE ID(track)=' + str(remote(record[0])._id) + \
					' OPTIONAL MATCH (user_clus)-[r:CLUSTERED]->(track) '\
					'DELETE r '\
					'CREATE (user_clus)-[:CLUSTERED {isValidated:"' + state_rel + '"}]->(track)'
		tx.run(statement)
	tx.commit()
	load_texture(graph,user_node)
	return str(remote(user_node)._id)



#Merge clusters into a user_defined cluster when they have a Track in common.
def merge_overlap_cluster(graph):
	tmps1 = time.time()
	tx = graph.begin()

	#first : When we have to merge 2 cluster
	notfinish =True
	compt = 0
	print "STEP 1"
	Do_not_cluster_list = []
	while notfinish:
		statement = 'MATCH (clus1:Cluster)-[:CLUSTERED]->(tr:Track)<-[:CLUSTERED]-(clus2:Cluster) '\
					'WHERE NOT((clus1)<-[:CLUSTERED]-(:User_Defined_Cluster)) AND NOT((clus2)<-[:CLUSTERED]-(:User_Defined_Cluster)) '\
					'WITH Collect(DISTINCT [clus1,clus2]) AS doublets '\
					'RETURN doublets'
		cursor = tx.run(statement)
		list_doublet = cursor.evaluate()
		notfinish = False
		
		compt_doublet = 0
		while notfinish is False and compt_doublet < len(list_doublet):
			doublet = list_doublet[compt_doublet]
			compt_doublet += 1
			if doublet not in Do_not_cluster_list:
				statement = 'MATCH (clus1:Cluster) '\
							'WHERE ID(clus1)=' + str(remote(doublet[0])._id) +\
							' MATCH (clus2:Cluster) '\
							'WHERE ID(clus2)=' + str(remote(doublet[1])._id) +\
							' MATCH (clus1:Cluster)-[:CLUSTERED]->(tr_commun:Track)<-[:CLUSTERED]-(clus2:Cluster) '\
							'MATCH (clus1:Cluster)-[:CLUSTERED]->(tr1:Track) '\
							'MATCH (clus2:Cluster)-[:CLUSTERED]->(tr2:Track) '\
							'WITH count(DISTINCT tr_commun) AS nb, count(DISTINCT tr1) AS nb1,count(DISTINCT tr2) AS nb2 '\
							'RETURN [nb,nb1,nb2]'
				cursor = tx.run(statement)
				tracks = cursor.evaluate()
				if (float(tracks[0]) / tracks[1] >= 0.5) or (float(tracks[0]) / tracks[2] >= 0.5):
					notfinish = True
					compt += 1
					print compt
					merge_Cluster_Cluster(graph,doublet[0],doublet[1])
				else:
					Do_not_cluster_list.append(doublet)


	
	#2nd : When we have to merge 1 user_defined cluster and a cluster
	notfinish =True
	print "STEP 2"
	Do_not_cluster_list = []
	while notfinish:
		statement = 'MATCH (user_clus:User_Defined_Cluster)-[:CLUSTERED]->(:Track)<-[:CLUSTERED]-(clus:Cluster) '\
					'WHERE NOT (clus)<-[:CLUSTERED]-(:User_Defined_Cluster) '\
					'WITH Collect(DISTINCT [user_clus,clus]) AS doublets '\
					'RETURN doublets'
		cursor = tx.run(statement)
		list_doublet = cursor.evaluate()
		notfinish = False
		compt_doublet = 0
		while notfinish is False and compt_doublet < len(list_doublet):
			doublet = list_doublet[compt_doublet]
			compt_doublet += 1
			if doublet not in Do_not_cluster_list:
				statement = 'MATCH (user_clus:User_Defined_Cluster) '\
							'WHERE ID(user_clus)=' + str(remote(doublet[0])._id) +\
							' MATCH (clus:Cluster) '\
							'WHERE ID(clus)=' + str(remote(doublet[1])._id) +\
							' MATCH (user_clus:User_Defined_Cluster)-[:CLUSTERED]->(tr_commun:Track)<-[:CLUSTERED]-(clus:Cluster) '\
							'MATCH (user_clus:User_Defined_Cluster)-[:CLUSTERED]->(tr1:Track) '\
							'MATCH (clus:Cluster)-[:CLUSTERED]->(tr2:Track) '\
							'WITH count(DISTINCT tr_commun) AS nb, count(DISTINCT tr1) AS nb1,count(DISTINCT tr2) AS nb2 '\
							'RETURN [nb,nb1,nb2]'
				cursor = tx.run(statement)
				tracks = cursor.evaluate()
				if (float(tracks[0]) / tracks[1] >= 0.5) or (float(tracks[0]) / tracks[2] >= 0.5):
					notfinish = True
					compt += 1
					print compt
					merge_userCluster_Cluster(graph,doublet[0],doublet[1])
				else:
					Do_not_cluster_list.append(doublet)


	#3rd : When we have to merge 2 user_defined_cluster
	notfinish =True
	print "STEP 3"
	Do_not_cluster_list = []
	while notfinish:
		statement = 'MATCH (user_clus1:User_Defined_Cluster)-[:CLUSTERED]->(:Track)<-[:CLUSTERED]-(user_clus2:User_Defined_Cluster) '\
					'WITH Collect(DISTINCT [user_clus1,user_clus2]) AS doublets '\
					'RETURN doublets'
		cursor = tx.run(statement)
		list_doublet = cursor.evaluate()
		notfinish = False
		compt_doublet = 0
		while notfinish is False and compt_doublet < len(list_doublet):
			doublet = list_doublet[compt_doublet]
			compt_doublet += 1
			if doublet not in Do_not_cluster_list:
				statement = 'MATCH (user_clus1:User_Defined_Cluster) '\
							'WHERE ID(user_clus1)=' + str(remote(doublet[0])._id) +\
							' MATCH (user_clus2:User_Defined_Cluster) '\
							'WHERE ID(user_clus2)=' + str(remote(doublet[1])._id) +\
							' MATCH (user_clus1:User_Defined_Cluster)-[:CLUSTERED]->(tr_commun:Track)<-[:CLUSTERED]-(user_clus2:User_Defined_Cluster) '\
							'MATCH (user_clus1:User_Defined_Cluster)-[:CLUSTERED]->(tr1:Track) '\
							'MATCH (user_clus2:User_Defined_Cluster)-[:CLUSTERED]->(tr2:Track) '\
							'WITH count(DISTINCT tr_commun) AS nb, count(DISTINCT tr1) AS nb1,count(DISTINCT tr2) AS nb2 '\
							'RETURN [nb,nb1,nb2]'
				cursor = tx.run(statement)
				tracks = cursor.evaluate()
				if (float(tracks[0]) / tracks[1] >= 0.5) or (float(tracks[0]) / tracks[2] >= 0.5):
					notfinish = True
					compt += 1
					print compt
					merge_userCluster_userCluster(graph,doublet[0],doublet[1])
				else:
					Do_not_cluster_list.append(doublet)

	tx.commit()
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."

def load_texture(graph,cluster):
	tx = graph.begin()
	image_str = ""
	statement = 'MATCH (n:Cluster) '\
				'WHERE Not (n)-[:CLUSTERED]-() '\
				'MATCH (n)-[r]->() '\
				'DELETE r '
	graph.run(statement)
	statement = 'MATCH (n:Cluster) '\
				'WHERE Not (n)-[:CLUSTERED]-() '\
				'DELETE n '
	graph.run(statement)
	if type(cluster) is str :
		sentence = cluster
	else:
		print cluster
		sentence = str(remote(cluster)._id)
		print sentence
	statement = 'MATCH (clus) '\
				'WHERE ID(clus)=' + sentence + \
				' MATCH (clus)-[:CLUSTERED]->(track:Track) '\
				'WITH track LIMIT 1 '\
				'MATCH (track)<-[:TRACK]-()<-[:COMPOSED_OF]-(:Jt)-[:BROADCAST_ON]->(day:Day)<-[:DAY]-(month:Month)<-[:MONTH]-(year:Year) '\
				'RETURN track.num_shot,track.num_track,day.name,month.name,year.name LIMIT 1'
	cursor = tx.run(statement)
	for record in cursor:
		print record
		year = str(record[4])
		month = str(record[3])
		day = str(record[2])
		num_shot = str(record[0])
		num_track = str(record[1])
		if len(day) == 1:
			day = "0" + day	
		if len(month) == 1:
			month = "0" + month

		f = open(texture_path_local + "image_track_" + str(year) +".json")
		json_texture_year = json.load(f) 
		f.close()

		image_str = str(json_texture_year[month + "_" + day][num_shot + "_" + num_track])
		statement = 'MATCH (clus) '\
					'WHERE ID(clus)=' + sentence +\
					' SET clus.texture ="' + image_str + '" '\
					'RETURN clus.texture '

		record = graph.run(statement)
	tx.commit
	return image_str


def load_all_texture(graph):
	tmps1 = time.time()
	tx = graph.begin()
	compt = 0
	statement = 'MATCH (clus:User_Defined_Cluster) '\
				'RETURN DISTINCT clus '\
				'UNION '\
				'MATCH (clus:Cluster) '\
				'WHERE NOT (clus)<-[:CLUSTERED]-() '\
				'RETURN DISTINCT clus '
	cursor = tx.run(statement)
	tx.commit()
	for record in cursor:
		compt += 1
		print compt
		load_texture(graph,record[0])

	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."

# example/testing
if __name__ == '__main__':
	#server = GraphServer()
	#server.start()

	print "Connexion to the Neo4j graph"
	authenticate(serveur_neo, id_neo, password_neo)
	graph = Graph(url_neo)
	print "Done"


	#update_topic_graph(graph)
	#update_track_graph(graph)
	#update_track_overlap(graph)
	#update_cluster_annoted(graph)
	#correct_name(graph)
	#update_cluster_unannoted(graph)
	#merge_overlap_cluster(graph)
	load_all_texture(graph)