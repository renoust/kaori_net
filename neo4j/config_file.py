#!/usr/bin/python
# -*-coding:Utf-8 -*

								#configuration for data_format
#data location
topic_seg_path = ""
track_path = ""
annotation_path = ""
unannoted_cluster_HighPrecisionLowRecall_path = ""
unannoted_cluster_LowPrecisionHighRecall_path = ""
unannoted_cluster_local = ""
image_path = ""
image_path_local = ""

#You have to generate a private/public key couple of your password :
#$ ssh-keygen -t rsa (use no passphrase)
#Then copy the public key on the serveur with the command :
#$ ssh-copy-id user@serveur

#The serveur you want to use and where your private key is.

serveur = ""
port = 0
user = ""
private_key = ""

										#configuration for neo4j_commit
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


