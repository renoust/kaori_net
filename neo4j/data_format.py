#!/usr/bin/python
# -*-coding:Utf-8 -*

# sublime_jedi.sublime-settings
{
    "sublime_completions_visibility": "default"
}

import os
import json
import paramiko
import time
from PIL import Image
import base64
import subprocess

#Functions which load the data on your computer.

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

port = ""
user = ""
private_key = ""

from config_file import *

#Return a client of the serveur where the data are located.
def createSSHClient(server, port, user, password, private_key):
	client = paramiko.SSHClient()
	client.load_system_host_keys()
	client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
	client.connect(server, port, user, password, key_filename=private_key)
	return client

#Load the segmentation_topic data and create a json file on your computer nammed topic_seg_data.json
def load_topic_seg(client):
	print ""
	print "Topic segmentation data loading..."
	tmps1 = time.time()
	(stdin, stdout, stderr) = client.exec_command("cat " + topic_seg_path)
	json_data = json.load(stdout)
	with open('topic_seg_data.json', 'w') as f:
		json.dump(json_data, f, indent=4)
	f.close()
	tmps2 = time.time() - tmps1
	print "Done in " + str(int(tmps2)) + " second(s)."

#Load the tracks data and create a json file on your computer nammed tracks_data.json. This takes arround 7 hours.
def load_tracks(client):
	tmps1 = time.time()
	print ""
	print "Tracks data loading..."
	(stdin, stdout, stderr) = client.exec_command("ls " + track_path + " | tr '\\n' ' '")
	years = [ x for x in str(stdout.readlines()[0]).split(" ") if x!='' ] #We have a list of all the years
	tracks_dict = {}
	compt = 0 
	for year in years:
			(stdin, stdout, stderr) = client.exec_command("ls " + track_path + "/" + year + " | tr '\\n' ' '")
			days = [ x for x in str(stdout.readlines()[0]).split(" ") if x!='' and x[-3:] != "lst"] #We have a list of all the days.
			days_dict = {}
			for day in days:
				(stdin, stdout, stderr) = client.exec_command("ls " + track_path + "/" + year + "/" + day + " | tr '\\n' ' '")
				tracks = [ x for x in str(stdout.readlines()[0]).split(" ") if x!=''] #We have a list of all the tracks
				tracks_day_dict = {}
				date = day.split("_")
				for track_file in tracks:
					track = track_file.split("-") # track is like : 2001_05_17_19_00-shot17_9-Track1.face.lst
					(stdin, stdout, stderr) = client.exec_command("cat " + track_path + "/" + year + "/" + day + "/" + track_file)
					frame_list = stdout.readlines()
					if frame_list: #The file is sometimes empty. In this case we don't load the track.
						first_frame = str(frame_list[0].splitlines()[0].split(" #$# ")[0].split("-")[3][5:])
						last_frame = str(frame_list[-1].splitlines()[0].split(" #$# ")[0].split("-")[3][5:])
					else:
						first_frame = -1
						last__frame = -1
					info_track = {}
					info_track["first_frame"] = first_frame
					info_track["last_frame"] = last_frame
					tracks_day_dict[track[1][7:]+ "_" + track[2].split(".")[0][5:]] = info_track
				compt += 1
				print compt
				#Every 100 tracks we oppen a new client. Otherwise the client will shut down by himself.
				if compt %100 == 0:
					client.close()
					client = createSSHClient(serveur, port, user, "", private_key)

				days_dict[date[1]+"_"+date[2]] = tracks_day_dict
			tracks_dict[year] = days_dict
	with open('tracks_data.json', 'w') as f:
		json.dump(tracks_dict, f, indent=4)
	f.close()
	tmps2 = time.time() - tmps1
	print "Done in :  " + str(int(tmps2)) + " second(s)."


#Load the annotation data and create a json file on your computer nammed annotation_data.json.
def load_annotation(client):
	print ""
	print "Annotation data loading..."
	tmps1 = time.time()
	(stdin, stdout, stderr) = client.exec_command("cat " + annotation_path)
	annotation_dict = {}
	compt = 0
	for annotation in stdout:
		info_annotation = str(annotation).split(" #$# ")
		if info_annotation[3] == "news7-lv":
			match_track = info_annotation[1].split("-")
			name = info_annotation[5][:-1]
			date = match_track[0].split("_")
			num_shot = match_track[1].split("_")[1] + "_" + match_track[2][5:]
			key = date[0] + "_" + date[1] + "_" + date[2] + "_" + num_shot
			annotation_dict[key] = name

	with open('annotation_data.json', 'w') as f:
		json.dump(annotation_dict, f, indent=4)
	f.close()
	tmps2 = time.time() - tmps1
	print "Done in :  " + str(int(tmps2)) + " second(s)."

#unannoted_cluster_HighPrecisionLowRecall and unannoted_cluster_LowPrecisionHighRecall have to be found manually.
#This function parse the file correctly and merge the two algorithm into one file
def load_cluster_unannoted(cluster_paths):
	print ""
	print "Unanotted cluster loading..."
	tmps1 = time.time()
	cluster2_dict = {}
	for cluster_path in cluster_paths:
		file = os.popen("cat " + cluster_path)
		compt = 0
		clusters_dict = {}
		for cluster in file:
			info_cluster = cluster.split(" #$# ")
			name_id = info_cluster[0][8:]
			tracks_list = []
			for track in info_cluster[2:]:
				infotrack = track.split("-")
				date = infotrack[0].split("_")
				value = date[0] + "_" + date[1] + "_" + date[2] + "_" + infotrack[1].split("_")[1] + "_" + infotrack[2].split("+")[0][5:]
				tracks_list.append(value)
			if tracks_list != []:
				clusters_dict[name_id] = tracks_list
		cluster2_dict[cluster_path.split("/")[8].split("-")[4].split(".")[0]] = clusters_dict

	with open(unannoted_cluster_local, 'w') as f:
		json.dump(cluster2_dict, f, indent=4)
	f.close()
	tmps2 = time.time() - tmps1
	print "Done in :  " + str(int(tmps2)) + " second(s)."



def load_image(client):
	print ""
	print "Image loading..."
	tmps1 = time.time()
	(stdin, stdout, stderr) = client.exec_command("ls " + image_path + " | tr '\\n' ' '")
	years = [ x for x in str(stdout.readlines()[0]).split(" ") if x!='' ] #We have a list of all the years
	compt = 0
	for year in years:
		#if int(year) == 2001:
		(stdin, stdout, stderr) = client.exec_command("ls " + image_path + "/" + year + " | tr '\\n' ' '")
		days = [ x for x in str(stdout.readlines()[0]).split(" ") if x!='' and x!="cache.t7"] #We have a list of all the days.
		days_dict = {}
		for day in days:
			#if day == "2001_12_28_19_00":
			(stdin, stdout, stderr) = client.exec_command("ls " + image_path + "/" + year + "/" + day + " | tr '\\n' ' '")
			tracks = [ x for x in str(stdout.readlines()[0]).split(" ") if x!='' and x!="cache.t7"] #We have a list of all the tracks
			date = day.split("_")
			track_dict = {}
			for track_file in tracks:
				track = track_file.split("-")
				(stdin, stdout, stderr) = client.exec_command("ls " + image_path + "/" + year + "/" + day + "/" + track_file + " | head -1" + " | tr '\\n' ' '") #ne prend que la première image.
				(stdin, stdout, stderr) = client.exec_command("cat " + image_path + "/" + year + "/" + day + "/" + track_file + "/" + str(stdout.readlines()[0]))
				track_dict[track[1].split("_")[1] + "_" + track[2][5:]] = stdout.read().encode('base64')
			days_dict[date[1] + "_" + date[2]] = track_dict
			compt += 1
			print compt
			if compt %100 == 0:
				client.close()
				client = createSSHClient(serveur, port, user, "", private_key)
		file_name = 'image_track_' + str(year) + '.json'
		with open(file_name, 'w') as f:
			json.dump(days_dict, f, indent=4)
		f.close()
		print str(year) + " is done"

	tmps2 = time.time() - tmps1
	print "Done in :  " + str(int(tmps2)) + " second(s)."


def merge_image_track():
	print ""
	print "Merge Image..."
	tmps1 = time.time()
	files =  os.popen("ls " + image_path_local + "/tracks_image_year").read().split("\n")[:-1]
	image_dict = {}
	for year_file in files:
		print str(year_file)
		f = open(image_path_local + "tracks_image_year/" + year_file)
		json_data = json.load(f) #we load the segmentation_topic data.
		f.close()
		image_dict[str(year_file.split(".")[0].split("_")[2])] = json_data

	with open(image_path_local + "image_tracks", 'w') as f:
		json.dump(image_dict, f, indent=4)
	f.close()




# example/testing
if __name__ == '__main__':
	client = createSSHClient(serveur, port, user, "", private_key)
	#load_topic_seg(client)
	#load_tracks(client)
	#load_annotation(client)
	#load_image(client)
	#merge_image_track()
	load_cluster_unannoted([unannoted_cluster_HighPrecisionLowRecall_path,unannoted_cluster_LowPrecisionHighRecall_path])
	client.close()