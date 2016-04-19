import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from RamSQL_TagUnique_Sum import RamSQL_Tag
from RamSQL_UserUnique_Sum import RamSQL_User

"""
####################################################################
# create table TagUnique_All
"""
def TagUnique_All_Init(connection):

	#Comd
	Comd_TagUnique_Init = "\
CREATE TABLE IF NOT EXISTS TagUnique_All\n\
(\n\
	tagText varchar(255) PRIMARY KEY NOT NULL,\n\
	tweetID_list_text LONGTEXT,\n\
	totalCall bigint NOT NULL,\n\
	totalCall_window_text LONGTEXT,\n\
	Score1_Ave float,\n\
	Score1_Std float,\n\
	Score2_Ave float,\n\
	Score2_Std float,\n\
	tagScore1_text LONGTEXT,\n\
	tagNcall1_text LONGTEXT,\n\
	tagScore2_text LONGTEXT,\n\
	tagNcall2_text LONGTEXT,\n\
	Score1_Ave_window_text LONGTEXT,\n\
	Score1_Std_window_text LONGTEXT,\n\
	Score2_Ave_window_text LONGTEXT,\n\
	Score2_Std_window_text LONGTEXT,\n\
	user_counter_text LONGTEXT,\n\
	user_degree_abs float,\n\
	user_counter_weight_text LONGTEXT,\n\
	user_degree_weighted float,\n\
	user_degree_abs_window_text LONGTEXT,\n\
	user_degree_weighted_window_text LONGTEXT,\n\
	tag_counter_text LONGTEXT,\n\
	tag_degree_abs float,\n\
	tag_counter_weight_text LONGTEXT,\n\
	tag_degree_weighted float,\n\
	tag_degree_abs_window_text LONGTEXT,\n\
	tag_degree_weighted_window_text LONGTEXT\n\
)"
	#print Comd_TagUnique_Init
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_TagUnique_Init)
		# commit commands
		print "TagUnique_All Initialized"
		connection.commit()
	finally:
		pass

"""
####################################################################
# extract information from tagunique_window
"""

def SQL_tagunique_table_extract(connection, tableName, RamSQL_TagUnique_All):

	####################################################################
	# get tagText and initiate RamSQL_TagUnique_All
	# get Scores data and update
	# comd
	comd_Scores = "\
select tagText, tagScore1_text, tagNcall1_text, tagScore2_text, tagNcall2_text, totalCall \n\
from "+tableName+"\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_Scores)
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:		
				# extract
				tagText = str(entry['tagText'])
				input_tagScore1 = str(entry['tagScore1_text'])
				input_tagNcall1 = str(entry['tagNcall1_text'])
				input_tagScore2 = str(entry['tagScore2_text'])
				input_tagNcall2 = str(entry['tagNcall2_text'])
				input_totalCall = int(entry['totalCall'])
				# check if tagText exists
				# (if not) initiate RamSQL_TagUnique_All
				if tagText not in RamSQL_TagUnique_All:
					RamSQL_TagUnique_All[tagText] = RamSQL_Tag(tagText)
				# load Scores into RamSQL_TagUnique_All
				RamSQL_TagUnique_All[tagText].ScoreInfor_Insert(table=tableName, 
					input_tagScore1=input_tagScore1, input_tagNcall1=input_tagNcall1, 
					input_tagScore2=input_tagScore2, input_tagNcall2=input_tagNcall2, input_totalCall=input_totalCall)
	finally:
		pass

	####################################################################
	# get User information and Tag information and update
	# comd
	comd_UserInfor = "\
select  tagText, user_counter_text, tag_counter_text, tweetID_list_text\n\
from "+tableName+"\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_UserInfor)
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:
				# extract
				tagText = str(entry['tagText'])
				Input_UserCounter = str(entry['user_counter_text'])
				Input_TagCounter = str(entry['tag_counter_text'])
				Input_tweetID_list = str(entry['tweetID_list_text'])				
				# load into RamSQL_TagUnique_All
				RamSQL_TagUnique_All[tagText].UserInfor_Insert(table=tableName, Input_UserCounter=Input_UserCounter)
				RamSQL_TagUnique_All[tagText].TagInfor_Insert(table=tableName, Input_TagCounter=Input_TagCounter)
				RamSQL_TagUnique_All[tagText].tweetID_Insert(Input_tweetID_list=Input_tweetID_list)
	finally:
		pass

	return RamSQL_TagUnique_All

"""
####################################################################
# load RamSQL_TagUnique_All into DB table TagUnique_All 
"""

def TagUnique_All_Insert(connection, RamSQL_TagUnique_All):
	
	# loop through keys of RamSQL_TagUnique_All
	for key in RamSQL_TagUnique_All:
		# pointer
		current = RamSQL_TagUnique_All[key]
		# INSERT tagText, tweetID_list_text, totalCall, totalCall_window_text
		comd_INSERT = "\
INSERT INTO TagUnique_All (\
tagText, tweetID_list_text, totalCall, totalCall_window_text,\
Score1_Ave, Score1_Std, Score2_Ave, Score2_Std,\
tagScore1_text, tagNcall1_text, tagScore2_text, tagNcall2_text,\
user_counter_text, user_degree_abs, user_counter_weight_text, user_degree_weighted,\
Score1_Ave_window_text, Score1_Std_window_text, Score2_Ave_window_text, Score2_Std_window_text,\
user_degree_abs_window_text, user_degree_weighted_window_text,\
tag_counter_text, tag_degree_abs, tag_counter_weight_text, tag_degree_weighted,\
tag_degree_abs_window_text, tag_degree_weighted_window_text)\n\
VALUES (\
'{}', '{}', {}, '{}',\
{}, {}, {}, {},\
'{}','{}','{}','{}',\
'{}', {},'{}', {},\
'{}','{}','{}','{}',\
'{}','{}',\
'{}', {},'{}', {},\
'{}','{}'\
)\n\
ON DUPLICATE KEY UPDATE totalCall = {}".format(
current.tagText, current.tweetID_list_str(), current.totalCall, current.totalCall_window_str(),
current.Score1_Ave, current.Score1_Std, current.Score2_Ave, current.Score2_Std,
current.tagScore1_str(), current.tagNcall1_str(), current.tagScore2_str(), current.tagNcall2_str(),
current.user_counter_str(), current.user_degree_abs, current.user_counter_weight_str(), current.user_degree_weighted, 
current.Score1_Ave_window_str(), current.Score1_Std_window_str(), current.Score2_Ave_window_str(), current.Score2_Std_window_str(), 
current.user_degree_abs_window_str(), current.user_degree_weighted_window_str(), 
current.tag_counter_str(), current.tag_degree_abs, current.tag_counter_weight_str(), current.tag_degree_weighted, 
current.tag_degree_abs_window_str(), current.tag_degree_weighted_window_str(),
current.totalCall)
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_INSERT)
			# commit commands
			connection.commit()
		finally:
			pass


"""
####################################################################
# create table UserUnique_All
"""

def UserUnique_All_Init(connection):

	#Comd
	Comd_UserUnique_Init = "\
CREATE TABLE IF NOT EXISTS UserUnique_All\n\
(\n\
	userID bigint PRIMARY KEY NOT NULL,\n\
	userName varchar(255),\n\
	followers bigint,\n\
	friends bigint,\n\
	tweetID_list_text LONGTEXT,\n\
	totalAction bigint NOT NULL,\n\
	totalAction_window_text LONGTEXT,\n\
	userScore1_text LONGTEXT,\n\
	userNcall1_text LONGTEXT,\n\
	userScore2_text LONGTEXT,\n\
	userNcall2_text LONGTEXT,\n\
	Score1_Ave float,\n\
	Score1_Std float,\n\
	Score2_Ave float,\n\
	Score2_Std float,\n\
	Score1_Ave_window_text LONGTEXT,\n\
	Score1_Std_window_text LONGTEXT,\n\
	Score2_Ave_window_text LONGTEXT,\n\
	Score2_Std_window_text LONGTEXT,\n\
	Muser_counter_text LONGTEXT,\n\
	Muser_degree_abs float,\n\
	Muser_counter_weight_text LONGTEXT,\n\
	Muser_degree_weighted float,\n\
	Muser_degree_abs_window_text LONGTEXT,\n\
	Muser_degree_weighted_window_text LONGTEXT,\n\
	Ruser_counter_text LONGTEXT,\n\
	Ruser_degree_abs float,\n\
	Ruser_counter_weight_text LONGTEXT,\n\
	Ruser_degree_weighted float,\n\
	Ruser_degree_abs_window_text LONGTEXT,\n\
	Ruser_degree_weighted_window_text LONGTEXT,\n\
	tag_counter_text LONGTEXT,\n\
	tag_degree_abs float,\n\
	tag_counter_weight_text LONGTEXT,\n\
	tag_degree_weighted float,\n\
	tag_degree_abs_window_text LONGTEXT,\n\
	tag_degree_weighted_window_text LONGTEXT\n\
)"
	#print Comd_TagUnique_Init
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_UserUnique_Init)
		# commit commands
		print "UserUnique_All Initialized"
		connection.commit()
	finally:
		pass

"""
####################################################################
# extract information from userunique_window
"""

def SQL_userunique_table_extract(connection, tableName, RamSQL_UserUnique_All):

	####################################################################
	# get tagText and initiate RamSQL_TagUnique_All
	# get Scores data and update
	# comd
	comd_Scores = "\
select userID, userName, followers, friends, userScore1_text, userNcall1_text, userScore2_text, userNcall2_text, totalAction \n\
from "+tableName+"\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_Scores)
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:		
				# extract
				userID = str(entry['userID'])
				userName = str(entry['userName'])
				followers_count = int(entry['followers'])
				friends_count = int(entry['friends'])
				input_tagScore1 = str(entry['userScore1_text'])
				input_tagNcall1 = str(entry['userNcall1_text'])
				input_tagScore2 = str(entry['userScore2_text'])
				input_tagNcall2 = str(entry['userNcall2_text'])
				input_totalCall = int(entry['totalAction'])
				# check if tagText exists
				# (if not) initiate RamSQL_UserUnique_All
				if userID not in RamSQL_UserUnique_All:
					RamSQL_UserUnique_All[userID] = RamSQL_User(user=userID, userName=userName, 
						followers_count=followers_count, friends_count=friends_count)
				# load Scores into RamSQL_UserUnique_All
				RamSQL_UserUnique_All[userID].ScoreInfor_Insert(table=tableName, 
					input_tagScore1=input_tagScore1, input_tagNcall1=input_tagNcall1, 
					input_tagScore2=input_tagScore2, input_tagNcall2=input_tagNcall2, input_totalCall=input_totalCall)
	finally:
		pass

	####################################################################
	# get User information and Tag information and update
	# comd
	comd_UserInfor = "\
select  userID, Muser_counter_text, Ruser_counter_text, Tag_counter_text, tweetID_list_text\n\
from "+tableName+"\n"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(comd_UserInfor)
			result = cursor.fetchall()
			# loop through all rows of this table
			for entry in result:
				# extract
				userID = str(entry['userID'])
				M_UserCounter = str(entry['Muser_counter_text'])
				R_UserCounter = str(entry['Ruser_counter_text'])
				Input_TagCounter = str(entry['Tag_counter_text'])
				Input_tweetID_list = str(entry['tweetID_list_text'])				
				# load into RamSQL_UserUnique_All
				RamSQL_UserUnique_All[userID].Men_UserInfor_Insert(table=tableName, Input_UserCounter=M_UserCounter)
				RamSQL_UserUnique_All[userID].Reply_UserInfor_Insert(table=tableName, Input_UserCounter=R_UserCounter)
				RamSQL_UserUnique_All[userID].TagInfor_Insert(table=tableName, Input_TagCounter=Input_TagCounter)
				RamSQL_UserUnique_All[userID].tweetID_Insert(Input_tweetID_list=Input_tweetID_list)
	finally:
		pass

	return RamSQL_UserUnique_All

"""
####################################################################
# load RamSQL_UserUnique_All into DB table UserUnique_All 
"""

def UserUnique_All_Insert(connection, RamSQL_UserUnique_All):
	
	# loop through keys of RamSQL_UserUnique_All
	for key in RamSQL_UserUnique_All:
		# pointer
		current = RamSQL_UserUnique_All[key]
		# INSERT tagText, tweetID_list_text, totalCall, totalCall_window_text
		comd_INSERT = "\
INSERT INTO UserUnique_All (\
userID, userName, followers, friends,\
tweetID_list_text, totalAction, totalAction_window_text,\
Score1_Ave, Score1_Std, Score2_Ave, Score2_Std,\
userScore1_text, userNcall1_text, userScore2_text, userNcall2_text,\
Score1_Ave_window_text, Score1_Std_window_text, Score2_Ave_window_text, Score2_Std_window_text,\
Muser_counter_text, Muser_degree_abs, Muser_counter_weight_text, Muser_degree_weighted,\
Muser_degree_abs_window_text, Muser_degree_weighted_window_text,\
Ruser_counter_text, Ruser_degree_abs, Ruser_counter_weight_text, Ruser_degree_weighted,\
Ruser_degree_abs_window_text, Ruser_degree_weighted_window_text,\
tag_counter_text, tag_degree_abs, tag_counter_weight_text, tag_degree_weighted,\
tag_degree_abs_window_text, tag_degree_weighted_window_text)\n\
VALUES (\
{}, '{}', {}, {},\
'{}', {}, '{}',\
{}, {}, {}, {},\
'{}','{}','{}','{}',\
'{}','{}','{}','{}',\
'{}', {},'{}', {},\
'{}','{}',\
'{}', {},'{}', {},\
'{}','{}',\
'{}', {},'{}', {},\
'{}','{}'\
)\n\
ON DUPLICATE KEY UPDATE totalAction = {}".format(
current.user, current.userName, current.followers_count, current.friends_count, 
current.tweetID_list_str(), current.totalAction, current.totalAction_window_str(),
current.Score1_Ave, current.Score1_Std, current.Score2_Ave, current.Score2_Std,
current.userScore1_str(), current.userNcall1_str(), current.userScore2_str(), current.userNcall2_str(),
current.Score1_Ave_window_str(), current.Score1_Std_window_str(), current.Score2_Ave_window_str(), current.Score2_Std_window_str(), 
current.Muser_counter_str(), current.Muser_degree_abs, current.Muser_counter_weight_str(), current.Muser_degree_weighted, 
current.Muser_degree_abs_window_str(), current.Muser_degree_weighted_window_str(), 
current.Ruser_counter_str(), current.Ruser_degree_abs, current.Ruser_counter_weight_str(), current.Ruser_degree_weighted, 
current.Ruser_degree_abs_window_str(), current.Ruser_degree_weighted_window_str(), 
current.tag_counter_str(), current.tag_degree_abs, current.tag_counter_weight_str(), current.tag_degree_weighted, 
current.tag_degree_abs_window_str(), current.tag_degree_weighted_window_str(),
current.totalAction)
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_INSERT)
			# commit commands
			connection.commit()
		finally:
			pass



