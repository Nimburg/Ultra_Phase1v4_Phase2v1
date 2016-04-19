import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors




####################################################################
# variable type check
def check_args(*types):
	def real_decorator(func):
		def wrapper(*args, **kwargs):
			for val, typ in zip(args, types):
				assert isinstance(val, typ), "Value {} is not of expected type {}".format(val, typ)
			return func(*args, **kwargs)
		return wrapper
	return real_decorator

####################################################################

# single input pd.timestamp
@check_args(pd.tslib.Timestamp)
def pd_timestamp_check(input):
	return True



"""
####################################################################
"""

####################################################################

def TweetStack_Init(connection):
	
	#Comd
	# Do NOT drop Tweet_Stack; Luigid consideration
	Comd_TweetStack_Init = "\
CREATE TABLE IF NOT EXISTS Tweet_Stack\n\
(\n\
	tweetID BIGINT PRIMARY KEY NOT NULL,\n\
	tweetTime TIMESTAMP NOT NULL,\n\
	userID BIGINT NOT NULL,\n\
	tweetText TEXT COLLATE utf8_bin,\n\
	reply_user_id BIGINT,\n\
	MenUserList_text TEXT,\n\
	TagList_text TEXT\n\
) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin"
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_TweetStack_Init)
		# commit commands
		print "Tweet_Stack Initialized"
		connection.commit()
	finally:
		pass

####################################################################

def TweetStack_load(connection, Tweet_OBJ):

	# DataTypeCheck for pd.timestamp; compare and update pin_time
	flag_timeStamp = False	
	try:
		flag_timeStamp = pd_timestamp_check(next(iter(Tweet_OBJ['tweet_time'])))
	except AssertionError:
		print "pin_time datatype failed"
		pass 
	else:
		pass
	
	# id_str check
	flag_id_str = False
	if next(iter(Tweet_OBJ['tweet_id'])).isdigit() and next(iter(Tweet_OBJ['user_id'])).isdigit() and next(iter(Tweet_OBJ['reply_to_userID'])).isdigit():
		flag_id_str = True
	
	# mentioned_userID check and parse
	mentioned_userID_str = ""
	for men_user in Tweet_OBJ['mentioned_userID']:
		if men_user.isdigit():
			mentioned_userID_str += men_user + ','
	if len(mentioned_userID_str) > 1:
		mentioned_userID_str = mentioned_userID_str[:-1] # for the last ','
		
	# parse tagList
	tagList_str = ""
	for tag_key in Tweet_OBJ['Tag_Keyword']:
		tagList_str += tag_key + ','
	for tag_rel in Tweet_OBJ['Tag_Relevant']:
		tagList_str += tag_rel + ','
	for tag_user in Tweet_OBJ['Tag_due_user']:
		tagList_str += tag_user + ','
	if len(tagList_str) > 1:
		tagList_str = tagList_str[:-1]

	####################################################################
	if flag_timeStamp and flag_id_str:
		# command for Tweet_Stack
		comd_TweetStack_Insert = "\
INSERT INTO Tweet_Stack (tweetID, tweetTime, userID, reply_user_id, tweetText, MenUserList_text, TagList_text)\n\
VALUES ("+next(iter(Tweet_OBJ['tweet_id']))+",'"+str(next(iter(Tweet_OBJ['tweet_time'])))+"',"+next(iter(Tweet_OBJ['user_id']))+","+next(iter(Tweet_OBJ['reply_to_userID']))+",'"+next(iter(Tweet_OBJ['text']))+"','"+mentioned_userID_str+"','"+tagList_str+"')\n\
ON DUPLICATE KEY UPDATE userID = " + next(iter(Tweet_OBJ['user_id'])) + ";\n"
		#print comd_TweetStack_Insert
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_TweetStack_Insert)
			# commit commands
			connection.commit()
		finally:
			pass

####################################################################

"""
####################################################################
"""

####################################################################

def TagUnique_Init(connection, pin_time):

	# table Name
	tableName = "TagUnique_"+pin_time.strftime('%Y_%m_%d_%H')
	#Comd
	Comd_TagUnique_Init = "\
CREATE TABLE IF NOT EXISTS "+tableName+"\n\
(\n\
	tagText varchar(255) PRIMARY KEY NOT NULL,\n\
	totalCall int NOT NULL,\n\
	score1_fin float,\n\
	Ncall1_fin int,\n\
	score2_fin float,\n\
	Ncall2_fin int,\n\
	tagScore1_text text,\n\
	tagNcall1_text text,\n\
	tagScore2_text text,\n\
	tagNcall2_text text,\n\
	tweetID_list_text text,\n\
	user_counter_text text,\n\
	tag_counter_text text\n\
)"
	print Comd_TagUnique_Init
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_TagUnique_Init)
		# commit commands
		print tableName+" Initialized"
		connection.commit()
	finally:
		pass

####################################################################

def TagUnique_Insert(connection, pin_time, RamSQL_TagUnique):

	# table Name
	tableName = "TagUnique_"+pin_time.strftime('%Y_%m_%d_%H')
	
	# go throught each key, parse contents, and upload
	for key in RamSQL_TagUnique:
		
		# parse variables into strings, using class function
		tagScore1_text = RamSQL_TagUnique[key].tagScore1_str()
		tagNcall1_text = RamSQL_TagUnique[key].tagNcall1_str()

		tagScore2_text = RamSQL_TagUnique[key].tagScore2_str()
		tagNcall2_text = RamSQL_TagUnique[key].tagNcall2_str()

		tweetID_list_text = RamSQL_TagUnique[key].tweetID_list_str()
		user_counter_text = RamSQL_TagUnique[key].user_counter_str()
		tag_counter_text = RamSQL_TagUnique[key].tag_counter_str()
		
		# get score_fin and Ncall_fin
		score1_fin = 0
		if len(RamSQL_TagUnique[key].tagScore1) > 0:
			score1_fin = RamSQL_TagUnique[key].tagScore1[-1]

		Ncall1_fin = 0
		if len(RamSQL_TagUnique[key].tagNcall1) > 0:	
			Ncall1_fin = RamSQL_TagUnique[key].tagNcall1[-1]

		score2_fin = 0
		if len(RamSQL_TagUnique[key].tagScore2) > 0:
			score2_fin = RamSQL_TagUnique[key].tagScore2[-1]

		Ncall2_fin = 0
		if len(RamSQL_TagUnique[key].tagNcall2) > 0:
			Ncall2_fin = RamSQL_TagUnique[key].tagNcall2[-1]

		# check totalCall
		if RamSQL_TagUnique[key].totalCall != len(RamSQL_TagUnique[key].tagScore1) or RamSQL_TagUnique[key].totalCall != len(RamSQL_TagUnique[key].tweetID_list):
			print "Bad Total Call"

		# command for Tweet_Stack
		comd_TagUnique_Insert = "\
INSERT INTO "+tableName+" (tagText, totalCall, score1_fin, Ncall1_fin, score2_fin, Ncall2_fin, tagScore1_text, tagNcall1_text, tagScore2_text, tagNcall2_text, tweetID_list_text, user_counter_text, tag_counter_text)\n\
VALUES ('"+RamSQL_TagUnique[key].tagText+"',"+str(RamSQL_TagUnique[key].totalCall)+","+str(score1_fin)+","+str(Ncall1_fin)+","+str(score2_fin)+","+str(Ncall2_fin)+",'"+tagScore1_text+"','"+tagNcall1_text+"','"+tagScore2_text+"','"+tagNcall2_text+"','"+tweetID_list_text+"','"+user_counter_text+"','"+tag_counter_text+"')\n\
ON DUPLICATE KEY UPDATE totalCall = " + str(RamSQL_TagUnique[key].totalCall) + ";\n"
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_TagUnique_Insert)
			# commit commands
			connection.commit()
		finally:
			pass


####################################################################

"""
####################################################################
"""

####################################################################

def UserUnique_Init(connection, pin_time):

	# table Name
	tableName = "UserUnique_"+pin_time.strftime('%Y_%m_%d_%H')
	#Comd
	Comd_UserUnique_Init = "\
CREATE TABLE IF NOT EXISTS "+tableName+"\n\
(\n\
	userID bigint PRIMARY KEY NOT NULL,\n\
	userName varchar(255),\n\
	totalAction int,\n\
	followers int,\n\
	friends int,\n\
	score1_fin float,\n\
	Ncall1_fin int,\n\
	score2_fin float,\n\
	Ncall2_fin int,\n\
	userScore1_text text,\n\
	userNcall1_text text,\n\
	userScore2_text text,\n\
	userNcall2_text text,\n\
	tweetID_list_text text,\n\
	Ruser_counter_text text,\n\
	Muser_counter_text text,\n\
	Tag_counter_text text\n\
)"
	print Comd_UserUnique_Init
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_UserUnique_Init)
		# commit commands
		print tableName+" Initialized"
		connection.commit()
	finally:
		pass

####################################################################

def UserUnique_Insert(connection, pin_time, RamSQL_UserUnique):

	# table Name
	tableName = "UserUnique_"+pin_time.strftime('%Y_%m_%d_%H')

	# go throught each key, parse contents, and upload
	for key in RamSQL_UserUnique:

		# parse variables into strings, using class function
		userScore1_text = RamSQL_UserUnique[key].userScore1_str()
		userNcall1_text = RamSQL_UserUnique[key].userNcall1_str()
		userScore2_text = RamSQL_UserUnique[key].userScore2_str()
		userNcall2_text = RamSQL_UserUnique[key].userNcall2_str()

		tweetID_list_text = RamSQL_UserUnique[key].tweetID_list_str()
		Ruser_counter_text = RamSQL_UserUnique[key].Ruser_counter_str()
		Muser_counter_text = RamSQL_UserUnique[key].Muser_counter_str()
		Tag_counter_text = RamSQL_UserUnique[key].tag_counter_str()

		# get score_fin and Ncall_fin
		if len(RamSQL_UserUnique[key].userScore1) > 0:
			score1_fin = RamSQL_UserUnique[key].userScore1[-1]
		else:
			score1_fin = 0

		if len(RamSQL_UserUnique[key].userNcall1) > 0:	
			Ncall1_fin = RamSQL_UserUnique[key].userNcall1[-1]
		else:
			Ncall1_fin = 0

		if len(RamSQL_UserUnique[key].userScore2) > 0:
			score2_fin = RamSQL_UserUnique[key].userScore2[-1]
		else:
			score2_fin = 0

		if len(RamSQL_UserUnique[key].userNcall2) > 0:	
			Ncall2_fin = RamSQL_UserUnique[key].userNcall2[-1]
		else:
			Ncall2_fin = 0

		# command for Tweet_Stack
		comd_UserUnique_Insert = "\
INSERT INTO "+tableName+" (userID, userName, totalAction, followers, friends, score1_fin, Ncall1_fin, score2_fin, Ncall2_fin, userScore1_text, userNcall1_text, userScore2_text, userNcall2_text, tweetID_list_text, Ruser_counter_text, Muser_counter_text, Tag_counter_text)\n\
VALUES ("+RamSQL_UserUnique[key].user+",'"+RamSQL_UserUnique[key].userName+"',"+str(RamSQL_UserUnique[key].totalAction)+","+str(RamSQL_UserUnique[key].followers_count)+","+str(RamSQL_UserUnique[key].friends_count)+","+str(score1_fin)+","+str(Ncall1_fin)+","+str(score2_fin)+","+str(Ncall2_fin)+",'"+userScore1_text+"','"+userNcall1_text+"','"+userScore2_text+"','"+userNcall2_text+"','"+tweetID_list_text+"','"+Ruser_counter_text+"','"+Muser_counter_text+"','"+Tag_counter_text+"')\n\
ON DUPLICATE KEY UPDATE totalAction = " + str(RamSQL_UserUnique[key].totalAction) + ";\n"
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_UserUnique_Insert)
			# commit commands
			connection.commit()
		finally:
			pass

"""
####################################################################
"""









