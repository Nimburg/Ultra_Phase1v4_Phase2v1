
	####################################################################
	# 
	# Design
	# 
	# Phase1_Main will perform readline loop control; 
	# define variables: RollingScoreBank, Tweet_OBJ, RamSQL and several flags
	# 
	# Stage0 will read JSON tweet, check conditions, (if) extract all infor into Tweet_OBJ, update pin_time
	# (if) pin_time, initialize DB tables
	# 
	# Stage1 will update RollingScoreBank
	# 
	# Stage2 will upload TweetStack
	# 
	# Stage3 will Define and Start updating RamSQL
	# 
	# Stage4, (if) pin_time or (if) end_of_file, load RamSQL into DB SQL, signal for new sliding window
	# 
	####################################################################

import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from RamSQL_TagUnique import RamSQL_Tag
from RamSQL_UserUnique import RamSQL_User

from Phase1_Stage0 import Stage0_Json
from Phase1_Stage0_TablePrep import TweetStack_Init, TweetStack_load, TagUnique_Init, TagUnique_Insert, UserUnique_Init, UserUnique_Insert
from Phase1_Stage1 import RollingScore_Update
from Phase1_Stage3 import RamSQL_UserUnique_update, RamSQL_TagUnique_update



"""
####################################################################
"""

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

# check each line from readline(), check length, start/end character
@check_args(str, int)
def Json_format_check(input_str, index_line):
	# check tweet_line, which should at least be "{}"
	fflag_line = True
	if len(input_str) <= 2:
		fflag_line = False
	# check if tweet_line is complete
	fflag_json = False
	if fflag_line and input_str[0:1] == '{':
		if input_str[-2:-1] == '}' or input_str[-1:] == '}': # last line has no '\n'
			fflag_json = True
	else:
		print "Line: {}, Incomplete or Empty Line".format(index_line)
	return fflag_line, fflag_json

# single input pd.timestamp
@check_args(pd.tslib.Timestamp)
def pd_timestamp_check(input):
	return True


"""
####################################################################
"""

def Phase1_Main(file_name, keyword1, keyword2, MySQL_DBkey, RollingScoreBank):

	####################################################################
	# read tweets.txt file data
	InputfileDir = os.path.dirname(os.path.realpath('__file__'))
	print InputfileDir
	data_file_name =  '../Data/' + file_name
	Inputfilename = os.path.join(InputfileDir, data_file_name) # ../ get back to upper level
	Inputfilename = os.path.abspath(os.path.realpath(Inputfilename))
	print Inputfilename
	file_input = open(Inputfilename,'r')

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	# Rolling Score Bank
	# this variable does NOT got wiped with each sliding window
	# Rolling Score Bank should be checked to create flags, rather than directly control RamSQL
	#RollingScoreBank = col.defaultdict(col.Counter)
	# tags that contain keywords
	#RollingScoreBank['tag_keyword1'] = col.Counter() # val = N_call
	#RollingScoreBank['tag_keyword2'] = col.Counter() # val = N_call
	# tags that with score >= 5
	# keys will overlap here
	#RollingScoreBank['tag_relevant1'] = col.Counter() # val = score
	#RollingScoreBank['tag_relevant1_N'] = col.Counter() # val = N_call
	#RollingScoreBank['tag_relevant2'] = col.Counter() # val = score
	#RollingScoreBank['tag_relevant2_N'] = col.Counter() # val = N_call
	# list of users
	#RollingScoreBank['user1'] = col.Counter() # key = id_str, val = score
	#RollingScoreBank['user1_N'] = col.Counter() # key = id_str, val = N_act
	#RollingScoreBank['user2'] = col.Counter() # key = id_str, val = score
	#RollingScoreBank['user2_N'] = col.Counter() # key = id_str, val = N_act

	####################################################################

	# main data structure, contains information from each tweet
	Tweet_OBJ = col.defaultdict(set)
	# initialize
	Tweet_OBJ['tweet_time'] = set([]) # pd timestamp
	Tweet_OBJ['tweet_id'] = set([]) # all id are id_str
	Tweet_OBJ['user_id'] = set([])
	Tweet_OBJ['user_name'] = set([])
	Tweet_OBJ['user_followers'] = set([])
	Tweet_OBJ['user_friends'] = set([])
	# eliminate reapeating tags
	# the following three will be: K & R, only R, only due_user
	Tweet_OBJ['Tag_Keyword'] = set([])
	Tweet_OBJ['Tag_Relevant'] = set([])
	Tweet_OBJ['Tag_due_user'] = set([])
	# make sure text is onto a single line.....
	Tweet_OBJ['text'] = set([])
	# eliminate repeating userIDs
	Tweet_OBJ['reply_to_userID'] = set([])
	Tweet_OBJ['mentioned_userID'] = set([])

	####################################################################

	# main logic structure, controled by readline() returns, exist at end of file
	flag_fileEnd = False
	count_emptyline = 0
	count_line = 0
	pin_time = pd.to_datetime("Thu Oct 29 18:51:50 +0000 2015") # make sure it is old enough

	# create table TweetStack
	TweetStack_Init(connection=connection)

	# for 1st sliding window
	# RamSQL_TagUnique
	RamSQL_TagUnique = col.defaultdict(RamSQL_Tag)
	# RamSQL_UserUnique
	RamSQL_UserUnique = col.defaultdict(RamSQL_User)

	####################################################################
	# this is the logic loop for EACH tweet
	while (flag_fileEnd == False):
		count_line += 1 
		tweet_line = file_input.readline()
		
		# json format check and file end check and data type check
		flag_line = False
		flag_json = False
		try:
			flag_line, flag_json = Json_format_check(tweet_line, count_line)
		except AssertionError:
			print "Line: {}, Json_format_check() data type check failed".format(index_line)
			pass 
		else:
			pass
		# count adjacent empty lines
		if flag_line == True:
			count_emptyline = 0
		else:
			count_emptyline += 1
		# flag end of file
		if count_emptyline > 4:
			flag_fileEnd = True

		####################################################################
		# Stage0
		# read JSON tweet, check conditions, (if) extract all infor into Tweet_OBJ, update pin_time
		# input/output RollingScoreBank; output Tweet_OBJ
		flag_TweetStack = False
		if flag_json == True:		
			
			# load JSON, extract information
			flag_TweetStack, Tweet_OBJ = Stage0_Json(input_str=tweet_line, index_line=count_line, 
				keyword1=keyword1, keyword2=keyword2, RollingScoreBank=RollingScoreBank)

			# if JSON extraction successful, check pin_time, check sliding_window
			flag_timeStamp = False
			if flag_TweetStack:
				# DataTypeCheck for pd.timestamp; compare and update pin_time
				try:
					flag_timeStamp = pd_timestamp_check(next(iter(Tweet_OBJ['tweet_time'])))
				except AssertionError:
					print "pin_time datatype failed"
					pass 
				else:
					pass

			# check pin_time
			Delta_time = 0
			flag_new_window = False
			if flag_TweetStack and flag_timeStamp:
				Delta_time = (next(iter(Tweet_OBJ['tweet_time'])) - pin_time)/np.timedelta64(1,'s')
			
			# cue for creating
			if Delta_time > 3600 and Delta_time < 86400: # one day
				flag_new_window = True
				pin_time_load = pin_time
				pin_time = next(iter(Tweet_OBJ['tweet_time']))

			# create TagUnique and UserUnique DB tables for the 1st sliding_window
			# create RamSQL variables for 1st sliding window
			# create 1st RamSQL
			if Delta_time >= 86400:
				pin_time = next(iter(Tweet_OBJ['tweet_time']))
				# Initialize DB table TagUnique
				TagUnique_Init(connection=connection, pin_time=pin_time)
				# Initialize DB table UserUnique
				UserUnique_Init(connection=connection, pin_time=pin_time)


		####################################################################
		# Stage1
		# update RollingScoreBank
		if flag_TweetStack:
			RollingScoreBank = RollingScore_Update(RollingScoreBank=RollingScoreBank, 
				Tweet_OBJ=Tweet_OBJ, keyword1=keyword1, keyword2=keyword2)

		####################################################################
		# Stage2
		# DataType Check
		# upload TweetStack
		if flag_TweetStack:
			TweetStack_load(connection=connection, Tweet_OBJ=Tweet_OBJ)

		####################################################################
		# Stage3
		# DataType Check
		# updating RamSQL
		if flag_TweetStack:
			# update RamSQL_TagUnique
			RamSQL_TagUnique = RamSQL_TagUnique_update(RamSQL_TagUnique=RamSQL_TagUnique,
				Tweet_OBJ=Tweet_OBJ, RollingScoreBank=RollingScoreBank)
			# update RamSQL_UserUnique
			RamSQL_UserUnique = RamSQL_UserUnique_update(RamSQL_UserUnique=RamSQL_UserUnique, 
				Tweet_OBJ=Tweet_OBJ, RollingScoreBank=RollingScoreBank)

		####################################################################
		# Stage4
		# (if) pin_time or (if) end_of_file, load RamSQL into DB SQL, signal for new sliding window
		if flag_TweetStack and flag_new_window:
			# load RamsQL_TagUnique into DB table TagUnique, of existing sliding window
			TagUnique_Insert(connection=connection, pin_time=pin_time_load, RamSQL_TagUnique=RamSQL_TagUnique)
			# load RamsQL_UserUnique into DB table TagUnique, of existing sliding window
			UserUnique_Insert(connection=connection, pin_time=pin_time_load, RamSQL_UserUnique=RamSQL_UserUnique)

		# for the last window before file ends, which might be short than 1 hour
		if flag_fileEnd == True:
			# load RamsQL_TagUnique into DB table TagUnique, of existing sliding window
			TagUnique_Insert(connection=connection, pin_time=pin_time_load, RamSQL_TagUnique=RamSQL_TagUnique)
			# load RamsQL_UserUnique into DB table TagUnique, of existing sliding window
			UserUnique_Insert(connection=connection, pin_time=pin_time_load, RamSQL_UserUnique=RamSQL_UserUnique)

		# create New DB table TagUnique and UserUnique
		# create New RamSQL
		if flag_TweetStack and flag_new_window:
			print "create new sliding window "+pin_time.strftime('%Y-%m-%d-%H')
			# create New DB table TagUnique
			TagUnique_Init(connection=connection, pin_time=pin_time)
			# NEW RamSQL_TagUnique
			RamSQL_TagUnique = col.defaultdict(RamSQL_Tag)
			# create New DB table UserUnique
			UserUnique_Init(connection=connection, pin_time=pin_time)
			# NEW RamSQL_UserUnique
			RamSQL_UserUnique = col.defaultdict(RamSQL_User)


	####################################################################
	# End of File
	connection.close()
	return RollingScoreBank



"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	file_name_list = ['US_tweets_Mar4th.txt', 'US_tweets_Mar5th.txt','US_tweets_Mar6th.txt','US_tweets_Mar7th.txt']
	#file_name_list = ['US_tweets_Mar6th.txt','US_tweets_Mar7th.txt']

	keyword1 = 'trump'
	keyword2 = 'hillary'
	MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}


	# Rolling Score Bank
	# This variable is "global" across ALL data files; 
	# this variable does NOT got wiped with each sliding window
	# Rolling Score Bank should be checked to create flags, rather than directly control RamSQL
	RollingScoreBank = col.defaultdict(col.Counter)
	# tags that contain keywords
	RollingScoreBank['tag_keyword1'] = col.Counter() # val = N_call
	RollingScoreBank['tag_keyword2'] = col.Counter() # val = N_call
	# tags that with score >= 5
	# keys will overlap here
	RollingScoreBank['tag_relevant1'] = col.Counter() # val = score
	RollingScoreBank['tag_relevant1_N'] = col.Counter() # val = N_call
	RollingScoreBank['tag_relevant2'] = col.Counter() # val = score
	RollingScoreBank['tag_relevant2_N'] = col.Counter() # val = N_call
	# list of users
	RollingScoreBank['user1'] = col.Counter() # key = id_str, val = score
	RollingScoreBank['user1_N'] = col.Counter() # key = id_str, val = N_act
	RollingScoreBank['user2'] = col.Counter() # key = id_str, val = score
	RollingScoreBank['user2_N'] = col.Counter() # key = id_str, val = N_act


	# LOOP through all data files
	for file_name in file_name_list:
		print "Start processing: ", file_name		
		RollingScoreBank = Phase1_Main(file_name = file_name, keyword1 = keyword1, keyword2 = keyword2, 
			MySQL_DBkey = MySQL_DBkey, RollingScoreBank = RollingScoreBank)
		print "Finished processing: ", file_name

	print "End of Execution of Phase 1"

#########################################
# performance
# 
# 23 hours real world data, ~ 2 million tweets
# 3300 seconds, ~ 11,000 related tweets
# 
# at anytime, tracking ~ 3000 tags and ~ 25000 users respectively
# 
# 