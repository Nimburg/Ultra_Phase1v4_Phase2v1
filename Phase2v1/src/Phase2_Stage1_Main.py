
	######################################################################################
	# 
	# Phase2_Stage1:
	# 
	# 
	# 
	# Calculate New TagUnique_All:
	# 1. New TotalCall, Average Score 1&2, Std of Scores
	# 2. Combined history (scores, Ncalls, tweetIDs)
	# 3. User_Degree_ABS, User_Freq_Dist, User_Degree_Weighted (against total call)
	# 4. Tag_Degree_ABS, Tag_Freq_Dist, Tag_Degree_Weighted (against total call)
	# 5. Sliding window History (per): actions, average scores, stds, Tag/User degress abs/weighted
	# 
	# 
	# 
	# Calculate New UserUnique_All:
	# 1. New TotalCall, Average Score 1&2, Std of Scores
	# 2. Combined history (scores, Ncalls, tweetIDs)
	# 3. Mentioned: User_Degree_ABS, User_Freq_Dist, User_Degree_Weighted (against total call, Squared)
	#    Replied: 
	# 4. Tag_Degree_ABS, Tag_Freq_Dist, Tag_Degree_Weighted (against total call, Squared)
	# 5. Sliding window History (per): actions, average scores, stds, Tag/User degress abs/weighted	
	#    Stds per Sliding window for actions
	# 6. New columns: sorted tags according to Freq, prep for senti analysis
	# 
	######################################################################################

import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from RamSQL_TagUnique_Sum import RamSQL_Tag
from RamSQL_UserUnique_Sum import RamSQL_User

from Phase2_Stage0_tableNames import Get_Table_Names
from Phase2_Stage1_SQLcomds import TagUnique_All_Init, SQL_tagunique_table_extract, TagUnique_All_Insert
from Phase2_Stage1_SQLcomds import UserUnique_All_Init, SQL_userunique_table_extract, UserUnique_All_Insert




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



"""
####################################################################
# Phase2_Stage1 Recal_TagUnique_All
"""

# perform preliminary analysis for all tags
def Recal_TagUnique_All(MySQL_DBkey, start_time, end_time):

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	# get list of tables for all tagunique_
	header = 'tagunique'
	list_table_names = Get_Table_Names(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time, header=header)

	# global across all windows
	RamSQL_TagUnique_All = col.defaultdict(RamSQL_Tag)

	# go through list of tables, add all tags into RamSQL_TagUnique_All
	for tableName in list_table_names:
		print "extracting information from table: {}".format(tableName)
		# loading per table
		RamSQL_TagUnique_All = SQL_tagunique_table_extract(connection=connection, 
			tableName=tableName, RamSQL_TagUnique_All=RamSQL_TagUnique_All)
		# check
		print "Number of tags in RamSQL_TagUnique_All: {}".format(len(RamSQL_TagUnique_All))

	# Initialized DB table TagUnique_All
	TagUnique_All_Init(connection=connection)
	# load RamSQL_TagUnique_All into DB table TagUnique_All
	TagUnique_All_Insert(connection=connection, RamSQL_TagUnique_All=RamSQL_TagUnique_All)

	connection.close()
	return RamSQL_TagUnique_All

"""
####################################################################
# Phase2_Stage1 Recal_UserUnique_All
"""

# perform preliminary analysis for all tags
def Recal_UserUnique_All(MySQL_DBkey, start_time, end_time):

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	# get list of tables for all tagunique_
	header = 'userunique'
	list_table_names = Get_Table_Names(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time, header=header)

	# global across all windows
	RamSQL_UserUnique_All = col.defaultdict(RamSQL_User)

	# go through list of tables, add all tags into RamSQL_UserUnique_All
	for tableName in list_table_names:
		print "extracting information from table: {}".format(tableName)
		# loading per table
		RamSQL_UserUnique_All = SQL_userunique_table_extract(connection=connection, 
			tableName=tableName, RamSQL_UserUnique_All=RamSQL_UserUnique_All)
		# check
		print "Number of users in RamSQL_UserUnique_All: {}".format(len(RamSQL_UserUnique_All))

	# Initialized DB table UserUnique_All
	UserUnique_All_Init(connection=connection)
	# load RamSQL_UserUnique_All into DB table UserUnique_All
	UserUnique_All_Insert(connection=connection, RamSQL_UserUnique_All=RamSQL_UserUnique_All)

	connection.close()
	return RamSQL_UserUnique_All

"""
####################################################################
# Phase2_Stage1 Mainfunction
"""

# MySQL_DBkey as dict; start_time, end_time as string
def Phase2_Stage1_Main(MySQL_DBkey, start_time, end_time):
	
	#MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}

	Start_Time = start_time
	Start_Time = pd.to_datetime(Start_Time)
	print "start time: ", Start_Time.strftime('_%Y_%m_%d_%H')

	End_Time = end_time
	End_Time = pd.to_datetime(End_Time)
	print "end time: ", End_Time.strftime('_%Y_%m_%d_%H')

	Recal_TagUnique_All(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time)

	Recal_UserUnique_All(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time)

	return None

"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}

	Start_Time = '2016-03-04 05:50:24'
	Start_Time = pd.to_datetime(Start_Time)
	print "start time: ", Start_Time.strftime('_%Y_%m_%d_%H')

	End_Time = '2016-03-08 06:50:24'
	End_Time = pd.to_datetime(End_Time)
	print "end time: ", End_Time.strftime('_%Y_%m_%d_%H')

	Recal_TagUnique_All(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time)

	Recal_UserUnique_All(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time)









####################################################################
# 
# analyzing four-days' worth of data
# tracking 22554 tags in RamSQL_TagUnique_All
# finished in 1820 seconds
# 330 tags with freq >= 40 calls/per 4-days
# 
# analyzing four-days' worth of data
# tracking 7022 users 
# (with freq cutoff effectively 1 keyword taged tweet per 10 tweets, with the other 9 completely irrelevant)
# finished in 974 seconds
# 



