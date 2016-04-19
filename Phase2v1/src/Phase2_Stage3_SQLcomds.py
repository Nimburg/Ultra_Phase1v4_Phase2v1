
	# Add Columns:
	# Both:
	# Score_Boundry (Ave - Std as 68.3% lower)
	# Norm_totalCall, Norm_totalActions (ln scale; not log) (get Max, find the ratio of scaling max to e^10)
	# Norm_MenUser_Degree_abs, Norm_Tag_Degree_abs
	# MenUser_Degree_ws, Tag_Degree_ws ( Sqrt(degree_abs/degree_weighted) )
	# Norm_MenUser_Degree_ws, Norm_Tag_Degree_ws ()

import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

"""
####################################################################
Add columns
"""

def TagUser_UniqueAll_AddColumn(connection):
	#Comd
	Comd_Tag = "\
ALTER TABLE TagUnique_All\n\
ADD COLUMN Score1_LB float,\n\
ADD COLUMN Score2_LB float,\n\
ADD COLUMN Norm_totalCall float,\n\
ADD COLUMN Norm_user_degree_abs float,\n\
ADD COLUMN user_degree_ws float,\n\
Add COLUMN Norm_user_degree_ws float,\n\
ADD COLUMN Norm_tag_degree_abs float,\n\
ADD COLUMN tag_degree_ws float,\n\
ADD COLUMN Norm_tag_degree_ws float,\n\
ADD COLUMN classification int;"
	#Comd
	Comd_User = "\
ALTER TABLE UserUnique_All\n\
ADD COLUMN Score1_LB float,\n\
ADD COLUMN Score2_LB float,\n\
ADD COLUMN Norm_totalAction float,\n\
ADD COLUMN Norm_Muser_degree_abs float,\n\
ADD COLUMN Muser_degree_ws float,\n\
Add COLUMN Norm_Muser_degree_ws float,\n\
ADD COLUMN Norm_tag_degree_abs float,\n\
ADD COLUMN tag_degree_ws float,\n\
ADD COLUMN Norm_tag_degree_ws float,\n\
ADD COLUMN classification int;"
	# execute commands
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_Tag)
			cursor.execute(Comd_User)
		# commit commands
		print "Adding New columns to TagUnique_All and UserUnique_All"
		connection.commit()
	finally:
		pass

"""
####################################################################
Tag Recalculate 
"""
# Get Max for Tags: totalCall, user_degree_abs, tag_degree_abs
# 
# Get All tagText as primary key
# retrieve for each tagText: 
# Aves and Stds
# totalCall
# user_degree_abs, tag_degree_abs
# user_degree_weighted, tag_degree_weighted
# 
# Calculate:
# Score_LB, Norm_totalCall, Norm_degree_abs
# 
# upload into table for THIS tagText
# 
# 
# 
# Find Max for degree_ws
# Normalize
# Load into table
# 
# 

def TagUnique_Normalize(connection):

	########################################################################
	# get Max values
	Comd_MaxValues = "\
select max(totalCall), max(user_degree_abs), max(tag_degree_abs)\n\
from tagunique_all;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_MaxValues)
			result = cursor.fetchall()
			# 
			Max_totalCall = float(result[0]['max(totalCall)'])
			Max_user_degree_abs = float(result[0]['max(user_degree_abs)'])
			Max_tag_degree_abs = float(result[0]['max(tag_degree_abs)'])
	finally:
		pass

	########################################################################
	# 
	Data_Storage = col.defaultdict(dict)
	# get Primary Keys and Values
	Comd_GetValues = "\
select tagText, Score1_Ave, Score1_Std, Score2_Ave, Score2_Std, totalCall,\
user_degree_abs, user_degree_weighted, tag_degree_abs, tag_degree_weighted\n\
from tagunique_all;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_GetValues)
			result = cursor.fetchall()
			# 
			for item in result:
				key = item['tagText']
				Data_Storage[key] = {}
				Data_Storage[key]['Score1_Ave'] = float(item['Score1_Ave'])
				Data_Storage[key]['Score1_Std'] = float(item['Score1_Std'])
				Data_Storage[key]['Score2_Ave'] = float(item['Score2_Ave'])
				Data_Storage[key]['Score2_Std'] = float(item['Score2_Std'])
				Data_Storage[key]['totalCall'] = int(item['totalCall'])
				Data_Storage[key]['user_degree_abs'] = int(item['user_degree_abs'])
				Data_Storage[key]['user_degree_weighted'] = float(item['user_degree_weighted'])
				Data_Storage[key]['tag_degree_abs'] = int(item['tag_degree_abs'])
				Data_Storage[key]['tag_degree_weighted'] = float(item['tag_degree_weighted'])
	finally:
		pass

	########################################################################
	# recalculate and load

	counter = 0
	for key in Data_Storage:
		counter += 1
		current = Data_Storage[key]
		# In case of empty entry
		user_degree_ws = 0.0
		if current['user_degree_weighted'] > 0:
			user_degree_ws = np.sqrt(1.0*current['user_degree_abs']/current['user_degree_weighted'])
		tag_degree_ws = 0.0
		if current['tag_degree_weighted'] > 0:
			tag_degree_ws = np.sqrt(1.0*current['tag_degree_abs']/current['tag_degree_weighted'])		
		# in case of -Inf
		Norm_totalCall = np.log( current['totalCall']*22026.0/Max_totalCall )
		if Norm_totalCall < -10:
			Norm_totalCall = -100 # reject later
		Norm_user_degree_abs = np.log( current['user_degree_abs']*22026.0/Max_user_degree_abs )
		if Norm_user_degree_abs < -10:
			Norm_user_degree_abs = -100
			print counter
		Norm_tag_degree_abs = np.log( current['tag_degree_abs']*22026.0/Max_tag_degree_abs )
		if Norm_tag_degree_abs < -10:
			Norm_tag_degree_abs = -100
			print counter
		# comd for update
		Comd_Update_1 = "\
update tagunique_all\n\
set\n\
	Score1_LB = {},\n\
	Score2_LB = {},\n\
	Norm_totalCall = {},\n\
	Norm_user_degree_abs = {},\n\
	user_degree_ws = {},\n\
	Norm_tag_degree_abs = {},\n\
	tag_degree_ws = {}\n\
where\n\
	tagText = '{}';".format(
current['Score1_Ave'] - current['Score1_Std'], 
current['Score2_Ave'] - current['Score2_Std'], 
Norm_totalCall,
Norm_user_degree_abs,
user_degree_ws,
Norm_tag_degree_abs,
tag_degree_ws,
key)
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(Comd_Update_1)
			# commit commands
			connection.commit()
		finally:
			pass

	########################################################################
	# get Max values for user_degree_ws and tag_degree_ws
	Comd_MaxValues = "\
select max(user_degree_ws), max(tag_degree_ws)\n\
from tagunique_all;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_MaxValues)
			result = cursor.fetchall()
			# 
			Max_user_degree_ws = float(result[0]['max(user_degree_ws)'])
			Max_tag_degree_ws = float(result[0]['max(tag_degree_ws)'])
	finally:
		pass

	########################################################################
	# recalculate and load Normalized user_degree_ws and tag_degree_ws
	counter = 0
	for key in Data_Storage:
		counter += 1
		current = Data_Storage[key]
		# in case of empty entry
		Norm_user_degree_ws = 0.0
		if current['user_degree_weighted'] > 0:
			Norm_user_degree_ws = np.log( np.sqrt(1.0*current['user_degree_abs']/current['user_degree_weighted'])*22026.0/Max_user_degree_ws)
		if Norm_user_degree_ws < -10:
			Norm_user_degree_ws = -100
			print counter
		Norm_tag_degree_ws = 0.0
		if current['tag_degree_weighted'] > 0:
			Norm_tag_degree_ws = np.log( np.sqrt(1.0*current['tag_degree_abs']/current['tag_degree_weighted'])*22026.0/Max_tag_degree_ws )
		if Norm_tag_degree_ws < -10:
			Norm_tag_degree_ws = -100
			print counter
		# comd for update
		Comd_Update_2 = "\
update tagunique_all\n\
set\n\
	Norm_user_degree_ws = {},\n\
	Norm_tag_degree_ws = {}\n\
where\n\
	tagText = '{}';".format(
Norm_user_degree_ws , 
Norm_tag_degree_ws, 
key)
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(Comd_Update_2)
			# commit commands
			connection.commit()
		finally:
			pass

"""
####################################################################
User Recalculate 
"""

def UserUnique_Normalize(connection):

	########################################################################
	# get Max values
	Comd_MaxValues = "\
select max(totalAction), max(Muser_degree_abs), max(tag_degree_abs)\n\
from userunique_all;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_MaxValues)
			result = cursor.fetchall()
			# 
			Max_totalAction = float(result[0]['max(totalAction)'])
			Max_Muser_degree_abs = float(result[0]['max(Muser_degree_abs)'])
			Max_tag_degree_abs = float(result[0]['max(tag_degree_abs)'])
	finally:
		pass

	########################################################################
	# 
	Data_Storage = col.defaultdict(dict)
	# get Primary Keys and Values
	Comd_GetValues = "\
select userID, Score1_Ave, Score1_Std, Score2_Ave, Score2_Std, totalAction,\
Muser_degree_abs, Muser_degree_weighted, tag_degree_abs, tag_degree_weighted\n\
from userunique_all;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_GetValues)
			result = cursor.fetchall()
			# 
			for item in result:
				key = item['userID']
				Data_Storage[key] = {}
				Data_Storage[key]['Score1_Ave'] = float(item['Score1_Ave'])
				Data_Storage[key]['Score1_Std'] = float(item['Score1_Std'])
				Data_Storage[key]['Score2_Ave'] = float(item['Score2_Ave'])
				Data_Storage[key]['Score2_Std'] = float(item['Score2_Std'])
				Data_Storage[key]['totalAction'] = int(item['totalAction'])
				Data_Storage[key]['Muser_degree_abs'] = int(item['Muser_degree_abs'])
				Data_Storage[key]['Muser_degree_weighted'] = float(item['Muser_degree_weighted'])
				Data_Storage[key]['tag_degree_abs'] = int(item['tag_degree_abs'])
				Data_Storage[key]['tag_degree_weighted'] = float(item['tag_degree_weighted'])
	finally:
		pass

	########################################################################
	# recalculate and load

	counter = 0
	for key in Data_Storage:
		counter += 1
		current = Data_Storage[key]
		# In case of empty entry
		Muser_degree_ws = 0.0
		if current['Muser_degree_weighted'] > 0:
			Muser_degree_ws = np.sqrt(1.0*current['Muser_degree_abs']/current['Muser_degree_weighted'])
		tag_degree_ws = 0.0
		if current['tag_degree_weighted'] > 0:
			tag_degree_ws = np.sqrt(1.0*current['tag_degree_abs']/current['tag_degree_weighted'])		
		# in case of -Inf
		Norm_totalAction = np.log( current['totalAction']*22026.0/Max_totalAction )
		if Norm_totalAction < -10:
			Norm_totalAction = -100 # reject later
		Norm_Muser_degree_abs = np.log( current['Muser_degree_abs']*22026.0/Max_Muser_degree_abs )
		if Norm_Muser_degree_abs < -10:
			Norm_Muser_degree_abs = -100
			print counter
		Norm_tag_degree_abs = np.log( current['tag_degree_abs']*22026.0/Max_tag_degree_abs )
		if Norm_tag_degree_abs < -10:
			Norm_tag_degree_abs = -100
			print counter
		# comd for update
		Comd_Update_1 = "\
update userunique_all\n\
set\n\
	Score1_LB = {},\n\
	Score2_LB = {},\n\
	Norm_totalAction = {},\n\
	Norm_Muser_degree_abs = {},\n\
	Muser_degree_ws = {},\n\
	Norm_tag_degree_abs = {},\n\
	tag_degree_ws = {}\n\
where\n\
	userID = '{}';".format(
current['Score1_Ave'] - current['Score1_Std'], 
current['Score2_Ave'] - current['Score2_Std'], 
Norm_totalAction,
Norm_Muser_degree_abs,
Muser_degree_ws,
Norm_tag_degree_abs,
tag_degree_ws,
key)
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(Comd_Update_1)
			# commit commands
			connection.commit()
		finally:
			pass

	########################################################################
	# get Max values for user_degree_ws and tag_degree_ws
	Comd_MaxValues = "\
select max(Muser_degree_ws), max(tag_degree_ws)\n\
from userunique_all;"
	# execute command
	try:
		with connection.cursor() as cursor:
			cursor.execute(Comd_MaxValues)
			result = cursor.fetchall()
			# 
			Max_Muser_degree_ws = float(result[0]['max(Muser_degree_ws)'])
			Max_tag_degree_ws = float(result[0]['max(tag_degree_ws)'])
	finally:
		pass

	########################################################################
	# recalculate and load Normalized user_degree_ws and tag_degree_ws
	counter = 0
	for key in Data_Storage:
		counter += 1
		current = Data_Storage[key]
		# in case of empty entry
		Norm_Muser_degree_ws = 0.0
		if current['Muser_degree_weighted'] > 0:
			Norm_Muser_degree_ws = np.log( np.sqrt(1.0*current['Muser_degree_abs']/current['Muser_degree_weighted'])*22026.0/Max_Muser_degree_ws)
		if Norm_Muser_degree_ws < -10:
			Norm_Muser_degree_ws = -100
			print counter
		Norm_tag_degree_ws = 0.0
		if current['tag_degree_weighted'] > 0:
			Norm_tag_degree_ws = np.log( np.sqrt(1.0*current['tag_degree_abs']/current['tag_degree_weighted'])*22026.0/Max_tag_degree_ws )
		if Norm_tag_degree_ws < -10:
			Norm_tag_degree_ws = -100
			print counter
		# comd for update
		Comd_Update_2 = "\
update userunique_all\n\
set\n\
	Norm_Muser_degree_ws = {},\n\
	Norm_tag_degree_ws = {}\n\
where\n\
	userID = '{}';".format(
Norm_Muser_degree_ws , 
Norm_tag_degree_ws, 
key)
		# execute commands
		try:
			with connection.cursor() as cursor:
				cursor.execute(Comd_Update_2)
			# commit commands
			connection.commit()
		finally:
			pass



