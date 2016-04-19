import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors


def Get_Table_Names(MySQL_DBkey, start_time, end_time, header):

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	db_name = MySQL_DBkey['db']
	list_table_names = []
	pin_time = start_time
	# loop through time
	while (pin_time < end_time):
		# table to check
		table_name = header + pin_time.strftime('_%Y_%m_%d_%H')
		# comd
		comd_table_check = "\
	SELECT IF( \n\
	(SELECT count(*) FROM information_schema.tables\n\
	WHERE table_schema = '"+db_name+"' AND table_name = '"+table_name+"'),\n\
	1, 0);"
		# execute command
		try:
			with connection.cursor() as cursor:
				cursor.execute(comd_table_check)
				result = cursor.fetchall()
				#print result
				for key in result[0]:
					pin = result[0][key]
				#print pin
		finally:
			pass
		# load results
		if pin == 1:
			print "{} exists.".format(table_name)
			list_table_names.append(table_name)
		else:
			print "{} does NOT exist.".format(table_name)
		# go to next time point
		pin_time = pin_time + np.timedelta64(3600,'s')

	# end of loop
	connection.close()
	return list_table_names

"""
####################################################################
# test code 
"""

if __name__ == "__main__":

	MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1','charset':'utf8mb4'}

	Start_Time = '2016-03-04 05:50:24'
	Start_Time = pd.to_datetime(Start_Time)
	print "start time: ", Start_Time.strftime('_%Y_%m_%d_%H')

	End_Time = '2016-03-08 06:50:24'
	End_Time = pd.to_datetime(End_Time)
	print "end time: ", End_Time.strftime('_%Y_%m_%d_%H')

	header = 'tagunique_'

	list_table_names = Get_Table_Names(MySQL_DBkey=MySQL_DBkey, start_time=Start_Time, end_time=End_Time, header=header)
	#print list_table_names




