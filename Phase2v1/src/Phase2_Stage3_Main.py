


import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from Phase2_Stage3_SQLcomds import TagUser_UniqueAll_AddColumn, TagUnique_Normalize, UserUnique_Normalize


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
"""





"""
####################################################################
# test code for Stage1 main
"""

if __name__ == "__main__":

	MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}

	####################################################################

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey['host'],
								 user=MySQL_DBkey['user'],
								 password=MySQL_DBkey['password'],
								 db=MySQL_DBkey['db'],
								 charset=MySQL_DBkey['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	
	####################################################################

	# add new columns
	TagUser_UniqueAll_AddColumn(connection=connection)

	# Normalize Values for classification
	TagUnique_Normalize(connection=connection)

	# Normalize Values for classification
	UserUnique_Normalize(connection=connection)


	connection.close()
