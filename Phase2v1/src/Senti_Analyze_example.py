import json
import os
import numpy as np 
import pandas as pd
import collections as col

import pymysql.cursors

from nltk.sentiment.vader import SentimentIntensityAnalyzer
from nltk import tokenize


####################################################################

MySQL_DBkey = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}

# Connect to the database
connection = pymysql.connect(host=MySQL_DBkey['host'],
							 user=MySQL_DBkey['user'],
							 password=MySQL_DBkey['password'],
							 db=MySQL_DBkey['db'],
							 charset=MySQL_DBkey['charset'],
							 cursorclass=pymysql.cursors.DictCursor)

####################################################################

SQL_Inputs = []

# comd
comd_single = "\
select tweetText \n\
from Tweet_Stack\n\
where userID = 72664613 and TagList_text LIKE '%hillary%';"
# execute command
try:
	with connection.cursor() as cursor:
		cursor.execute(comd_single)
		result = cursor.fetchall()
		#print result
		for item in result:
			SQL_Inputs.append(str(item['tweetText']))
finally:
	pass
# end of loop
connection.close()

####################################################################

Results = []

sid = SentimentIntensityAnalyzer()

for i in range(len(SQL_Inputs)):
	#print sentences[i]
	ss = sid.polarity_scores(SQL_Inputs[i])
	#print ss
	res_X = ss['neu']
	res_Y = -ss['neg'] + ss['pos']
	print "Result {} : X~{}, Y~{}".format(i+1, res_X, res_Y)
	if int(res_X) != 1:
		Results.append([res_X, res_Y])

####################################################################

Results = np.array(Results)
print "{}\n Post-filtering: {}".format(Results, len(Results))
flag_res = False

if len(Results) > 0 and len(Results[0]) == 2:
	flag_res = True

	X_mean = np.mean(Results[:,0])
	X_std = np.std(Results[:,0])
	X_95_half = 3*X_std/np.sqrt(len(Results))

	Y_mean = np.mean(Results[:,1])
	Y_std = np.std(Results[:,1])
	Y_95_half = 3*Y_std/np.sqrt(len(Results))

	#print "for X, mean~{}, std~{}, 95%: {}~{}".format(X_mean, X_std, X_mean-X_95_half, X_mean+X_95_half)
	#print "for Y, mean~{}, std~{}, 95%: {}~{}".format(Y_mean, Y_std, Y_mean-Y_95_half, Y_mean+Y_95_half)

####################################################################

Tweet_Select = Results
counter = 0

while flag_res and counter < 10 :
	# calculate new 95% interval
	X_mean = np.mean(Tweet_Select[:,0])
	X_std = np.std(Tweet_Select[:,0])
	X_95_half = 3*X_std/np.sqrt(len(Tweet_Select))

	Y_mean = np.mean(Tweet_Select[:,1])
	Y_std = np.std(Tweet_Select[:,1])
	Y_95_half = 3*Y_std/np.sqrt(len(Tweet_Select))	

	print "for X, mean~{}, std~{}, 95%: {}~{}".format(X_mean, X_std, X_mean-X_95_half, X_mean+X_95_half)
	print "for Y, mean~{}, std~{}, 95%: {}~{}".format(Y_mean, Y_std, Y_mean-Y_95_half, Y_mean+Y_95_half)

	# index Tweet_Select by 95% on both X and Y
	index = np.ones(len(Tweet_Select))
	for i in range(len(Tweet_Select)):
		flag_reject = False
		if Tweet_Select[i,0] <= X_mean-X_95_half or Tweet_Select[i,0] >= X_mean+X_95_half:
			flag_reject = True
		if Tweet_Select[i,1] <= Y_mean-Y_95_half or Tweet_Select[i,1] >= Y_mean+Y_95_half:
			flag_reject = True
		if flag_reject == True:
			index[i] = 0
		else:
			index[i] = 1
	index = index == 1
	Tweet_Select = Tweet_Select[index]
	print "New Tweet_Select has {} entries.".format(len(Tweet_Select))
	# conditions
	if len(Tweet_Select) == len(Results):
		flag_res = False
	if len(Tweet_Select) <= 0.75*len(Results):
		flag_res = False
	if (X_std/np.sqrt(len(Tweet_Select)) < 1E-3) or (Y_std/np.sqrt(len(Tweet_Select)) < 1E-3):
		flag_res = False
	counter += 1



print "End of Execution"


