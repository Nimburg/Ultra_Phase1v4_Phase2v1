
import json
import os
import numpy as np 
import pandas as pd
import collections as col


################################################################################################
# intended for two keyword (2 projection axis)

class RamSQL_User:

	def __init__(self, user, userName, followers_count=0, friends_count=0):
		
		# basic items of initialization
		self.user = user
		self.userName = userName
		self.followers_count = followers_count
		self.friends_count = friends_count

		# list of tweetIDs
		self.tweetID_list = []

		#########################################################
		# item for scores and calls
		self.totalAction = 0 
		self.totalAction_window = col.Counter()
		# score history
		self.userScore1 = []
		self.userNcall1 = []
		self.userScore2 = []
		self.userNcall2 = []		
		# Ave and Std of scores
		self.Score1_Ave = 0.0
		self.Score1_Std = 0.0
		self.Score2_Ave = 0.0
		self.Score2_Std = 0.0
		# Ave and Std of scores per sliding window; key as table name
		self.Score1_Ave_window = col.Counter()
		self.Score1_Std_window = col.Counter()
		self.Score2_Ave_window = col.Counter()
		self.Score2_Std_window = col.Counter()

		#########################################################
		# items for Mentioned users

		# all history
		self.Muser_counter = col.Counter()
		# degree
		self.Muser_degree_abs = len(self.Muser_counter)
		self.Muser_counter_weight = col.Counter()
		self.Muser_degree_weighted = 0.0
		# per sliding window
		self.Muser_degree_abs_window = col.Counter()
		self.Muser_degree_weighted_window = col.Counter()

		#########################################################
		# items for Replied users

		# all history
		self.Ruser_counter = col.Counter()
		# degree
		self.Ruser_degree_abs = len(self.Ruser_counter)
		self.Ruser_counter_weight = col.Counter()
		self.Ruser_degree_weighted = 0.0
		# per sliding window
		self.Ruser_degree_abs_window = col.Counter()
		self.Ruser_degree_weighted_window = col.Counter()

		#########################################################
		# items for tags

		# all history
		self.tag_counter = col.Counter()
		# degree
		self.tag_degree_abs = len(self.tag_counter)
		self.tag_counter_weight = col.Counter()
		self.tag_degree_weighted = 0.0
		# per sliding window
		self.tag_degree_abs_window = col.Counter()
		self.tag_degree_weighted_window = col.Counter()

	################################################################################################

	################################################################################################
	# ALL THE UPDATE FUNCTIONS PERFORMS PER EACH TABLE
	# Insert functions should be called in THIS ORDER as listed

	########################################################
	# update scores and totalCall
	# input are strings extracted from SQL tables
	# need to parse into list first, then convert from str to float
	def ScoreInfor_Insert(self, table, input_tagScore1, input_tagNcall1, input_tagScore2, input_tagNcall2, input_totalCall):
		
		# parse inputs
		list_input_tagScore1 = input_tagScore1.split(',')
		list_input_tagNcall1 = input_tagNcall1.split(',')
		list_input_tagScore2 = input_tagScore2.split(',')
		list_input_tagNcall2 = input_tagNcall2.split(',')
		# convert str to float
		for i in range(len(list_input_tagScore1)):
			list_input_tagScore1[i] = float(list_input_tagScore1[i])

		for i in range(len(list_input_tagNcall1)):
			list_input_tagNcall1[i] = int(list_input_tagNcall1[i])

		for i in range(len(list_input_tagScore2)):
			list_input_tagScore2[i] = float(list_input_tagScore2[i])

		for i in range(len(list_input_tagNcall2)):
			list_input_tagNcall2[i] = int(list_input_tagNcall2[i])

		# load into self.
		self.userScore1 = self.userScore1 + list_input_tagScore1
		self.userNcall1 = self.userNcall1 + list_input_tagNcall1
		self.userScore2 = self.userScore2 + list_input_tagScore2
		self.userNcall2 = self.userNcall2 + list_input_tagNcall2

		# totalCall
		self.totalAction = self.totalAction + input_totalCall
		self.totalAction_window[table] = input_totalCall

		# update OVERALL Ave and Std
		Score1_array = np.array(self.userScore1)
		self.Score1_Ave = np.mean(Score1_array)
		self.Score1_Std = np.std(Score1_array)
		
		Score2_array = np.array(self.userScore2)
		self.Score2_Ave = np.mean(Score2_array)
		self.Score2_Std = np.std(Score2_array)

		# update window Ave and Std
		self.Score1_Ave_window[table] = np.mean(np.array(list_input_tagScore1))
		self.Score1_Std_window[table] = np.std(np.array(list_input_tagScore1))
		self.Score2_Ave_window[table] = np.mean(np.array(list_input_tagScore2))
		self.Score2_Std_window[table] = np.std(np.array(list_input_tagScore2))

	########################################################
	# update Mentioned user information
	# Input_UserCounter is a string 
	def Men_UserInfor_Insert(self, table, Input_UserCounter):
		# check if is empty
		if len(Input_UserCounter) > 3 and (':' in Input_UserCounter):
			# parse Input_UserCounter into dict{}
			# str to list
			list_input_userCounter = Input_UserCounter.split(',')
			# each list item to dict item
			col_input_userCounter = col.Counter()
			for item in list_input_userCounter:
				templist = item.split(':')
				userID = templist[0]
				userActs = int(templist[1])
				col_input_userCounter[userID] = userActs
			# add to self.user_counter
			self.Muser_counter = self.Muser_counter + col_input_userCounter
			self.Muser_degree_abs = len(self.Muser_counter)
			
			# update window information
			# update user_degree_abs_window
			self.Muser_degree_abs_window[table] = len(col_input_userCounter)
			# calculate user_degree_weighted_window
			# this totalCall is how many call instances of all users, >= tweetID number
			window_totalCall = 0
			for key in col_input_userCounter:
				window_totalCall += col_input_userCounter[key]
			# calculate for weights; using (act/total_act)^2 per each user
			window_userDegree_weighted = 0.0
			if window_totalCall > 0:
				for key in col_input_userCounter:
					window_userDegree_weighted += (1.0*col_input_userCounter[key]/window_totalCall)**2
			self.Muser_degree_weighted_window[table] = window_userDegree_weighted

			# update user_degree_weighted and user_counter_weight
			current_userDegree_weighted = 0.0
			current_totalCall = 0.0 # not self.totalCall
			for key in self.Muser_counter:
				current_totalCall += self.Muser_counter[key]
			if current_totalCall > 0:
				for key in self.Muser_counter:
					current_userDegree_weighted += (1.0*self.Muser_counter[key]/current_totalCall)**2
					self.Muser_counter_weight[key] = (1.0*self.Muser_counter[key]/current_totalCall)
			self.Muser_degree_weighted = current_userDegree_weighted

	########################################################
	# update Replied to user information
	# Input_UserCounter is a string 
	def Reply_UserInfor_Insert(self, table, Input_UserCounter):
		# check if is empty
		if len(Input_UserCounter) > 3 and (':' in Input_UserCounter):
			# parse Input_UserCounter into dict{}
			# str to list
			list_input_userCounter = Input_UserCounter.split(',')
			# each list item to dict item
			col_input_userCounter = col.Counter()
			for item in list_input_userCounter:
				templist = item.split(':')
				userID = templist[0]
				userActs = int(templist[1])
				col_input_userCounter[userID] = userActs
			# add to self.user_counter
			self.Ruser_counter = self.Ruser_counter + col_input_userCounter
			self.Ruser_degree_abs = len(self.Ruser_counter)
			
			# update window information
			# update user_degree_abs_window
			self.Ruser_degree_abs_window[table] = len(col_input_userCounter)
			# calculate user_degree_weighted_window
			# this totalCall is how many call instances of all users, >= tweetID number
			window_totalCall = 0
			for key in col_input_userCounter:
				window_totalCall += col_input_userCounter[key]
			# calculate for weights; using (act/total_act)^2 per each user
			window_userDegree_weighted = 0.0
			if window_totalCall > 0:
				for key in col_input_userCounter:
					window_userDegree_weighted += (1.0*col_input_userCounter[key]/window_totalCall)**2
			self.Ruser_degree_weighted_window[table] = window_userDegree_weighted

			# update user_degree_weighted and user_counter_weight
			current_userDegree_weighted = 0.0
			current_totalCall = 0.0 # not self.totalCall
			for key in self.Ruser_counter:
				current_totalCall += self.Ruser_counter[key]
			if current_totalCall > 0:
				for key in self.Ruser_counter:
					current_userDegree_weighted += (1.0*self.Ruser_counter[key]/current_totalCall)**2
					self.Ruser_counter_weight[key] = (1.0*self.Ruser_counter[key]/current_totalCall)
			self.Ruser_degree_weighted = current_userDegree_weighted

	########################################################
	# update tag information
	# input is str
	def TagInfor_Insert(self, table, Input_TagCounter):
		# check if is empty
		if len(Input_TagCounter) > 3 and (':' in Input_TagCounter):
			# parse Input_TagCounter into Counter
			list_input_tagCounter = Input_TagCounter.split(',')
			# each list item to dict item
			col_input_tagCounter = col.Counter()
			for item in list_input_tagCounter:
				templist = item.split(':')
				tag = templist[0]
				tagCalls = int(templist[1])
				col_input_tagCounter[tag] = tagCalls
			# add to self.user_counter
			self.tag_counter = self.tag_counter + col_input_tagCounter		
			self.tag_degree_abs = len(self.tag_counter)

			# update window information
			# update tag_degree_abs_window
			self.tag_degree_abs_window[table] = len(col_input_tagCounter)
			# calculate tag_degree_weighted_window
			# this totalCall is how many call instances of all tags, >= tweetID number
			window_totalCall = 0
			for key in col_input_tagCounter:
				window_totalCall += col_input_tagCounter[key]
			# calculate for weights; using (act/total_act)^2 per each user
			window_tagDegree_weighted = 0.0
			if window_totalCall > 0:
				for key in col_input_tagCounter:
					window_tagDegree_weighted += (1.0*col_input_tagCounter[key]/window_totalCall)**2
			self.tag_degree_weighted_window[table] = window_tagDegree_weighted

			# update tag_degree_weighted and tag_counter_weight
			current_tagDegree_weighted = 0.0
			current_totalCall = 0.0 # not self.totalCall
			for key in self.tag_counter:
				current_totalCall += self.tag_counter[key]
			if current_totalCall > 0:
				for key in self.tag_counter:
					current_tagDegree_weighted += (1.0*self.tag_counter[key]/current_totalCall)**2
					self.tag_counter_weight[key] = (1.0*self.tag_counter[key]/current_totalCall)
			self.tag_degree_weighted = current_tagDegree_weighted

	########################################################
	# update tweetID list
	# input is str, append to list
	def tweetID_Insert(self, Input_tweetID_list):
		# parse into list
		list_input_tweetID = Input_tweetID_list.split(',')
		# add to list
		for item in list_input_tweetID:
			if len(item) > 0:
				self.tweetID_list.append(item)

	################################################################################################

	################################################################################################

	# parse tweetID_list into string
	def tweetID_list_str(self):
		list_str = ""
		for item in self.tweetID_list:
			list_str = list_str + item + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse totalAction_window
	def totalAction_window_str(self):
		dict_str = ""
		for key in self.totalAction_window:
			dict_str = dict_str+key+":"+str(self.totalAction_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse userScore1
	def userScore1_str(self):
		list_str = ""
		for item in self.userScore1:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse userNcall1
	def userNcall1_str(self):
		list_str = ""
		for item in self.userNcall1:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse userScore2
	def userScore2_str(self):
		list_str = ""
		for item in self.userScore2:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse userNcall2
	def userNcall2_str(self):
		list_str = ""
		for item in self.userNcall2:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse Score1_Ave_window
	def Score1_Ave_window_str(self):
		dict_str = ""
		for key in self.Score1_Ave_window:
			dict_str = dict_str+key+":"+str(self.Score1_Ave_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Score1_Std_window
	def Score1_Std_window_str(self):
		dict_str = ""
		for key in self.Score1_Std_window:
			dict_str = dict_str+key+":"+str(self.Score1_Std_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Score2_Ave_window
	def Score2_Ave_window_str(self):
		dict_str = ""
		for key in self.Score2_Ave_window:
			dict_str = dict_str+key+":"+str(self.Score2_Ave_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Score2_Std_window
	def Score2_Std_window_str(self):
		dict_str = ""
		for key in self.Score2_Std_window:
			dict_str = dict_str+key+":"+str(self.Score2_Std_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Muser_counter
	def Muser_counter_str(self):
		dict_str = ""
		for key in self.Muser_counter:
			dict_str = dict_str+key+":"+str(self.Muser_counter[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Muser_counter_weight
	def Muser_counter_weight_str(self):
		dict_str = ""
		for key in self.Muser_counter_weight:
			dict_str = dict_str+key+":"+str(self.Muser_counter_weight[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Muser_degree_abs_window
	def Muser_degree_abs_window_str(self):
		dict_str = ""
		for key in self.Muser_degree_abs_window:
			dict_str = dict_str+key+":"+str(self.Muser_degree_abs_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Muser_degree_weighted_window
	def Muser_degree_weighted_window_str(self):
		dict_str = ""
		for key in self.Muser_degree_weighted_window:
			dict_str = dict_str+key+":"+str(self.Muser_degree_weighted_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Ruser_counter
	def Ruser_counter_str(self):
		dict_str = ""
		for key in self.Ruser_counter:
			dict_str = dict_str+key+":"+str(self.Ruser_counter[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Ruser_counter_weight
	def Ruser_counter_weight_str(self):
		dict_str = ""
		for key in self.Ruser_counter_weight:
			dict_str = dict_str+key+":"+str(self.Ruser_counter_weight[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Ruser_degree_abs_window
	def Ruser_degree_abs_window_str(self):
		dict_str = ""
		for key in self.Ruser_degree_abs_window:
			dict_str = dict_str+key+":"+str(self.Ruser_degree_abs_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse Ruser_degree_weighted_window
	def Ruser_degree_weighted_window_str(self):
		dict_str = ""
		for key in self.Ruser_degree_weighted_window:
			dict_str = dict_str+key+":"+str(self.Ruser_degree_weighted_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse tag_counter
	def tag_counter_str(self):
		dict_str = ""
		for key in self.tag_counter:
			dict_str = dict_str+key+":"+str(self.tag_counter[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse tag_counter_weight
	def tag_counter_weight_str(self):
		dict_str = ""
		for key in self.tag_counter_weight:
			dict_str = dict_str+key+":"+str(self.tag_counter_weight[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse tag_degree_abs_window
	def tag_degree_abs_window_str(self):
		dict_str = ""
		for key in self.tag_degree_abs_window:
			dict_str = dict_str+key+":"+str(self.tag_degree_abs_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse tag_degree_weighted_window
	def tag_degree_weighted_window_str(self):
		dict_str = ""
		for key in self.tag_degree_weighted_window:
			dict_str = dict_str+key+":"+str(self.tag_degree_weighted_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	################################################################################################

	################################################################################################
	# just in case
	def selfPrint(self):
		print "{} {} followers:{} friends:{}".format(self.user, self.userName, self.followers_count, self.friends_count)
		
		print "totalAction: {}".format(self.totalAction)
		print "totalAction window:\n {}".format(self.totalAction_window)
		
		print "Scores: \n{} \n{} \n{} \n{}".format(self.userScore1, self.userNcall1, self.userScore2, self.userNcall2)
		print "Scores Ave and Std: {} {} {} {}".format(self.Score1_Ave, self.Score1_Std, self.Score2_Ave, self.Score2_Std)
		print "Ave and Std of windows:\n {}\n {}\n {}\n {}".format(self.Score1_Ave_window, self.Score1_Std_window, 
			self.Score2_Ave_window, self.Score2_Std_window)
		
		print "Muser_counter:\n {}".format(self.Muser_counter)
		print "Muser_degree_abs: {} Muser_degree_weighted: {}".format(self.Muser_degree_abs, self.Muser_degree_weighted)
		print "Muser_counter_weight:\n {}".format(self.Muser_counter_weight)

		print "Ruser_counter:\n {}".format(self.Ruser_counter)
		print "Ruser_degree_abs: {} Ruser_degree_weighted: {}".format(self.Ruser_degree_abs, self.Ruser_degree_weighted)
		print "Ruser_counter_weight:\n {}".format(self.Ruser_counter_weight)

		print "tag_counter:\n {}".format(self.tag_counter)
		print "tag_degree_abs: {} tag_degree_weighted: {}".format(self.tag_degree_abs, self.tag_degree_weighted)
		print "tag_counter_weight:\n {}".format(self.tag_counter_weight)



"""
#####################################################################################

test code

#####################################################################################
"""

if __name__ == "__main__":

	Test_User = RamSQL_User(user='1234', userName='asdf')

	table1 = 'table1'
	input_tagScore1 = '1,2,3,4,5'
	input_tagNcall1 = '1,1,1,1,1'
	input_tagScore2 = '2,3,4,5,6'
	input_tagNcall2 = '1,1,1,1,1'
	input_totalCall = 5
	Input_UserCounter = "'12345':3,'234545':5"
	Input_TagCounter = "'trump':5,'hill':4"

	Test_User.ScoreInfor_Insert(table=table1, input_tagScore1=input_tagScore1, input_tagNcall1=input_tagNcall1, 
		input_tagScore2=input_tagScore2, input_tagNcall2=input_tagNcall2, input_totalCall=input_totalCall)
	Test_User.Men_UserInfor_Insert(table=table1, Input_UserCounter=Input_UserCounter)
	Test_User.Reply_UserInfor_Insert(table=table1, Input_UserCounter=Input_UserCounter)
	Test_User.TagInfor_Insert(table=table1, Input_TagCounter=Input_TagCounter)
	Test_User.selfPrint()

	table2 = 'table2'
	input_tagScore1 = '3,4,5,6,7'
	input_tagNcall1 = '1,1,1,1,1'
	input_tagScore2 = '4,5,6,7,8'
	input_tagNcall2 = '1,1,1,1,1'
	input_totalCall = 5
	Input_UserCounter = "'12345':3,'123412343':2"
	Input_TagCounter = "'trump':5,'job':4"

	Test_User.ScoreInfor_Insert(table=table2, input_tagScore1=input_tagScore1, input_tagNcall1=input_tagNcall1, 
		input_tagScore2=input_tagScore2, input_tagNcall2=input_tagNcall2, input_totalCall=input_totalCall)
	Test_User.Men_UserInfor_Insert(table=table2, Input_UserCounter=Input_UserCounter)
	Test_User.Reply_UserInfor_Insert(table=table2, Input_UserCounter=Input_UserCounter)
	Test_User.TagInfor_Insert(table=table2, Input_TagCounter=Input_TagCounter)
	Test_User.selfPrint()
