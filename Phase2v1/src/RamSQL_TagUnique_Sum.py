
import json
import os
import numpy as np 
import pandas as pd
import collections as col


################################################################################################
# intended for two keyword (2 projection axis)
class RamSQL_Tag:

	def __init__(self, tagText):
		
		# basic items of initialization
		self.tagText = tagText

		self.tweetID_list = []

		#########################################################
		# item for scores and calls
		
		self.totalCall = 0 # totalCall should match length of tweetID_list
		self.totalCall_window = col.Counter()
		# score history
		self.tagScore1 = []
		self.tagNcall1 = []
		self.tagScore2 = []
		self.tagNcall2 = []
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
		# items for users

		# all history
		self.user_counter = col.Counter() # use [key] += 1 directly
		# degree
		self.user_degree_abs = len(self.user_counter)
		self.user_counter_weight = col.Counter()
		self.user_degree_weighted = 0.0
		# per sliding window
		self.user_degree_abs_window = col.Counter()
		self.user_degree_weighted_window = col.Counter()

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
		self.tagScore1 = self.tagScore1 + list_input_tagScore1
		self.tagNcall1 = self.tagNcall1 + list_input_tagNcall1
		self.tagScore2 = self.tagScore2 + list_input_tagScore2
		self.tagNcall2 = self.tagNcall2 + list_input_tagNcall2

		# totalCall
		self.totalCall = self.totalCall + input_totalCall
		self.totalCall_window[table] = input_totalCall

		# update OVERALL Ave and Std
		Score1_array = np.array(self.tagScore1)
		self.Score1_Ave = np.mean(Score1_array)
		self.Score1_Std = np.std(Score1_array)
		
		Score2_array = np.array(self.tagScore2)
		self.Score2_Ave = np.mean(Score2_array)
		self.Score2_Std = np.std(Score2_array)

		# update window Ave and Std
		self.Score1_Ave_window[table] = np.mean(np.array(list_input_tagScore1))
		self.Score1_Std_window[table] = np.std(np.array(list_input_tagScore1))
		self.Score2_Ave_window[table] = np.mean(np.array(list_input_tagScore2))
		self.Score2_Std_window[table] = np.std(np.array(list_input_tagScore2))

	########################################################
	# update user information
	# Input_UserCounter is a string 
	def UserInfor_Insert(self, table, Input_UserCounter):
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
			self.user_counter = self.user_counter + col_input_userCounter
			self.user_degree_abs = len(self.user_counter)
			
			# update window information
			# update user_degree_abs_window
			self.user_degree_abs_window[table] = len(col_input_userCounter)
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
			self.user_degree_weighted_window[table] = window_userDegree_weighted

			# update user_degree_weighted and user_counter_weight
			current_userDegree_weighted = 0.0
			current_totalCall = 0.0 # not self.totalCall
			for key in self.user_counter:
				current_totalCall += self.user_counter[key]
			if current_totalCall > 0:
				for key in self.user_counter:
					current_userDegree_weighted += (1.0*self.user_counter[key]/current_totalCall)**2
					self.user_counter_weight[key] = (1.0*self.user_counter[key]/current_totalCall)
			self.user_degree_weighted = current_userDegree_weighted

	########################################################
	# update tag information
	# input is str
	def TagInfor_Insert(self, table, Input_TagCounter):
		# check if list_input_tagCounter is empty
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

	# parse totalCall_window
	def totalCall_window_str(self):
		dict_str = ""
		for key in self.totalCall_window:
			dict_str = dict_str+key+":"+str(self.totalCall_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse tagScore1
	def tagScore1_str(self):
		list_str = ""
		for item in self.tagScore1:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse tagNcall1
	def tagNcall1_str(self):
		list_str = ""
		for item in self.tagNcall1:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse tagScore2
	def tagScore2_str(self):
		list_str = ""
		for item in self.tagScore2:
			list_str = list_str + str(item) + ","
		if len(list_str) > 1:
			list_str = list_str[:-1] # get rid of the last ','
		return list_str

	# parse tagNcall1
	def tagNcall2_str(self):
		list_str = ""
		for item in self.tagNcall2:
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

	# parse user_counter
	def user_counter_str(self):
		dict_str = ""
		for key in self.user_counter:
			dict_str = dict_str+key+":"+str(self.user_counter[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse user_counter_weight
	def user_counter_weight_str(self):
		dict_str = ""
		for key in self.user_counter_weight:
			dict_str = dict_str+key+":"+str(self.user_counter_weight[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse user_degree_abs_window
	def user_degree_abs_window_str(self):
		dict_str = ""
		for key in self.user_degree_abs_window:
			dict_str = dict_str+key+":"+str(self.user_degree_abs_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse user_degree_weighted_window
	def user_degree_weighted_window_str(self):
		dict_str = ""
		for key in self.user_degree_weighted_window:
			dict_str = dict_str+key+":"+str(self.user_degree_weighted_window[key])+","
		if len(dict_str) > 1:
			dict_str = dict_str[:-1]
		return dict_str

	# parse user_degree_weighted_window
	def user_degree_weighted_window_str(self):
		dict_str = ""
		for key in self.user_degree_weighted_window:
			dict_str = dict_str+key+":"+str(self.user_degree_weighted_window[key])+","
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
		print self.tagText
		
		print "totalCall: {}".format(self.totalCall)
		print "totalCall window:\n {}".format(self.totalCall_window)
		
		print "Scores: \n{} \n{} \n{} \n{}".format(self.tagScore1, self.tagNcall1, self.tagScore2, self.tagNcall2)
		print "Scores Ave and Std: {} {} {} {}".format(self.Score1_Ave, self.Score1_Std, self.Score2_Ave, self.Score2_Std)
		print "Ave and Std of windows:\n {}\n {}\n {}\n {}".format(self.Score1_Ave_window, self.Score1_Std_window, 
			self.Score2_Ave_window, self.Score2_Std_window)
		
		print "user_counter:\n {}".format(self.user_counter)
		print "user_degree_abs: {} user_degree_weighted: {}".format(self.user_degree_abs, self.user_degree_weighted)
		print "user_counter_weight:\n {}".format(self.user_counter_weight)
		
		print "tag_counter:\n {}".format(self.tag_counter)
		print "tag_degree_abs: {} tag_degree_weighted: {}".format(self.tag_degree_abs, self.tag_degree_weighted)
		print "tag_counter_weight:\n {}".format(self.tag_counter_weight)


"""
#####################################################################################

test code

#####################################################################################
"""

if __name__ == "__main__":

	Test_Tag = RamSQL_Tag('trump')
	
	table1 = 'table1'
	input_tagScore1 = '1,2,3,4,5'
	input_tagNcall1 = '1,1,1,1,1'
	input_tagScore2 = '2,3,4,5,6'
	input_tagNcall2 = '1,1,1,1,1'
	input_totalCall = 5
	Input_UserCounter = "'12345':3,'234545':5"
	Input_TagCounter = "'trump':5,'hill':4"

	Test_Tag.ScoreInfor_Insert(table=table1, input_tagScore1=input_tagScore1, input_tagNcall1=input_tagNcall1, 
		input_tagScore2=input_tagScore2, input_tagNcall2=input_tagNcall2, input_totalCall=input_totalCall)
	Test_Tag.UserInfor_Insert(table=table1, Input_UserCounter=Input_UserCounter)
	Test_Tag.TagInfor_Insert(table=table1, Input_TagCounter=Input_TagCounter)
	#Test_Tag.tweetID_Insert(self, Input_tweetID_list)
	Test_Tag.selfPrint()

	table2 = 'table2'
	input_tagScore1 = '3,4,5,6,7'
	input_tagNcall1 = '1,1,1,1,1'
	input_tagScore2 = '4,5,6,7,8'
	input_tagNcall2 = '1,1,1,1,1'
	input_totalCall = 5
	Input_UserCounter = "'12345':3,'123412343':2"
	Input_TagCounter = "'trump':5,'job':4"

	Test_Tag.ScoreInfor_Insert(table=table2, input_tagScore1=input_tagScore1, input_tagNcall1=input_tagNcall1, 
		input_tagScore2=input_tagScore2, input_tagNcall2=input_tagNcall2, input_totalCall=input_totalCall)
	Test_Tag.UserInfor_Insert(table=table2, Input_UserCounter=Input_UserCounter)
	Test_Tag.TagInfor_Insert(table=table2, Input_TagCounter=Input_TagCounter)
	#Test_Tag.tweetID_Insert(self, Input_tweetID_list)
	Test_Tag.selfPrint()


	print "\n\nReplace"
	test_replace = Test_Tag
	print test_replace.selfPrint()





