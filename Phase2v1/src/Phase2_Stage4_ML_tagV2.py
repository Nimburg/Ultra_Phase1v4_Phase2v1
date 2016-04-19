import json
import os
import numpy as np 
import pandas as pd
import collections as col
import time

import pymysql.cursors

import matplotlib.pyplot as plt

from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn.metrics.pairwise import pairwise_distances_argmin
from sklearn.datasets.samples_generator import make_blobs





"""
##############################################################################
"""
def Data_Extract():

	#######################################################################

	MySQL_DBkey2 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}

	# command
	comd_Score_TH = "\
	select tagText, Score1_LB, Score2_LB, Norm_totalCall, \
	Norm_user_degree_abs, Norm_user_degree_ws, Norm_tag_degree_abs, Norm_tag_degree_ws\n\
	from tagunique_all\n\
	where Norm_totalCall >= -10 and Norm_user_degree_abs >= -10 and Norm_tag_degree_abs >= -10;\n"

	temp_data = [[],[],[],[],[],[],[],[]]

	# Connect to the database
	connection = pymysql.connect(host=MySQL_DBkey2['host'],
								 user=MySQL_DBkey2['user'],
								 password=MySQL_DBkey2['password'],
								 db=MySQL_DBkey2['db'],
								 charset=MySQL_DBkey2['charset'],
								 cursorclass=pymysql.cursors.DictCursor)
	print "Extracting data"
	try: 
		with connection.cursor() as cursor:
			cursor.execute(comd_Score_TH)
			result = cursor.fetchall()
			# result is a list of dicts: {u'tagText': u'100yearsold'}
			print "Finished extracting data"
			for item in result:
				temp_data[0].append( str(item['tagText']))
				temp_data[1].append( float(item['Score1_LB']))
				temp_data[2].append( float(item['Score2_LB']))
				temp_data[3].append( float(item['Norm_totalCall']))
				temp_data[4].append( float(item['Norm_user_degree_abs']))
				temp_data[5].append( float(item['Norm_user_degree_ws']))
				temp_data[6].append( float(item['Norm_tag_degree_abs']))
				temp_data[7].append( float(item['Norm_tag_degree_ws']))	
	finally:
		pass
	
	connection.close()
	return temp_data


"""
##############################################################################
"""



def Gross_K_means(Data_list, n_clusters, n_init, max_iter, weight):

	# K-means
	#n_clusters = 9
	#n_init = 100
	#max_iter = 100
	# top 11 colors
	colors = ['firebrick', 'red', 'orange', 'yellow','tan', 'green', 'skyblue', 'blue', 'violet', 'magenta','black']

	##############################################################################
	# convert to numpy array
	temp_data = Data_list
	Data_TH = np.array(temp_data[1:][:])
	# rewrite array format into data points
	DataPoints = []
	for i in range(len(Data_TH[0])):
		DataPoints.append([Data_TH[0,i],Data_TH[1,i],Data_TH[2,i],weight*Data_TH[3,i],weight*Data_TH[4,i],
			weight*Data_TH[5,i],weight*Data_TH[6,i]])
	DataPoints = np.array(DataPoints)

	##############################################################################
	# plot data for demonstration
	fig = plt.figure(figsize=(16, 16))
	fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)
	
	# Score1 and Score2
	ax = fig.add_subplot(2, 3, 1)
	ax.plot(DataPoints[:, 0], DataPoints[:, 1], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('Score1 vs Score2')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Muser_degree_abs and Muser_degree_ws
	ax = fig.add_subplot(2, 3, 2)
	ax.plot(DataPoints[:, 3], DataPoints[:, 4], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('User_Degree_abs vs User_Degree_weighted')
	label_range = [weight*(i+2) for i in range(10)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Tag_Degree_abs and Tag_Degree_weighted
	ax = fig.add_subplot(2, 3, 3)
	ax.plot(DataPoints[:, 5], DataPoints[:, 6], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('Tag_Degree_abs vs Tag_Degree_weighted')
	label_range = [weight*(i+2) for i in range(10)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Score1 and Norm_totalAction
	ax = fig.add_subplot(2, 3, 4)
	ax.plot(DataPoints[:, 0], DataPoints[:, 2], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('Score1 vs Norm_totalAction')
	label_x = [i-2 for i in range(14)]
	label_y = [i+2 for i in range(10)]
	ax.set_xticks(label_x)
	ax.set_xticklabels(label_x)
	ax.set_yticks(label_y)
	ax.set_yticklabels(label_y)	

	# Score2 and Norm_totalAction
	ax = fig.add_subplot(2, 3, 5)
	ax.plot(DataPoints[:, 1], DataPoints[:, 2], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('Score2 vs Norm_totalAction')
	label_x = [i-2 for i in range(14)]
	label_y = [i+2 for i in range(10)]
	ax.set_xticks(label_x)
	ax.set_xticklabels(label_x)
	ax.set_yticks(label_y)
	ax.set_yticklabels(label_y)	
	
	# tag_degree_abs and user_degree_abs
	ax = fig.add_subplot(2, 3, 6)
	ax.plot(DataPoints[:, 5], DataPoints[:, 3], 'w', markerfacecolor='blue', marker='.')
	ax.set_title('tag_degree_abs vs user_degree_abs')
	label_x = [weight*(i+2) for i in range(10)]
	label_y = [weight*(i+2) for i in range(10)]
	ax.set_xticks(label_x)
	ax.set_xticklabels(label_x)
	ax.set_yticks(label_y)
	ax.set_yticklabels(label_y)	

	plt.savefig('../output/Tag_DataDemo.png')
	plt.show()


	##############################################################################

	##############################################################################

	# K-means with Scores ONLY
	print "K-means with Scores ONLY"
	k_means = KMeans(init='k-means++', n_clusters=n_clusters, n_init=n_init, max_iter = max_iter)
	k_means.fit(DataPoints[:,:2])
	Scores_labels = k_means.labels_
	Scores_cluster_centers = k_means.cluster_centers_
	Scores_labels_unique = np.unique(Scores_labels)

	# K-means with Scores and totalAction
	print "K-means with Scores and totalAction"
	k_means.fit(DataPoints[:,:3])

	Scores_Actions_labels = k_means.labels_
	Scores_Actions_cluster_centers = k_means.cluster_centers_
	Scores_Actions_labels_unique = np.unique(Scores_Actions_labels)

	# K-means with Scores and tag_degree_abs
	print "K-means with Scores and tag_degree_abs"
	index = [0,1,5]
	k_means.fit(DataPoints[:,index])

	Scores_tagDegree_labels = k_means.labels_
	Scores_tagDegree_centers = k_means.cluster_centers_
	Scores_tagDegree_unique = np.unique(Scores_tagDegree_labels)

	# K-means with Scores and user_degree_abs
	print "K-means with Scores and user_degree_abs"
	index = [0,1,3]
	k_means.fit(DataPoints[:,index])

	Scores_userDegree_labels = k_means.labels_
	Scores_userDegree_centers = k_means.cluster_centers_
	Scores_userDegree_unique = np.unique(Scores_userDegree_labels)

	# K-means with Scores and user_degree_abs and tag_degree_abs
	print "K-means with Scores and user_degree_abs and tag_degree_abs"
	index = [0,1,3,5]
	k_means.fit(DataPoints[:,index])

	Scores_bothDegree_labels = k_means.labels_
	Scores_bothDegree_centers = k_means.cluster_centers_
	Scores_bothDegree_unique = np.unique(Scores_bothDegree_labels)

	##############################################################################
	# We want to have the same colors for the same cluster from the
	# MiniBatchKMeans and the KMeans algorithm. Let's pair the cluster centers per
	# closest one.

	order_SA = pairwise_distances_argmin(Scores_cluster_centers, Scores_Actions_cluster_centers[:,0:2])
	order_ST = pairwise_distances_argmin(Scores_cluster_centers, Scores_tagDegree_centers[:,0:2])
	order_SU = pairwise_distances_argmin(Scores_cluster_centers, Scores_userDegree_centers[:,0:2])
	order_STU = pairwise_distances_argmin(Scores_cluster_centers, Scores_bothDegree_centers[:,0:2])

	print "\nScores_cluster_centers:\n ", Scores_cluster_centers
	print "\nScores_Actions_cluster_centers:\n ", Scores_Actions_cluster_centers[order_SA]
	print "\nScores_tagDegree_centers:\n ", Scores_tagDegree_centers[order_ST]
	print "\nScores_userDegree_centers:\n ", Scores_userDegree_centers[order_SU]
	print "\nScores_bothDegree_centers:\n ", Scores_bothDegree_centers[order_STU]

	##############################################################################

	##############################################################################
	# Plot result
	fig = plt.figure(figsize=(16, 16))
	fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)
	
	# Scores
	ax = fig.add_subplot(2, 3, 1)
	for k, col in zip(range(n_clusters), colors):
		my_members = Scores_labels == k
		cluster_center = Scores_cluster_centers[k]	
		ax.plot(DataPoints[my_members, 0], DataPoints[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('Clustering of Scores')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Scores and totalAction
	ax = fig.add_subplot(2, 3, 2)
	for k, col in zip(range(n_clusters), colors):
		my_members = Scores_Actions_labels == order_SA[k]
		cluster_center = Scores_Actions_cluster_centers[order_SA[k]]	
		ax.plot(DataPoints[my_members, 0], DataPoints[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=8)

	ax.set_title('Scores and totalAction')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Scores and tag_degree
	ax = fig.add_subplot(2, 3, 3)
	for k, col in zip(range(n_clusters), colors):
		my_members = Scores_tagDegree_labels == order_ST[k]
		cluster_center = Scores_tagDegree_centers[order_ST[k]]	
		ax.plot(DataPoints[my_members, 0], DataPoints[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('Scores and tag_degree')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Scores and user_degree
	ax = fig.add_subplot(2, 3, 4)
	for k, col in zip(range(n_clusters), colors):
		my_members = Scores_userDegree_labels == order_SU[k]
		cluster_center = Scores_userDegree_centers[order_SU[k]]	
		ax.plot(DataPoints[my_members, 0], DataPoints[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('Scores and user_degree')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# Scores and both_Degrees
	ax = fig.add_subplot(2, 3, 5)
	for k, col in zip(range(n_clusters), colors):
		my_members = Scores_bothDegree_labels == order_STU[k]
		cluster_center = Scores_bothDegree_centers[order_STU[k]]	
		ax.plot(DataPoints[my_members, 0], DataPoints[my_members, 1], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[0], cluster_center[1], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('Scores and both_Degrees')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	# user_degree vs tag_degree
	ax = fig.add_subplot(2, 3, 6)
	for k, col in zip(range(n_clusters), colors):
		my_members = Scores_bothDegree_labels == order_STU[k]
		cluster_center = Scores_bothDegree_centers[order_STU[k]]	
		ax.plot(DataPoints[my_members, 3], DataPoints[my_members, 5], 'w',
				markerfacecolor=col, marker='.')
		ax.plot(cluster_center[2], cluster_center[3], 'o', markerfacecolor=col,
				markeredgecolor='k', markersize=6)

	ax.set_title('user_degree vs tag_degree')
	label_range = [weight*(i+2) for i in range(10)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	

	####################################################################
	plt.savefig('../output/Tag_Kmeans_N{}_W3_5.png'.format(n_clusters))
	plt.show()
	####################################################################


"""
#################################################################################
"""

if __name__ == "__main__":
	# extract data in python list format; NOT np.array
	Data_list = Data_Extract()

	Gross_K_means(Data_list =Data_list, n_clusters=9, n_init=500, max_iter=500, weight=3.5)

