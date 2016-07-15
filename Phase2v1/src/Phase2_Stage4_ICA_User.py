import json
import os
import numpy as np 
import pandas as pd
import collections as col
import time

import pymysql.cursors

import matplotlib.pyplot as plt

from sklearn.cluster import MiniBatchKMeans, KMeans
from sklearn.cluster import DBSCAN
from sklearn.metrics.pairwise import pairwise_distances_argmin
from sklearn.datasets.samples_generator import make_blobs
from sklearn.decomposition import PCA, FastICA





"""
##############################################################################
"""
def Data_Extract():

	#######################################################################

	MySQL_DBkey2 = {'host':'localhost', 'user':'sa', 'password':'fanyu01', 'db':'ultra_v4_phase1_run2','charset':'utf8mb4'}

	# command
	comd_Score_TH = "\
	select userID, Score1_LB, Score2_LB, Norm_totalAction, \
	Norm_Muser_degree_abs, Norm_Muser_degree_ws, Norm_tag_degree_abs, Norm_tag_degree_ws\n\
	from userunique_all\n\
	where Norm_totalAction >= -10 and Norm_Muser_degree_abs >= -10 and Norm_tag_degree_abs >= -10;\n"

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
				temp_data[0].append( str(item['userID']))
				temp_data[1].append( float(item['Score1_LB']))
				temp_data[2].append( float(item['Score2_LB']))
				temp_data[3].append( float(item['Norm_totalAction']))
				temp_data[4].append( float(item['Norm_Muser_degree_abs']))
				temp_data[5].append( float(item['Norm_Muser_degree_ws']))
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
	
	index = [0,1,3,5]

	# ICA
	rng = np.random.RandomState(42)
	ica = FastICA(random_state=rng)
	X_ica = ica.fit(DataPoints[:,index]).transform(DataPoints[:,index])  # Estimate the sources
	X_ica /= X_ica.std(axis=0)

	##############################################################################

	##############################################################################
	# Plot result
	fig = plt.figure(figsize=(16, 16))
	fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)

	# ICA
	ax = fig.add_subplot(2, 3, 1)
	ax.plot(X_ica[:, 0], X_ica[:, 1], 'w',
				markerfacecolor='blue', marker='.')
	ax.set_title('ICA compnent 1 vs 2, Full Data Set')   

	# ICA
	ax = fig.add_subplot(2, 3, 2)
	ax.plot(X_ica[:, 0], X_ica[:, 2], 'w',
				markerfacecolor='blue', marker='.')
	ax.set_title('ICA compnent 1 vs 3, Full Data Set')   

	# ICA
	ax = fig.add_subplot(2, 3, 3)
	ax.plot(X_ica[:, 0], X_ica[:, 3], 'w',
				markerfacecolor='blue', marker='.')
	ax.set_title('ICA compnent 1 vs 4, Full Data Set')

	# ICA
	ax = fig.add_subplot(2, 3, 4)
	ax.plot(X_ica[:, 1], X_ica[:, 2], 'w',
				markerfacecolor='blue', marker='.')
	ax.set_title('ICA compnent 2 vs 3, Full Data Set')

	# ICA
	ax = fig.add_subplot(2, 3, 5)
	ax.plot(X_ica[:, 1], X_ica[:, 3], 'w',
				markerfacecolor='blue', marker='.')
	ax.set_title('ICA compnent 2 vs 4, Full Data Set')

	# ICA
	ax = fig.add_subplot(2, 3, 6)
	ax.plot(X_ica[:, 2], X_ica[:, 3], 'w',
				markerfacecolor='blue', marker='.')
	ax.set_title('ICA compnent 3 vs 4, Full Data Set')


	####################################################################
	plt.savefig('../output/ICA_Components.png'.format(n_clusters))
	plt.show()
	####################################################################

	##############################################################################


	# DBScan 
	print "DBScan with PCA results"

	index = [2,3]
	db = DBSCAN(eps=0.5, min_samples=50, algorithm='kd_tree').fit(X_ica[:,index])
	ICA_core_samples_mask = np.zeros_like(db.labels_, dtype=bool)
	ICA_core_samples_mask[db.core_sample_indices_] = True
	ICA_labels = db.labels_

	# Number of clusters in labels, ignoring noise if present.
	n_clusters_ = len(set(ICA_labels)) - (1 if -1 in ICA_labels else 0)
	print('Estimated number of clusters: %d' % n_clusters_)


	##############################################################################

	##############################################################################
	
	# Plot result
	fig = plt.figure(figsize=(16, 16))
	fig.subplots_adjust(left=0.02, right=0.98, bottom=0.05, top=0.9)


	# DBScan on ICA by Scores
	ax = fig.add_subplot(3, 3, 1)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	colors = plt.cm.Spectral(np.linspace(0, 1, len(unique_labels)))
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = DataPoints[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = DataPoints[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 1], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by Scores')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	


	# DBScan on ICA by Scores
	ax = fig.add_subplot(3, 3, 2)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = DataPoints[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 3], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = DataPoints[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 3], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by Score1 vs User_Degree')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	


	# DBScan on ICA by Scores
	ax = fig.add_subplot(3, 3, 3)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = DataPoints[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 3], xy[:, 5], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = DataPoints[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 3], xy[:, 5], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by User_Degree vs Tag_Degree')
	label_range = [i-2 for i in range(14)]
	ax.set_xticks(label_range)
	ax.set_xticklabels(label_range)
	ax.set_yticks(label_range)
	ax.set_yticklabels(label_range)	


	# DBScan on ICA by ICA
	ax = fig.add_subplot(3, 3, 4)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = X_ica[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 1], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = X_ica[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 1], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by ICA components 1&2')


	# DBScan on ICA by ICA
	ax = fig.add_subplot(3, 3, 5)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = X_ica[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 2], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = X_ica[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 2], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by ICA components 1&3')	


	# DBScan on ICA by ICA
	ax = fig.add_subplot(3, 3, 6)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = X_ica[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 3], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = X_ica[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 0], xy[:, 3], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by ICA components 1&4')


	# DBScan on ICA by ICA
	ax = fig.add_subplot(3, 3, 7)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = X_ica[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 1], xy[:, 2], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = X_ica[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 1], xy[:, 2], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by ICA components 2&3')


	# DBScan on ICA by ICA
	ax = fig.add_subplot(3, 3, 8)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = X_ica[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 1], xy[:, 3], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = X_ica[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 1], xy[:, 3], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by ICA components 2&4')


	# DBScan on ICA by ICA
	ax = fig.add_subplot(3, 3, 9)
	# Black removed and is used for noise instead.
	unique_labels = set(ICA_labels)
	for k, col in zip(unique_labels, colors):
		if k == -1:
			# Black used for noise.
			col = 'k'
		class_member_mask = (ICA_labels == k)
		xy = X_ica[class_member_mask & ICA_core_samples_mask]
		ax.plot(xy[:, 2], xy[:, 3], 'o', markerfacecolor=col,
				 markeredgecolor='k', markersize=6)
		xy = X_ica[class_member_mask & ~ICA_core_samples_mask]
		ax.plot(xy[:, 2], xy[:, 3], '.', markerfacecolor=col,
				 markeredgecolor='k', markersize=1)
	ax.set_title('DSscan on ICA, by ICA components 3&4')


	####################################################################
	plt.savefig('../output/User_DBscan_ICA_W{}.png'.format(weight))
	plt.show()
	####################################################################


"""
#################################################################################
"""

if __name__ == "__main__":
	# extract data in python list format; NOT np.array
	Data_list = Data_Extract()

	Gross_K_means(Data_list =Data_list, n_clusters=5, n_init=500, max_iter=500, weight=1)

