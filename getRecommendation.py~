import calendar
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams
import numpy as np
import random
import json
import csv

def get_recommendation_list():
	dict_business = {}
	b_json = open("/home/rachita/Desktop/mr_final_project/yelp_academic_dataset_business.json")
	for line in b_json:
		if len(line) > 0:
			data_b = json.loads(line)
			if data_b["business_id"] not in dict_business:
				dict_business.update({data_b["business_id"] : data_b["name"]})
	b_json.close();

	dict_user = {}
	u_json = open("/home/rachita/Desktop/mr_final_project/yelp_academic_dataset_user.json")
	for line in u_json:
		if len(line) > 0:
			data_u = json.loads(line)
			if data_u["user_id"] not in dict_user:
				dict_user.update({data_u["user_id"]: data_u["name"]})
	u_json.close();


	dict_recommendation = {}
	rdata = open("/home/rachita/Desktop/mr_final_project/myRecommendations")
	for line in rdata:
		if len(line) > 0:
			user_id = line.strip('\n').split("\t")[0]
			score = line.strip('\n').split("\t")[1]
			if user_id not in dict_recommendation:
				dict_recommendation.update({user_id: score});
	rdata.close()

	# print(lst_recommendation)

	dict_sim ={}
	sim_data = open("/home/rachita/Desktop/mr_final_project/usersSimilarity")
	for line in sim_data:
		if len(line) > 0 :
			user_id = line.strip('\n').split(" ")[1].split("\t")[0]
			similarity = line.strip('\n').split("\t")[1]
			if user_id not in dict_sim:
				dict_sim.update({user_id: similarity});
	sim_data.close()

	# print(dict_sim)

	dict_graph = {}
	graph_data = open("/home/rachita/Desktop/transitive_matrix")
	for line in graph_data:
		if len(line) > 0:
			user_id = line.strip('\n').split("\t")[1].split(" ")[1]
			distance = line.strip('\n').split("\t")[0]
			if user_id not in dict_graph:
				dict_graph.update({user_id: distance});
	graph_data.close()


	from collections import defaultdict
	review_json = open("/home/rachita/Desktop/mr_final_project/cPath")
	dict_review = defaultdict(list)
	for line in review_json:
		r_data = json.loads(line)
		if "user_id" in r_data and r_data["user_id"] in dict_recommendation:
			if r_data["user_id"] not in [x for v in dict_review for x in v]:
				dict_review[r_data["user_id"]].append(r_data["business_id"])

	# print(dict_review)

	for key in dict_review:
		if key in dict_user and key in dict_sim and key in dict_graph:
			username = dict_user[key]
			print("------------------------------------------------")
			print(username)
			print("Recommendation Score :", dict_recommendation[key])
			print(  "Similarity Score :", dict_sim[key])
			print("Distance from user :", dict_graph[key] )
			print("-------------------------------------------------")
			for v in dict_review[key]:
				if v in dict_business:
					business = dict_business[v]
					print(business)



if __name__ == '__main__':
	# generate_sequence()
	get_recommendation_list()
