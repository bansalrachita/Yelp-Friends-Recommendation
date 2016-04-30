import calendar
import matplotlib.pyplot as plt
from matplotlib import rc, rcParams
import numpy as np
import random
import json
import csv

def generate_sequence():
	dict_usr = {}
	r_json = open("/home/rachita/Desktop/mr_final_project/yelp_academic_dataset_review.json")
	for line in r_json:
		if len(line) > 0:
			data_r = json.loads(line)
			if data_r["user_id"] in dict_usr:
				dict_usr[data_r["user_id"]] = dict_usr[data_r["user_id"]] + 1
			else:
				dict_usr.update({data_r["user_id"] : 1})
	r_json.close()
	print(dict_usr)
	with open('/home/rachita/Desktop/users.csv', 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=['Users','Reviews'])
		writer.writeheader()
		for key, value in dict_usr.items():
			writer.writerow({'Users': key, 'Reviews': value})
			# writer.writerow({'Reviews': value})

def generate_business_sequence():
	dict_business = {}
	r_json = open("/home/rachita/Desktop/mr_final_project/yelp_academic_dataset_review.json")
	for line in r_json:
		if len(line) > 0:
			data_r = json.loads(line)
			if data_r["business_id"] in dict_business:
				dict_business[data_r["business_id"]] = dict_business[data_r["business_id"]] + 1
			else:
				dict_business.update({data_r["business_id"] : 1})
	r_json.close()
	print(dict_business)
	with open('/home/rachita/Desktop/mr_final_project/business.csv', 'w') as csvfile:
		writer = csv.DictWriter(csvfile, fieldnames=['Business','Reviews'])
		writer.writeheader()
		for key, value in dict_business.items():
			writer.writerow({'Business': key, 'Reviews': value})
			# writer.writerow({'Reviews': value})


if __name__ == '__main__':
	# generate_sequence()
	get_recommendation_list()

