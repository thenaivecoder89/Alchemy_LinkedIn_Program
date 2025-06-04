import json
import traceback
from collections import defaultdict
import pandas as pd
import requests
import time
import os
from datetime import datetime
from dotenv import load_dotenv
import re

try:
	start_time = time.time()

	# Variable initializations
	status_check = 'pending'
	status_message = ''
	experience_historical = 0
	experience_current = 0
	master_columns = ['Full Name', 'About']
	skill_list = [] #Initiatilizing skills list
	job_type_list = [] #Initiatilizing job type list
	duration_list = [] #Initiatilizing duration list
	total_experience_list = [] #Initializing total experience list
	unique_experience_list = [] #Initializing unique experience list
	company_and_experience_dict = {} #Initializing company and experience dictionary
	master_dict = {
					'PROFILE ID': [],
					'FULL NAME':[],
					'LINKEDIN URL':[],
					'ABOUT':[],
					'CURRENT JOB TITLE':[],
					'CURRENT COMPANY':[],
					'SKILLS': [],
					'JOB TYPES': [],
					'ALL COMPANIES THE CANDIDATE HAS WORKED WITH': [],
					'TOTAL EXPERIENCE (in years)': [],
					'FLAG (Denotes R if no experience or too many jumps)': []
				}
	load_dotenv()
	api_key = os.getenv('API_KEY')
	api_host = "fresh-linkedin-profile-data.p.rapidapi.com"

	#User inputs
	input_role = 'AI Engineer' #input('Enter search roles (separate with commas if searching for more than one role): ')
	input_function = 'Consulting' #input('Select Function: ')
	input_role_list = [x.strip() for x in input_role.split(',')]
	input_function_list = [x.strip() for x in input_function.split(',')]
	input_search_country = 'Egypt' #input('Enter country for search: ')
	input_number_of_records = 5 #int(input('Enter total number of profiles needed (max. 100): '))

	# Code to run query
	url = "https://fresh-linkedin-profile-data.p.rapidapi.com/search-employees"
	geo_locations = {
		'Egypt': 106155005,
		'India': 102713980
	}
	selected_location = geo_locations.get(input_search_country)
	payload = {
		#"current_company_ids": [162479, 1053],
		"title_keywords": input_role_list,
		"functions": input_function_list,
		"geo_codes": [selected_location],
		"limit": input_number_of_records
	}
	headers = {
		"x-rapidapi-key": api_key,
		"x-rapidapi-host": api_host,
		"Content-Type": "application/json"
	}
	response = requests.post(url, json=payload, headers=headers)
	message = response.json()
	req_id = message.get('request_id')

	# Code to check search status
	url = "https://fresh-linkedin-profile-data.p.rapidapi.com/check-search-status"

	while status_check == 'pending' or status_check == 'processing':
		querystring = {"request_id":req_id}

		headers = {
			"x-rapidapi-key": api_key,
			"x-rapidapi-host": api_host
		}
		response_check = requests.get(url, headers=headers, params=querystring)
		message_status_check = response_check.json()
		status_message = message_status_check.get('message')
		index = status_message.find("Please")
		status_check = message_status_check.get('status')
		print(f'Current Status of Request: {status_check}. Message: {' '.join(status_message[:index].split())}')

	# Code to get search results
	url = "https://fresh-linkedin-profile-data.p.rapidapi.com/get-search-results"
	querystring = {"request_id":req_id, "page":"1"}
	headers = {
		"x-rapidapi-key": api_key,
		"x-rapidapi-host": api_host
	}
	response_data = requests.get(url, headers=headers, params=querystring)
	master_response = response_data.json()
	master_data = master_response.get('data')
	parsed_master_data = json.dumps(master_data, indent=4)
	print(f'Parsed Master Data:\n{parsed_master_data}')

	for item in master_data:
		#Setting all lists and dictionaries to null for each iteration
		skill_list = []
		job_type_list = []
		duration_list = []
		total_experience_list = []
		all_companies_list = []
		experience_list = []
		unique_companies_list = []
		unique_experience_list = []
		company_and_experience_dict = {}

		flag = '' #Setting flag to null for each iteration

		master_dict['PROFILE ID'].append(item.get('profile_id'))
		master_dict['FULL NAME'].append(item.get('full_name'))
		master_dict['LINKEDIN URL'].append(item.get('linkedin_url'))
		master_dict['ABOUT'].append(item.get('about'))
		master_dict['CURRENT JOB TITLE'].append(item.get('job_title'))
		master_dict['CURRENT COMPANY'].append(item.get('company'))

		for item2 in item.get('experiences'):
			skill_list.append(item2.get('skills'))
			job_type_list.append(item2.get('job_type'))
			duration_list.append(item2.get('duration'))
			all_companies_list = item2.get('company')

			if not item2.get('is_current'):
				try:
					start_date_str = '1/' + str(item2.get('start_month')) + '/' + str(item2.get('start_year'))
					end_date_str = '1/' + str(item2.get('end_month')) + '/' + str(item2.get('end_year'))
					start_date = datetime.strptime(start_date_str, '%d/%m/%Y')
					end_date = datetime.strptime(end_date_str, '%d/%m/%Y')
					delta = end_date - start_date
					experience_historical = round(delta.days / 365, 1)
				except Exception as e:
					experience = 0
				experience_list.append(experience_historical)

			elif item2.get('is_current'):
				try:
					start_date_str = '1/' + str(item2.get('start_month')) + '/' + str(item2.get('start_year'))
					start_date = datetime.strptime(start_date_str, '%d/%m/%Y')
					today = datetime.today()
					delta = today - start_date
					experience_current = round(delta.days / 365, 1)
				except Exception as e:
					experience = 0
				experience_list.append(experience_current)

		for item in all_companies_list:
			if item in unique_companies_list or item == 'Career Break':
				pass
			else:
				unique_companies_list.append(item)

		company_and_experience_dict = {
			'COMPANY': all_companies_list,
			'EXPERIENCE': experience_list
		}

		grouped_dict = defaultdict(list)

		for company, exp in zip(company_and_experience_dict['COMPANY'], company_and_experience_dict['EXPERIENCE']):
			grouped_dict[company].append(exp)

		final_company_and_experience_dict = dict(grouped_dict)

		for key, value in company_and_experience_dict.items():
			if key == 'Career Break':
				pass
			else:
				try:
					numeric_values = [float(v) for v in value]
					unique_experience = round(sum(numeric_values)/len(numeric_values), 1)
				except (ZeroDivisionError, ValueError):
					unique_experience = 0
				unique_experience_list.append(unique_experience)


		master_dict['SKILLS'].append(skill_list)
		master_dict['JOB TYPES'].append(job_type_list)
		master_dict['ALL COMPANIES THE CANDIDATE HAS WORKED WITH'].append(unique_companies_list)
		master_dict['TOTAL EXPERIENCE (in years)'].append(sum(unique_experience_list))
		master_dict['FLAG (Denotes R if no experience or too many jumps)'].append(flag)

	df = pd.DataFrame(master_dict)
	print(f'Master Data in Dataframe:\n{df}')
	df.to_excel(f'LinkedIn_Dump_{input_role_list}.xlsx')
	end_time = time.time()
	runtime = end_time - start_time
	print(f'Program Execution Time: {runtime: .2f} seconds.')
except (ValueError, ConnectionError, TimeoutError, KeyError, TypeError) as detected:
	match = re.search(r'line (\d+)', traceback.format_exc())
	if match:
		print(f'Error encountered:{detected} in line number: {match.group(1)}.')
	else:
		print(f'Error encountered:{detected} and line not found.')
except Exception as e:
	match = re.search(r'line (\d+)', traceback.format_exc())
	if match:
		print(f'Error encountered:{e} in line number: {match.group(1)}.')
	else:
		print(f'Error encountered:{e} and line not found.')