import json
import pandas as pd
import requests
import time
import os
from datetime import datetime
from dotenv import load_dotenv

try:
	start_time = time.time()

	# Variable initializations
	status_check = 'pending'
	status_message = ''
	master_columns = ['Full Name', 'About']
	skill_list = [] #Initiatilizing skills list
	job_type_list = [] #Initiatilizing job type list
	duration_list = [] #Initiatilizing duration list
	total_experience_list = [] #Initializing total experience list
	master_dict = {
					'PROFILE ID': [],
					'FULL NAME':[],
					'LINKEDIN URL':[],
					'ABOUT':[],
					'CURRENT JOB TITLE':[],
					'CURRENT COMPANY':[],
					'SKILLS': [],
					'JOB TYPES': [],
					'DURATION WITH CURRENT AND PAST COMPANIES': [],
					'TOTAL EXPERIENCE (in years)': [],
					'FLAG (Denotes R if no experience or too many jumps)': []
				}
	company_experience_dict = {
		'FULL NAME': [],
		'COMPANY NAME': [],
		'EXPERIENCE': []
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
		skill_list = [] #Setting skill list to null for each iteration
		job_type_list = [] #Setting job type list to null for each iteration
		duration_list = [] #Setting duration list to null for each iteration
		total_experience_list = [] #Setting total experience list to null
		all_companies_list = [] #Setting all companies list to null for each iteration
		experience_list = []
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
			all_companies_list.append(item2.get('company'))
			if not item2.get('is_current'):
				try:
					experience_years = item2.get('end_year') - item2.get('start_year')
					experience_months = item2.get('end_month') - item2.get('start_month') + 1
				except ValueError as ve:
					experience_years = 0
					experience_months = 0
				total_experience_historical = str(experience_years) + 'years and' + str(experience_months) + 'months'
				experience_list.append(total_experience_historical)

			elif item2.get('is_current'):
				try:
					today = datetime.today()
					current_year = today.year
					current_month = today.month
					experience_years = current_year - item2.get('start_year')
					experience_months = current_month - item2.get('start_month') + 1
				except ValueError as ve:
					experience_years = 0
					experience_months = 0
				# total_experience_current = str(experience_years) + 'years and' + str(experience_months) + 'months'
				# experience_list.append(total_experience_current)

		master_dict['SKILLS'].append(skill_list)
		master_dict['JOB TYPES'].append(job_type_list)
		master_dict['DURATION WITH CURRENT AND PAST COMPANIES'].append(experience_list)
		#master_dict['TOTAL EXPERIENCE (in years)'].append(sum(total_experience_list))
		master_dict['FLAG (Denotes R if no experience or too many jumps)'].append(flag)
		company_experience_dict['COMPANY NAME'].append(all_companies_list)
		company_experience_dict['EXPERIENCE'].append(experience_list)
		company_experience_dict['FULL NAME'].append(item.get('full_name'))

	df = pd.DataFrame(master_dict)
	print(f'Master Data in Dataframe:\n{df}')
	df2 = pd.DataFrame(company_experience_dict)
	print(f'Data Frame for Company and Experiences\n{df2}')
	#df.to_excel(f'LinkedIn_Dump_{input_role_list}.xlsx')
	df2.to_excel('Company and Experiences.xlsx')
	end_time = time.time()
	runtime = end_time - start_time
	print(f'Program Execution Time: {runtime: .2f} seconds.')
except ValueError as ve:
	print(f'Error encountered:{ve}')
except ConnectionError as ce:
	print(f'Error encountered:{ce}')
except TimeoutError as te:
	print(f'Error encountered:{te}')
except KeyError as ke:
	print(f'Error encountered:{ke}')
except TypeError as tye:
	print(f'Error encountered:{tye}')
except Exception as e:
	print(f'Error encountered:{e}')