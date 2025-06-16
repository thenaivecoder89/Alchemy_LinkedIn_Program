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
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity

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
	end_year = []  # added to manage overlapping experiences - leading to inflated total experience figures
	unique_experience_list = [] #Initializing unique experience list
	company_and_experience_dict = {} #Initializing company and experience dictionary
	final_company_and_experience_dict = {} #Initializing final company and experience dictionary
	input_role_combinations = []
	input_role_list = []
	input_role_list_lower = []
	input_role_list_upper = []
	input_role_list_normal = []
	experience_current_list = [] # added to manage multiple current experiences reported
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
					'EXPERIENCE IN EACH COMPANY': [],
					'TOTAL EXPERIENCE (in years)': [],
					'EXPERIENCE SCORE': [],
					'SKILLS SCORE': [],
					'MATCH SCORE (total of experience and skills scores - higher is better)': []
				}
	load_dotenv()
	api_key = os.getenv('API_KEY')
	api_host = "fresh-linkedin-profile-data.p.rapidapi.com"

	#User inputs
	input_role = input('Enter search roles (separate with commas if searching for more than one role): ')
	input_function = input('Select Function: ')
	input_search_country = input('Enter country for search: ')
	input_required_min_years_of_experience = int(input('Enter minimum years of experience: '))
	input_required_max_years_of_experience = int(input('Enter maximum years of experience: '))
	input_number_of_records = int(input('Enter total number of profiles needed (max. 100): '))
	input_required_skills = input('Enter the list of skills that ideal candidate should have: ')

	#Input alterations for wider search
	for i in range(len(input_role)):
		input_role_combinations.append(input_role[i])

	for i in range(len(input_role_combinations)):
		input_role_list_lower.append(input_role_combinations[i].lower()) #all lower case letters
		input_role_list_upper.append(input_role_combinations[i].upper()) #all upper case letters
		input_role_list_normal.append(input_role_combinations[i]) #all letters as-is
	#Combinations of input role characters for wider search
	input_role_list.append(str(''.join(input_role_list_lower)))
	input_role_list.append(str(''.join(input_role_list_upper)))
	input_role_list.append(str(''.join(input_role_list_normal)))
	input_role_list.append(input_role.title())

	input_function_list = [x.strip() for x in input_function.split(',')]

	# Code to run query
	url = "https://fresh-linkedin-profile-data.p.rapidapi.com/search-employees"
	geo_locations = {
                'Egypt': 106155005,
                'India': 102713980,
                'United States': 103644278,
                'Canada': 101174742,
                'United Kingdom': 101165590,
                'Germany': 101282230,
                'France': 105015875,
                'Australia': 101452733,
                'Brazil': 106057199,
                'Singapore': 102454443,
                'Netherlands': 102890719,
                'UAE': 104305776,
                'Spain': 105646813,
                'Italy': 103350119,
                'Mexico': 103323778,
                'Japan': 101355337,
                'South Korea': 105149562,
                'China': 102890883,
                'Russia': 101728296,
                'Poland': 105072130,
                'Turkey': 105117694,
                'Israel': 101620260,
                'Switzerland': 106693272,
                'Sweden': 105117694,
                'Norway': 103819153,
                'Denmark': 104514075,
                'Finland': 100456013,
                'Belgium': 100565514,
                'Austria': 103883259,
                'Ireland': 104738515,
                'New Zealand': 105490917,
                'South Africa': 105365761,
                'Argentina': 100876405,
                'Chile': 104621616,
                'Colombia': 100876405,
                'Peru': 102890719,
                'Ukraine': 102264497,
                'Czech Republic': 104508036,
                'Portugal': 100364837,
                'Greece': 104677530,
                'Hungary': 100288700,
                'Romania': 106670623,
                'Bulgaria': 105333783,
                'Croatia': 104688944,
                'Serbia': 101855366,
                'Slovakia': 105238872,
                'Slovenia': 106137034,
                'Lithuania': 101464403,
                'Latvia': 104341318,
                'Estonia': 102974008,
                'Thailand': 105072130,
                'Malaysia': 106808692,
                'Philippines': 103121230,
                'Indonesia': 102478259,
                'Vietnam': 104195383,
                'Taiwan': 104187078,
                'Hong Kong': 102890719
            }

	selected_location = geo_locations.get(input_search_country)
	payload = {
		# "current_company_ids": [1002153, 6043316, 1319547, 79676435, 30140054, 14435789], # TO focus on Museums, Heritage Authorities. - Enabled only for AHA. DISABLE AFTER SOURCING
															# Companies shortlisted:
															# DCT (1002153),
															# Dubai Culture and Arts (6043316),
															# Sharjah Museums Authority (1319547)
															# Ministry of Culture - UAE (79676435)
															# DHAKIRA Center for Heritage Studies - UAE (30140054)
															# Louvre Abu Dhabi (14435789)
		"title_keywords": input_role_list,
		"functions": input_function_list,
		"geo_codes": [selected_location],
		# "past_company_ids": [1002153, 6043316, 1319547, 79676435, 30140054, 14435789], # TO focus on Museums, Heritage Authorities. - Enabled only for AHA. DISABLE AFTER SOURCING
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
		clean_skill_list = []
		job_type_list = []
		duration_list = []
		all_companies_list = []
		experience_list = []
		unique_companies_list = []
		unique_experience_list = []
		experience_current_list = [] # added to manage multiple current experiences reported
		end_year = [] # added to manage overlapping experiences - leading to inflated total experience figures
		company_and_experience_dict = {}
		final_company_and_experience_dict = {}


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
					start_date_str = '1/' + str(item2.get('start_month')) + '/' + str(item2.get('start_year'))
					end_date_str = '1/' + str(item2.get('end_month')) + '/' + str(item2.get('end_year'))
					start_date = datetime.strptime(start_date_str, '%d/%m/%Y')
					end_date = datetime.strptime(end_date_str, '%d/%m/%Y')
					delta = end_date - start_date
					experience_historical = round(delta.days / 365, 1)
				except Exception as e:
					experience = 0
				experience_list.append(experience_historical)
				end_year.append(item2.get('end_year'))  # End years of all past experiences appended to end_year list.

			elif item2.get('is_current'):
				try:
					start_date_str = '1/' + str(item2.get('start_month')) + '/' + str(item2.get('start_year'))
					start_date = datetime.strptime(start_date_str, '%d/%m/%Y')
					today = datetime.today()
					delta = today - start_date
					experience_current = round(delta.days / 365, 1)
				except Exception as e:
					experience = 0
				experience_current_list.append(experience_current)

			# added to manage multiple current experiences reported
			try:
				average_current_experience = sum(experience_current_list)/ len(experience_current_list)
			except ZeroDivisionError:
				average_current_experience = 0
			experience_list.append(average_current_experience) #added to manage multiple current experiences reported

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

		for key, value in final_company_and_experience_dict.items():
			if key == 'Career Break':
				pass
			else:
				try:
					numeric_values = [float(v) for v in value]
					unique_experience = round(max(numeric_values), 1) #Calculation of overlapping experiences done based on maximum value.
				except (ZeroDivisionError, ValueError):
					unique_experience = 0
				unique_experience_list.append(unique_experience)

		# Calculating candidate experience score
		# try except block added to handle null values in the end_year list
		try:
			if isinstance(min(end_year), int) or isinstance(min(end_year), float):
				minimum_end_year_value = min(end_year)
			else:
				minimum_end_year_value = int(datetime.today().year)
		except Exception:
			minimum_end_year_value = int(datetime.today().year)
		candidate_total_unique_experience = int(datetime.today().year) - minimum_end_year_value  # added to manage overlapping experiences - leading to inflated total experience figures
		avg_required_years_of_experience = (input_required_min_years_of_experience + input_required_max_years_of_experience) / 2
		if candidate_total_unique_experience < avg_required_years_of_experience:
			candidate_experience_score = 0
		else:
			candidate_experience_score = candidate_total_unique_experience - avg_required_years_of_experience

		#Calculating candidate skill match score
		clean_skill_list = [skill for skill in skill_list if isinstance(skill, str) and skill.strip() != '']
		model = SentenceTransformer('all-MiniLM-L6-v2')
		query_embedding = model.encode([input_required_skills])
		if clean_skill_list:
			skill_texts = [' '.join(clean_skill_list)]  # Combine all skills into one string
			skill_embedding = model.encode(skill_texts)
			similarity_score = cosine_similarity(query_embedding, skill_embedding).flatten()
		else:
			similarity_score = [0.0]  # Or any default score if no skills exist

		if sum(similarity_score) < 0:
			candidate_skills_score = 0
		else:
			candidate_skills_score = sum(similarity_score)

		match_score = candidate_experience_score + candidate_skills_score

		master_dict['SKILLS'].append(skill_list)
		master_dict['JOB TYPES'].append(job_type_list)
		master_dict['ALL COMPANIES THE CANDIDATE HAS WORKED WITH'].append(unique_companies_list)
		master_dict['EXPERIENCE IN EACH COMPANY'].append(unique_experience_list)
		master_dict['TOTAL EXPERIENCE (in years)'].append(candidate_total_unique_experience) # added to manage overlapping experiences - leading to inflated total experience figures
		master_dict['EXPERIENCE SCORE'].append(candidate_experience_score)
		master_dict['SKILLS SCORE'].append(candidate_skills_score)
		master_dict['MATCH SCORE (total of experience and skills scores - higher is better)'].append(match_score)

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