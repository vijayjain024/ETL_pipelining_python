import pandas as pd
import yaml
import os
from sqlalchemy import create_engine
from sqlalchemy.sql import text
#import threading
#from multiprocessing import pool
import psycopg2
import logging
import datetime
import sys
import numpy as np
import time
import boto3

#Defining global variables
BIN_DIR = os.path.abspath(os.path.dirname(__name__))
PROJ_DIR = os.path.dirname(BIN_DIR)
LOG_DIR = os.path.join(PROJ_DIR, 'log')
PRIV_DIR = os.path.join(PROJ_DIR, 'priv')
OUTPUT_DIR = os.path.join(PROJ_DIR, 'output')
process_variables = {}
db_variables = {}
s3_variables = {}

def set_environment():
	"""Loads the environment file containing the configuration settings."""
	global process_variables
	init_logging()
	logging.info('Starting the run')
	path = os.path.join(PRIV_DIR, "conf.yml")
	logging.info('Fetching the configuration file from {}'.format(path))
	try:
		with open(path, 'r') as ymlfile:
			process_variables = yaml.load(ymlfile)
			if process_variables['load_to_DB'] != 'redshift':
				logging.error("Database type is incorrect in the configuration file. It should be redshift.")
				sys.exit(1)
			logging.info('Successfully loaded the configuration file')
			return process_variables
	except Exception as error:
		logging.error('Could not find the configuration file {}'.format(path))
		logging.exception(error)
		logging.info('Program is terminated.')
		sys.exit(1)

def init_logging():
	"""Creates the log file for a run"""
	now = datetime.datetime.now()
	name = "wikidumps_{0}.log".format(now.strftime('%Y-%m-%d_%H-%M-%S'))
	logging.basicConfig(filename=os.path.join(LOG_DIR, name), level=logging.DEBUG, 
		format='PID:%(process)d - %(asctime)s - %(levelname)s: %(message)s', datefmt='%d-%b-%y %H:%M:%S')

def create_urls():
	"""Generates the download urls based on the conf file in the priv directory"""
	year = process_variables['year']
	month = process_variables['month']
	day = process_variables['day']
	hours = process_variables['hours']
	base_url = process_variables['base_url']	
	file_dict = {'{}-{}-{} {}:00'.format(year,month,day,hour):'{}{}/{}-{}/pagecounts-{}{}{}-{}0000.gz'.format(base_url,year,year,month,year,month,day,hour) for hour in hours}
	for hr,url in file_dict.items():
		#threading.Thread(target=download_file(hr,url)).start()
		logging.info('Parameter values are: year = {}, month = {}, day = {}, hours = {}'.format(process_variables['year'], process_variables['month'], process_variables['day'], process_variables['hours']))
		logging.info('Downloading file: {}'.format(url))
		download_file(hr,url)
	
def download_file(hr,url):
	"""Reads the wiki dumps file in a dataframe"""
	colnames = ['language_type', 'page_name', 'num_views']
	try:
		df = pd.read_csv(url, sep=' ', encoding='latin-1', header=None, usecols=[0,1,2], names=colnames, dtype={'language_type':object, 'page_name':object, 'num_views':np.int32}, chunksize=2000000)
	except Exception as error:
		logging.error("Failed to read the file: {}".format(url))
		logging.exception(error)
	else:
		logging.info("Retrieving the file in chunks")
		for data in df:
			manipulate_data(data, hr)
	
def manipulate_data(data,hr):
	"""Performs data cleaning for the file downloaded"""
	pd.options.mode.chained_assignment = None
	data['language_type'].replace('', np.nan, inplace=True)
	data.dropna(inplace=True)
	try:
		data = data[~data['page_name'].str.lower().str.startswith(('special:', 'user:', 'file:', 'image:', 'template:', 'talk:', '%'))]
		data = data[data['page_name'].map(len) < 255]
		if data.shape[0] == 0:
			return
		else:
			data['time'] = hr	
			data['language'] = data.language_type.str.split(".", n=1, expand=True)[0]
			data.drop(columns =['language_type'], inplace = True)
			logging.info('After cleaning, the chunk file has {} rows.'.format(data.shape[0]))
			#load_to_DB(data)
			write_df_to_csv(data)
	except Exception as error:
		logging.error('Error occured while manupulating the data')
		logging.exception(error)

def write_df_to_csv(data):
	"""Writes the dataframe to a local file in the intermediate directory"""
	intermediate_dir = os.path.join(PROJ_DIR, 'intermediate')
	timestr = time.strftime("%Y%m%d-%H%M%S")
	filename = "wiki_{}".format(timestr)
	filepath = os.path.join(intermediate_dir, filename)
	try:
		data.to_csv(filepath, sep='|', index=False)
	except Exception as error:
		logging.error("Could not write the dataframe to csv")
		logging.exception(error)
		sys.exit(1)
	else:
		upload_to_s3(filepath)

def upload_to_s3(filepath):
	"""Uploads the file from intermediate directory to s3 bucket"""
	s3 = makes3connection()
	filename = os.path.basename(filepath)
	try:
		data_chunk = open(filepath,'rb')
		s3.Bucket(s3_variables['bucket']).put_object(Key=filename, Body=data_chunk)
	except Exception as error:
		logging.error("Upload failed for file: {}".format(filepath))
		logging.exception(error)
	else:
		logging.info("Successfully uploaded: {}".format(filepath))
		load_to_redshift_from_s3(filepath)

def makes3connection ():
	"""Make a connection to AWS s3"""
	global s3_variables
	s3_file = os.path.join(PRIV_DIR, '{}.yml'.format(process_variables['s3']))
	logging.info('Fetching s3 credentials from: {}'.format(s3_file))
	try:
		with open(s3_file, 'r') as ymlfile:
			s3_variables = yaml.load(ymlfile)
	except Exception as error:
		logging.error('Could not load the s3 credentials file: {}'.format(s3_file))
		logging.exception(error)
		logging.info('Program is terminated.')
		sys.exit(1)
	else:
		access_key_id =  s3_variables['aws_access_key_id']
		access_secret_key = s3_variables['aws_secret_access_key']
		s3 = boto3.resource('s3', aws_access_key_id=access_key_id, aws_secret_access_key=access_secret_key)				
	return s3 

def load_to_redshift_from_s3(filepath):
	"""Loads the file from s3 to redshift"""
	conn = make_connection()
	filename = os.path.basename(filepath)
	table_name = db_variables['table_name']
	schema_name = db_variables['schema_name']
	#print(filename)
	load_script = "commit;begin; lock {}.{};copy {}.{} from 's3://{}/{}' credentials 'aws_access_key_id={};aws_secret_access_key={}'\
	IGNOREHEADER 1\
	ACCEPTINVCHARS '^'\
	REMOVEQUOTES\
	DELIMITER '|'\
	maxerror as 100000;end;commit".format(schema_name, table_name, schema_name, table_name, s3_variables['bucket'], filename, s3_variables['aws_access_key_id'], s3_variables['aws_secret_access_key'])
	logging.info(load_script)
	try:
		conn.execute(text(load_script).execution_options(autocommit=True))
	except Exception as error:
		logging.error("Failed to load records to the database")
		logging.exception(error)
	else:
		logging.info("Successfully loaded the records to database.")

def make_connection():
	"""This function is used to make connection to redshift based on the information in the redshift.yml file"""
	global db_variables
	db_type = process_variables['load_to_DB']
	db_file = os.path.join(PRIV_DIR, '{}.yml'.format(db_type))
	logging.info('Fetching database credentials from: {}'.format(db_file))
	try:
		with open(db_file, 'r') as ymlfile:
			db_variables = yaml.load(ymlfile)
	except Exception as error:
		logging.error('Could not load the database credentials file: {}'.format(db_file))
		logging.exception(error)
		logging.info('Program is terminated.')
		sys.exit(1)
	else:
		database_name =  db_variables['database_name']
		host_name = db_variables['host_name']
		user = db_variables['user']
		password = db_variables['password']
		port = db_variables['port']
		logging.info('Connecting to {} database {} on {} with user {}'.format(db_type, database_name, host_name, user))
		#if db_type == 'sqlserver':
		#	connection_string = "mssql+pyodbc://{0}:{1}@{2}:{3}/{4}?driver=ODBC+Driver+13+for+SQL+Server".format(user, password, host_name, port, database_name)	
		if db_type == 'redshift':
			connection_string = "postgresql://{0}:{1}@{2}:{3}/{4}".format(user, password, host_name, port, database_name)
		try:
			conn = create_engine(connection_string)
		except Exception as error:
			logging.error('Could not connect to {} database {} on {} with user {}'.format(db_type, database_name, host_name, user))
			logging.exception(error)
			logging.info('Program is terminated.')
			sys.exit(1)
		else:
			logging.info("Successfully connected to the database")
			return conn

def get_top_n_pages_by_language(n, language):
	"""Returns a csv with results for top n pages by languages"""
	set_environment()
	conn = make_connection()
	table_name = db_variables['table_name']
	schema_name = db_variables['schema_name']
	if language == 'all':
		query = "end;\ncommit;\nwith cte as(select language, page_name, sum(num_views) as sn from {}.{} group by language, page_name),\
		cte2 as(select language, page_name, sn, dense_rank() over(partition by language order by sn desc) as rn from cte)\
		select language, page_name, sn as number_of_views, rn as rank_num from cte2 where rn <={};".format(schema_name, table_name, n)
	else:
		query = "end;\ncommit;\nwith cte as(select language, page_name, sum(num_views) as sn from {}.{} where language = '{}' group by language, page_name),\
		cte2 as(select language, page_name, sn, dense_rank() over(partition by language order by sn desc) as rn from cte)\
		select language, page_name, sn as number_of_views, rn as rank_num from cte2 where rn <={};".format(schema_name, table_name, language, n)
	#print(query)
	try:
		df = pd.read_sql(query, conn)
	except Exception as error:
	#	print(error)
		print("Something went wrong. Please execute end and commit statements in the database and then try again.")
	else:
		filename = "top_n_pages_by_language.csv"
		filepath = os.path.join(OUTPUT_DIR, filename)
		df.to_csv(filepath, sep='|')
		print("Please check the output directory for the output")
	finally:
		conn.dispose()		
		
def get_total_pages_by_language(language):
	"""Returns the csv with sum of views by languages"""
	set_environment()
	conn = make_connection()
	table_name = db_variables['table_name']
	schema_name = db_variables['schema_name']
	if language == 'all':
		query = "end;\ncommit;\nselect language, sum(num_views) from {}.{} group by language;".format(schema_name, table_name)
	else:
		query = "end;\ncommit;\nselect language, sum(num_views) from {}.{} where language = '{}' group by language;".format(schema_name,table_name,language)
	try:
		df = pd.read_sql(query, conn)
	except Exception as error:
	#	print(error)
		print("Something went wrong. Please execute end and commit statements in the database and then try again.")
	else:
		filename = "total_pages_by_language.csv"
		filepath = os.path.join(OUTPUT_DIR, filename)
		df.to_csv(filepath, sep='|')
		print("Please check the output directory for the output")
	finally:
		conn.dispose()

def get_total_pages_by_date(dt):
	"""Returns the csv with sum of views by date"""
	set_environment()
	conn = make_connection()
	schema_name = db_variables['schema_name']
	table_name = db_variables['table_name']
	if dt == 'all':
		query = 'end;\ncommit;\nselect split_part("time",\' \',1), sum(num_views) from {}.{} group by split_part("time",\' \',1)'.format(schema_name, table_name)
	else:
		query = 'end;\ncommit;\nselect split_part("time",\' \',1), sum(num_views) from {}.{} where time like \'{}%%\' group by split_part("time",\' \',1)'.format(schema_name, table_name, dt)
	#print(query)
	try:
		df = pd.read_sql(query, conn)
	except Exception as error:
	#	print(error)
		print("Something went wrong")
	else:
		filename = "total_pages_by_date.csv"
		filepath = os.path.join(OUTPUT_DIR, filename)
		print("Please check the output directory for the output")
		df.to_csv(filepath, sep='|')
	finally:
		conn.dispose()	

