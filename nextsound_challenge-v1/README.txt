##Author: Vijay Jain
##For: NextSound Challenge
##Description: Building a scalable ETL pipeline to load wikilogs into a database system and query the database to analyze dataset

Folder Description:
bin - contains 3 files. 1. common_modules.py 2. initialize.py 3. analysis.py
	common_modules.py - contains the main code for the application.
	initialize.py - this script extracts, transforms and loads the data by calling functions in common_modules.py
	analysis.py - this script provides the options to analyze the data. It requires input parameters from user

intermediate - this directory is used to store the intermediate file generated during the process

log - contains the log file for every run of initialize.py

output - contains the output files as a result of running analysis.py

priv - contains the config file. These files needs to be setup before running any bin scripts

tests - contains the test script and data

######## HOW TO RUN ######
This code needs an AWS account. Also install the packages mentioned in packages_required file.

Step 1: Create a Redshift cluster and setup a bucket in s3
Step 2: Create a schema and a table in Redshift. Note the schema name and table name needs to be updated in the redshift.yml file. DDL for the table is:
	Create schema_name.table_name(
	page_name varchar,
	num_views  int,
	time varchar,
	language varchar
	)

Step 2: Edit the files in the priv directory.
	conf.yml - Provide the dates for which data needs to be loaded. Year, month, day are single valued. hours parameter can accept a list of hours
	to load any arbitary hours. It can be left unchanged. By default it will load data for 2012-01-01 00:00.
	redshift.yml - provide the connection details for redshift. All parameters are single valued. Provide the table name and schema name used in step 2.
	s3_logon.yml - provide the connection details for s3 bucket.
Step 3: Run the initialize.py file in the bin directory. This will load the data in redshift
Ste 4: Run the analysis.py file in the bin directory. This will provide options to select from and output the data in the intermediate directory.

In case of any issues, please feel free to contact at vijayjain024@gmail.com.

Thanks.

#### TESTING ####
Since there is lot of dependency between the functions and most functions do not return a value, we'll take a different approach than the conventional testing.

Step 1 : Run the ddls file in the test directory
Step 2 : Update the redshift.yml file in the bin directory with the table name used in the step1
Step 3 : Run the analysis.py file
Step 4 : Select option 1. Pass 1, en as the arguments. The function should write a file to output directory with filename top_n_pages_by_language.csv.
	 It should have a row with following with following values: en nn 12
Step 5 : Select option 2. Pass en as the argument. The function should write a file to output directory with filename total_pages_by_language.csv.
	 It should have a row with following with following values: en 91
Step 6 : Select option 3. Pass 2012-01-01 as the argument. The function should write a file to output directory with filename total_pages_by_date.csv.
	 It should have a row with following with following values: 2012-01-01 132


 





