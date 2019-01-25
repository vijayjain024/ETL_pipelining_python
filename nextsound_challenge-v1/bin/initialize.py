import common_modules as common
import pandas as pd
import os
import logging



def main():
	process_variables = common.set_environment()
	common.create_urls()
	#common.get_top_n_pages_by_language()
	
if __name__ == '__main__':
    main()

