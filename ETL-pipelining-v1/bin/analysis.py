import common_modules as common

def main():
	option = 4
	while option:
		print('Please enter a number to select an option:\n1. Top n most viewed page by language.'
			'\n2. Total pages viewed by language'
			'\n3. Total pages viewed by day'
			'\n0. Quit'
			)
		try:
			option = int(input())
		except Exception as error:
			print('Please enter a number')
			continue
		if 0 >= option >= 3:
			print("Enter a valid option")
			continue
		if option == 0:
			print("Thanks for running the application. Have a good day!!")
			break
		elif option == 1:
			print('Enter a number for how many top pages you want to view.')
			n = int(input())
			print('Specify a language code or enter all for results for all languages.')
			language = input()
			common.get_top_n_pages_by_language(n, language)
		elif option == 2:
			print('Specify a language code or enter all for results for all languages.')
			language = input()
			if isinstance(language, str):
				common.get_total_pages_by_language(language)
			else:
				print("Please enter a string")
				continue
		elif option == 3:
			print('Specify a date in this format: YYYY-MM-DD e.g. 2012-01-21 or specify all for all the dates')
			date = input()
			common.get_total_pages_by_date(date)

if __name__ == '__main__':
	main()