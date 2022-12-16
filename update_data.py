"""
Script that exports to a single xlsx file all the data relevant to be sent to Google Cloud.
"""
import configparser

from nau import Reports


def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	report:Reports = Reports(True, config)
	to_sync_keys = config.get('data', 'synchronize', fallback=','.join(report.available_sheets_to_export_keys())).split(',')
	
	for key in to_sync_keys:
		report.sheets_data([key])
	

if __name__ == "__main__":
	main()
