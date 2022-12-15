"""
Script that exports the queries to multiple csv files.
"""
from array import array
import configparser
import gspread
import datetime
from gspread.spreadsheet import Spreadsheet
from gspread.exceptions import WorksheetNotFound
from gspread.worksheet import Worksheet
from gspread.utils import ValueInputOption

from nau import Reports

def transform_value(value):
	if isinstance(value, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
		return value.strftime("%Y-%m-%d") #  %H:%M:%S
	elif value is None:
		return ""
	else:
		# convert Decimal to str
		return str(value)

def transform_values(values : array):
	new_values = []
	for value in values:
		new_value = transform_value(value)
		new_values.append(new_value)
	return new_values

def write_data(data: dict, worksheet: Worksheet):
	"""
	Write the result of the SQL query to a Google Sheet worksheet
	"""
	alter_data = []

	# append header
	alter_data.append(transform_values(data[0].keys()))
	
	for line in data:
		new_line = transform_values(line.values())
		alter_data.append(new_line)
	
	worksheet.update(alter_data, value_input_option=ValueInputOption.user_entered)


def export_queries_to_google(config : configparser.ConfigParser, report:Reports):
	"""
	Export the spread sheet information to Google Sheets.
	Each table can be exported to a different Google Sheet file.
	"""

	credentials_list_tuples = config.items(section='google_service_account')
	credentials_dict = dict(credentials_list_tuples)
	gc = gspread.service_account_from_dict(credentials_dict)
	
	for sheet_key, worksheet_id in config.items('google_sheets'):
		spreadsheet : Spreadsheet = gc.open_by_key(worksheet_id)		
		sheets_results = report.sheets_data([sheet_key])
		for sheet_title, sheet_result in sheets_results:
			# Get existing worksheet or create a new one
			worksheet : Worksheet
			try:
				worksheet = spreadsheet.worksheet(sheet_title)
			except WorksheetNotFound:
				rows_count = len(sheet_result)
				column_count = len(sheet_result[0]) if sheet_result[0] else 1
				worksheet = spreadsheet.add_worksheet(sheet_title, rows_count, column_count)

		write_data(sheet_result, worksheet)
	
	# Close connection to Google Cloud
	gc.session.close()
	
def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	nau_reports = Reports(False, config)
	export_queries_to_google(config, nau_reports)


if __name__ == "__main__":
	main()
