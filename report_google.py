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


def export_queries_to_google(config : configparser.ConfigParser, queries):

	credentials_list_tuples = config.items(section='google_service_account')
	credentials_dict = dict(credentials_list_tuples)
	gc = gspread.service_account_from_dict(credentials_dict)

	worksheet_id = config.get('google_sheets', 'worksheet_id')
	spreadsheet : Spreadsheet = gc.open_by_key(worksheet_id)
	
	progress : bool = config.get('google_sheets', 'progress', fallback=True)
	
	for name, query_result in queries:
		if progress: 
			print("Producing... " + name)
		
		# Get existing worksheet or create a new one
		worksheet : Worksheet
		try:
			worksheet = spreadsheet.worksheet(name)
		except WorksheetNotFound:
			rows_count = len(query_result)
			column_count = len(query_result[0]) if query_result[0] else 1
			worksheet = spreadsheet.add_worksheet(name, rows_count, column_count)

		write_data(query_result, worksheet)
	
	# Close connection to Google Cloud
	gc.session.close()
	
def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	
	nau_connection_settings : dict = {}
	nau_connection_settings["host"] = config.get('connection', 'host', fallback='localhost')
	nau_connection_settings["port"] = config.get('connection', 'port', fallback='3306')
	nau_connection_settings["database"] = config.get('connection', 'database', fallback='edxapp')
	nau_connection_settings["user"] = config.get('connection', 'user', fallback='edxapp')
	nau_connection_settings["password"] = config.get('connection', 'password')
	
	debug : bool = config.get('connection', 'debug', fallback=False)
	if debug:
		print("Connection Settings: ", nau_connection_settings)
	
	nau_reports = Reports(nau_connection_settings)
	
	export_queries_to_google(config, [
		# ("Summary", nau_reports.summary()),
		
		# Global - Now
		
		("Organizations", nau_reports.organizations()),
		("Courses", nau_reports.courses()),
		
		# Global - History
		
		# ("Users", nau_reports.global_enrollment_history()),
		# ("Certificates", nau_reports.certificates_by_date()),
		
		# # Per Course - Now
		
		# ("Course Metrics", nau_reports.overall_course_metrics()),
		
		# # Per Course - History
		
		# ("Enrollment", nau_reports.student_enrolled_by_course_by_date()),
		# ("Students Passed", nau_reports.student_passed_by_date()		),
		# ("Blocks Completed", nau_reports.completed_blocks_by_date()),
		
		# # Usage - History
		
		# ("Last Login by Day", nau_reports.last_login_by_day()),
		# ("Distinct Users by Day", nau_reports.block_access_distinct_user_per_day()),
		# ("Distinct Users by Month", nau_reports.block_access_distinct_user_per_month()),
		
		# # Final Summary
		
		# ("Final Summary", nau_reports.final_summary()),
	])


if __name__ == "__main__":
	main()
