"""
Script that exports to a single xlsx file all the data relevant to be sent to Google Cloud.
"""
import configparser
import datetime

import xlsxwriter

from nau import Reports


def xlsx_worksheet(data, worksheet):
	row = 0
	col = 0
	
	for key in data[0]:
		worksheet.write(row, col, key)
		col += 1
	
	row = + 1
	
	for line in data:
		col = 0
		for value in line.values():
			if isinstance(value, (datetime.datetime, datetime.date, datetime.time, datetime.timedelta)):
				worksheet.write_datetime(row, col, value)
			else:
				worksheet.write(row, col, value)
			col += 1
		row += 1


def xlsx_export_queries(config : configparser.ConfigParser, queries):
	file_name : str = config.get('xlsx', 'file', fallback='report.xlsx')
	default_date_format : str = config.get('xlsx', 'default_date_format', fallback='yyyy-mm-dd')
	progress : bool = config.get('xlsx', 'progress', fallback=True)
	
	workbook = xlsxwriter.Workbook(file_name, {'default_date_format': default_date_format})
	
	for name, query_result in queries:
		if progress: 
			print("Producing... " + name)
		worksheet = workbook.add_worksheet(name)
		xlsx_worksheet(query_result, worksheet)
	
	workbook.close()


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
	
	xlsx_export_queries(config, [
		("Summary", nau_reports.summary()),
		
		# Global - Now
		
		("Organizations", nau_reports.organizations()),
		("Courses", nau_reports.courses()),
		
		# Global - History
		
		("Users", nau_reports.global_enrollment_history()),
		("Certificates", nau_reports.certificates_by_date()),
		
		# Per Course - Now
		
		("Course Metrics", nau_reports.overall_course_metrics()),
		
		# Per Course - History
		
		("Enrollment", nau_reports.student_enrolled_by_course_by_date()),
		("Students Passed", nau_reports.student_passed_by_date()		),
		("Blocks Completed", nau_reports.completed_blocks_by_date()),
		
		# Usage - History
		
		("Last Login by Day", nau_reports.last_login_by_day()),
		("Distinct Users by Day", nau_reports.block_access_distinct_user_per_day()),
		("Distinct Users by Month", nau_reports.block_access_distinct_user_per_month()),
		
		# Final Summary
		
		("Final Summary", nau_reports.final_summary()),
	])


if __name__ == "__main__":
	main()
