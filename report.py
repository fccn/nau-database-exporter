import configparser
import xlsxwriter

from nau import Reports

debug = True
nau_connection_settings = dict({"host":"localhost", "port":"3308", "user":"ruiribeiro", "password":"dr3Suqaceswl",
							   "database":"edxapp"})


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
			worksheet.write(row, col, value)
			col += 1
		row += 1


def xlsx_export_queries(queries, file_name):
	workbook = xlsxwriter.Workbook(file_name)
	
	for name, query_result in queries:
		if debug: print("Producing... " + name)
		worksheet = workbook.add_worksheet(name)
		xlsx_worksheet(query_result, worksheet)
	
	workbook.close()


def main():
	
	global debug
	global nau_connection_settings
	
	config = configparser.ConfigParser()
	config.read('config.ini')
	
	debug = config.get('general', 'debug')
	nau_connection_settings["host"] = config.get('connection', 'host')
	nau_connection_settings["port"] = config.get('connection', 'port')
	nau_connection_settings["database"] = config.get('connection', 'database')
	nau_connection_settings["user"] = config.get('connection', 'user')
	nau_connection_settings["password"] = config.get('connection', 'password')
	
	nau_reports = Reports(nau_connection_settings)
	
	xlsx_export_queries([
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
		("Blocks Completed", nau_reports.completed_blocks_by_date())
		
	],
	config.get('output', 'file'))


if __name__ == "__main__":
	main()
