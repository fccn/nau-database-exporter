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


def xlsx_export_queries(config : configparser.ConfigParser, report:Reports):

	file_name : str = config.get('xlsx', 'file', fallback='report.xlsx')
	default_date_format : str = config.get('xlsx', 'default_date_format', fallback='yyyy-mm-dd')
	workbook = xlsxwriter.Workbook(file_name, {'default_date_format': default_date_format})

	sheets_to_export_keys = config.get('xlsx', 'export', fallback=','.join(report.available_sheets_to_export_keys())).split(',')
	
	for sheet_key in sheets_to_export_keys:
		sheets_results = report.sheets_data([sheet_key])
		for sheet_title, sheet_result in sheets_results:
			worksheet = workbook.add_worksheet(sheet_title)
			xlsx_worksheet(sheet_result, worksheet)
	
	workbook.close()


def main():
	config = configparser.ConfigParser()
	config.read('config.ini')
	nau_reports = Reports(config)
	xlsx_export_queries(config, nau_reports)


if __name__ == "__main__":
	main()
