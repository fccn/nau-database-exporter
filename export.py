"""
Script that exports data to xlsx or to a Google Sheet.
"""
import argparse
import configparser

from nau import Reports


if __name__ == "__main__":
	parser = argparse.ArgumentParser(
        prog='NAU Open edX Database exporter',
        description='Exports to xlsx or to Google Sheet with Open edX DB',
		epilog='This program exports to a xlsx file or directly to a Google Sheet information from the Open edX database, so it can be analyze or integrated with dashboard application.',
	)
	parser.add_argument('--config', type=argparse.FileType('r'), required=True, help='The path to a config.ini with the required configurations.')
	parser.add_argument('--export', required=True, choices=['xlsx','google_sheets'], help='The export mode selected.')
	args = parser.parse_args()

	config_file = args.config
	config_file_content = config_file.read()

	config = configparser.ConfigParser()
	config.read_string(config_file_content)
	reports:Reports = Reports(config)
	export_mode = args.export

	match export_mode:
		case 'xlsx':
			from report_xlsx import export_to_xlsx
			export_to_xlsx(config, reports)
		case 'google_sheets':
			from report_google import export_queries_to_google
			export_queries_to_google(config, reports)
		case _:
			raise ValueError(f"Invalid export mode selected {export_mode}")
