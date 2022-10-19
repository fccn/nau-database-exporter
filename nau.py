from datetime import datetime
import mysql.connector
import configparser

class DataLink:
	connection = None
	settings = dict({"host": "localhost", "port": "3306", "user": "", "password": "", "database": "edxapp"})
	
	def __init__(self, config):
		self.settings = config
		self.connect()
	
	def connect(self):
		self.connection = mysql.connector.connect(
			host=self.settings["host"],
			port=self.settings["port"],
			user=self.settings["user"],
			passwd=self.settings["password"],
			database=self.settings["database"]
		)
	
	def close(self):
		self.connection.close()
	
	def query(self, query):  # return a query result set as an list of dicts
		mycursor = self.connection.cursor()
		mycursor.execute(query)
		description = mycursor.description
		
		result = []
		
		for row in mycursor.fetchall():
			r = {}
			for idx, column in enumerate(description):
				r[column[0]] = row[idx]
			result.append(r)
		return result
	
	def get(self, query):  # returns only one value on one line
		mycursor = self.connection.cursor()
		mycursor.execute(query)
		row = mycursor.fetchone()
		return row[0]


class Reports:
	data_link = None
	config : configparser.ConfigParser = None
	progress : bool
	
	def __init__(self, config: configparser.ConfigParser):
		settings : dict = {}
		settings["host"] = config.get('connection', 'host', fallback='localhost')
		settings["port"] = config.get('connection', 'port', fallback='3306')
		settings["database"] = config.get('connection', 'database', fallback='edxapp')
		settings["user"] = config.get('connection', 'user', fallback='edxapp')
		settings["password"] = config.get('connection', 'password')
		
		debug : bool = config.get('connection', 'debug', fallback=False)
		if debug:
			print("Connection Settings: ", settings)
		
		self.data_link = DataLink(settings)
		self.config = config

		self.progress = config.get('sheets', 'progress', fallback=True)

		self.available_data = {
			# Global - Now
			"organizations": { 
				'title': "Organizations", 
				'data': lambda: self.organizations() 
			},
			"course_runs": { 
				'title': "Course runs", 
				'data': lambda: self.course_runs() 
			},
			"course_run_by_date": { 
				'title': "Course run by date", 
				'data': lambda: self.course_run_by_date() 
			},
			"enrollments_with_profile_info": {
				'title': "Enrollments with profile info", 
				'data': lambda: self.enrollments_with_profile_info() 
			},
			"enrollments_year_of_birth": {
				'title': "Enrollments with year of birth",
				'data': lambda: self.enrollments_year_of_birth()
			},
			"enrollments_gender": {
				'title': "Enrollments with gender",
				'data': lambda: self.enrollments_gender()
			},
			"enrollments_level_of_education": {
				'title': "Enrollments with level of education",
				'data': lambda: self.enrollments_level_of_education()
			},
			"enrollments_country": {
				'title': "Enrollments with country",
				'data': lambda: self.enrollments_country()
			},
			"enrollments_employment_situation": {
				'title': "Enrollments with employment situation",
				'data': lambda: self.enrollments_employment_situation()
			},
			"users": { 
				'title': "Users", 
				'data': lambda: self.users()
			},
			"registered_users_by_day": { 
				'title': "Registered users by day", 
				'data': lambda: self.registered_users_by_day()
			},
			"distinct_users_by_day": { 
				'title': "Distinct Users by Day", 
				'data': lambda: self.distinct_users_by_day() 
			},
			"distinct_users_by_month": { 
				'title': "Distinct Users by Month", 
				'data': lambda: self.distinct_users_by_month() 
			},
			"final_summary": { 
				'title': "Final Summary", 
				'data': lambda: self.final_summary() 
			},
		}

	def available_sheets_to_export_keys(self):
		return self.available_data.keys()

	def sheets_data(self, enabled_sheets_keys_list:list):
		# keys = enabled_sheets_keys_list if isinstance(enabled_sheets_keys_list, list) else self.available_data.values()
		filtered_available_data = [value for key, value in self.available_data.items() if key in enabled_sheets_keys_list]
		return list(map(self._apply_data, filtered_available_data))

	def sheets_data_enabled(self):
		enabled_data_keys = self.config.get('sheets', 'enabled', fallback=','.join(self.available_data.keys())).split(',')
		return {k:v for (k,v) in self.available_data.items() if k in enabled_data_keys}

	def _apply_data(self, d:dict):
		title = d.get('title')
		if self.progress:
			print("Producing... " + title)
		return (title, d.get('data')())

	def summary(self):
		return [dict({
			"Version": "v2",
			"DataBase": (self.data_link.settings["host"] + ":" + self.data_link.settings["port"]),
			"Date": datetime.now(),
			"Organizations": self.data_link.get("SELECT count(1) FROM organizations_organization"),
			# Global
			"Courses": self.data_link.get("SELECT count(1) FROM course_overviews_courseoverview"),
			"Users": self.data_link.get("SELECT count(1) FROM auth_user"),
			"Enrollments": self.data_link.get("SELECT count(1) FROM student_courseenrollment"),
			"Certificates": self.data_link.get("SELECT count(1) FROM certificates_generatedcertificate"),
			# 7 Days
			"New Users - 7 days": self.data_link.get("SELECT count(1) FROM auth_user au WHERE au.date_joined > NOW() - INTERVAL 7 DAY"),
			"New Enrollments - 7 days": self.data_link.get("SELECT count(1) FROM student_courseenrollment sce WHERE sce.created > NOW() - INTERVAL 7 DAY"),
			"News Certificates - 7 days": self.data_link.get("SELECT count(1) FROM certificates_generatedcertificate cgc WHERE cgc.created_date > NOW() - INTERVAL 7 DAY"),
			# 15 Days
			"New Users - 15 days": self.data_link.get("SELECT count(1) FROM auth_user au WHERE au.date_joined > NOW() - INTERVAL 15 DAY"),
			"New Enrollments - 15 days": self.data_link.get("SELECT count(1) FROM student_courseenrollment sce WHERE sce.created > NOW() - INTERVAL 15 DAY"),
			"News Certificates - 15 days": self.data_link.get("SELECT count(1) FROM certificates_generatedcertificate cgc WHERE cgc.created_date > NOW() - INTERVAL 15 DAY"),
			# 30 Days
			"New Users - 30 days": self.data_link.get("SELECT count(1) FROM auth_user au WHERE au.date_joined > NOW() - INTERVAL 30 DAY"),
			"New Enrollments - 30 days": self.data_link.get("SELECT count(1) FROM student_courseenrollment sce WHERE sce.created > NOW() - INTERVAL 30 DAY"),
			"News Certificates - 30 days": self.data_link.get("SELECT count(1) FROM certificates_generatedcertificate cgc WHERE cgc.created_date > NOW() - INTERVAL 30 DAY"),
		})]

	def final_summary(self):
		return [dict({
			"Version": "v2",
			"DataBase": (self.data_link.settings["host"] + ":" + self.data_link.settings["port"]),
			"Date": datetime.now(),
		})]
	
	def organizations(self):
		return self.data_link.query("""
			SELECT 
				id, created, modified, name, short_name, description, logo, active 
			FROM organizations_organization
		""")
	
	def course_runs(self):
		"""
		Each line is a course run.
		"""
		return self.data_link.query("""
			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(id, '+', -1) as edition_code,
				(select oo.name from organizations_organization oo WHERE org_code = oo.short_name) as org_name,
				created, modified, id, _location, display_name, 
				start, end, 
				advertised_start, 
				CONCAT('https://lms.nau.edu.pt', course_image_url) as course_image_url, 
				social_sharing_url, 
				certificates_display_behavior, 
				certificates_show_before_end, cert_html_view_enabled, 
				has_any_active_web_certificate, cert_name_short, cert_name_long, 
				lowest_passing_grade, days_early_for_beta, mobile_available, 
				visible_to_staff_only, enrollment_start, 
				enrollment_end, enrollment_domain, invitation_only, 
				max_student_enrollments_allowed, announcement, catalog_visibility, 
				course_video_url, effort, self_paced, 
				certificate_available_date, end_date, start_date,
				COALESCE(enrollment_start, start_date) as enrollment_start_or_course_start,
    			COALESCE(enrollment_end, end_date) as enrollment_end_or_course_end,
				DATEDIFF(COALESCE(enrollment_start, start_date), NOW()) as days_to_enrollment_start,
    			DATEDIFF(start_date, NOW()) as days_to_course_start,
    			DATEDIFF(COALESCE(enrollment_end, end_date), NOW()) as days_to_enrollment_end,
    			DATEDIFF(end_date, NOW()) as days_to_course_end,
				(select count(1) from student_courseenrollment sce WHERE sce.course_id = coc.id) AS enrolled_count,
				(select count(1) from student_courseenrollment sce WHERE sce.course_id = coc.id and sce.is_active) AS enrolled_count_active,
			    (select count(1) from certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificates_count,
			    (select AVG(grade) from certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS average_grade,
				(select count(1) from course_overviews_courseoverview coc2 where course_code=SUBSTRING_INDEX(SUBSTRING_INDEX(coc2.id, '+', -2), '+', 1)) as course_runs_count,
				(select id from course_overviews_courseoverview coc2 where course_code = SUBSTRING_INDEX(SUBSTRING_INDEX(coc2.id, '+', -2), '+', 1) order by created asc limit 1) = id as course_run_is_first_edition,
				(select count(1) from grades_persistentcoursegrade gpcg where coc.id = gpcg.course_id and gpcg.passed_timestamp is not null) as passed
			FROM course_overviews_courseoverview coc
			ORDER BY created ASC
		""")
	
	def course_run_by_date(self):
		return self.data_link.query("""
			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(course_id, '+', -1) as edition_code,
				(select oo.name from organizations_organization oo WHERE org_code = oo.short_name) as org_name,				
				course_id, 
				date, 
				SUM(enrollments_count) as enrollments_count, 
				SUM(passed) as passed,
				SUM(certificates_count) as certificates_count,
				SUM(block_completion_count) as block_completion_count,
				SUM(distinct_users_count) as distinct_users_count
			FROM (
			(
				SELECT 
					course_id, 
					DATE_FORMAT(sce.created, "%Y-%m-%d") date, 
					count(1) as enrollments_count,
					0 as passed,
					0 as certificates_count,
					0 as block_completion_count,
					0 AS distinct_users_count
				FROM student_courseenrollment sce
				GROUP BY course_id, date
			) UNION (
				SELECT 
					course_id, 
					DATE_FORMAT(gpg.passed_timestamp, "%Y-%m-%d") AS date, 
					0 as enrollments_count,
					count(1) as passed,
					0 as certificates_count,
					0 as block_completion_count,
					0 AS distinct_users_count
				FROM grades_persistentcoursegrade gpg
				WHERE gpg.passed_timestamp is not null
				GROUP BY course_id, date
			) UNION (
				SELECT
					course_id,
					DATE_FORMAT(created_date, "%Y-%m-%d") AS date, 
					0 as enrollments_count,
					0 as passed,
					count(1) AS certificates_count,
					0 as block_completion_count,
					0 AS distinct_users_count
				FROM certificates_generatedcertificate
				GROUP BY course_id, date
			) UNION (
				SELECT 
					course_key as course_id,
					date_format(cbc.created, "%Y-%m-%d") as date,
					0 as enrollments_count,
					0 as passed,
					0 AS certificates_count,
					COUNT(1) as block_completion_count,
					0 AS distinct_users_count
				FROM completion_blockcompletion cbc
				GROUP BY course_key, date
			) UNION (
				SELECT 
					course_key as course_id,
					date_format(cbc.created, "%Y-%m-%d") as date,
					0 as enrollments_count,
					0 as passed,
					0 AS certificates_count,
					0 AS block_completion_count,
					COUNT(DISTINCT user_id) as distinct_users_count
				FROM completion_blockcompletion cbc
				GROUP BY course_key, date
			)
			) as t
			GROUP BY course_id, date
			ORDER BY date ASC
			"""
		)

	def enrollments_with_profile_info(self):
		"""
		Enrollment data with student information
		"""
		return self.data_link.query("""
			SELECT
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(sce.course_id, '+', -1) as edition_code,
				(select oo.name from organizations_organization oo WHERE org_code = oo.short_name) as org_name,
				aup.year_of_birth, 
				aup.gender, 
				aup.level_of_education, 
				aup.country,
				sce.course_id, 
				DATE_FORMAT(sce.created, "%Y-%m-%d") AS enrollment_created_date, 
				sce.is_active as enrollment_is_active, 
				sce.mode as enrollment_mode, 
				nuem.employment_situation,
				(select count(1) from student_courseenrollment sce2 where sce2.user_id = sce.user_id) as user_enrollments_count,
				(select count(1) from student_courseenrollment sce2 where sce2.user_id = sce.user_id and SUBSTRING_INDEX(SUBSTRING_INDEX(sce2.course_id, ':', -1), '+', 1) = org_code ) as same_org_enrollments_count,
				(select count(1) from student_courseenrollment sce2 where sce2.user_id = sce.user_id and SUBSTRING_INDEX(SUBSTRING_INDEX(sce2.course_id, ':', -1), '+', 1) != org_code ) = 0 as only_enrollments_this_org,
				(select count(1) from grades_persistentcoursegrade gpcg where sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null) as passed,
				(select gpcg.passed_timestamp from grades_persistentcoursegrade gpcg where sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null) as passed_timestamp,
				(select count(1) from certificates_generatedcertificate cgc where sce.user_id = cgc.user_id and sce.course_id = cgc.course_id) as certificate,
				aup.country in ('AO','BR','CV','GW','GQ','MZ','PT','ST','TL') as cplp
			FROM student_courseenrollment sce
			left join auth_userprofile aup on sce.user_id = aup.user_id
			left join nau_openedx_extensions_nauuserextendedmodel nuem on nuem.user_id = sce.user_id
		""")

	def enrollments_year_of_birth(self):
		"""
		Enrollment data with year of birth
		"""
		return self.data_link.query("""
			SELECT
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(sce.course_id, '+', -1) as edition_code,
				aup.year_of_birth,
				count(gpcg.id) as approved,
				count(1) as enrolled
			FROM student_courseenrollment sce
			left join auth_userprofile aup on sce.user_id = aup.user_id
			left join grades_persistentcoursegrade gpcg on sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null
			GROUP BY org_code, course_code, edition_code, year_of_birth, sce.course_id
		""")

	def enrollments_gender(self):
		"""
		Enrollment data with year of birth
		"""
		return self.data_link.query("""
			SELECT
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(sce.course_id, '+', -1) as edition_code,
				aup.gender,
				count(gpcg.id) as approved,
				count(1) as enrolled
			FROM student_courseenrollment sce
			left join auth_userprofile aup on sce.user_id = aup.user_id
			left join grades_persistentcoursegrade gpcg on sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null
			GROUP BY sce.course_id, gender, sce.course_id
		""")

	def enrollments_level_of_education(self):
		"""
		Enrollment data with year of birth
		"""
		return self.data_link.query("""
			SELECT
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(sce.course_id, '+', -1) as edition_code,
				aup.level_of_education,
				count(gpcg.id) as approved,
				count(1) as enrolled
			FROM student_courseenrollment sce
			left join auth_userprofile aup on sce.user_id = aup.user_id
			left join grades_persistentcoursegrade gpcg on sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null
			GROUP BY sce.course_id, level_of_education, sce.course_id
		""")

	def enrollments_country(self):
		"""
		Enrollment data with year of birth
		"""
		return self.data_link.query("""
			SELECT
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(sce.course_id, '+', -1) as edition_code,
				aup.country,
				aup.country in ('AO','BR','CV','GW','GQ','MZ','PT','ST','TL') as cplp,
				count(gpcg.id) as approved,
				count(1) as enrolled
			FROM student_courseenrollment sce
			left join auth_userprofile aup on sce.user_id = aup.user_id
			left join grades_persistentcoursegrade gpcg on sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null
			GROUP BY sce.course_id, country
		""")

	def enrollments_employment_situation(self):
		"""
		Enrollment data with employment situation
		"""
		return self.data_link.query("""
			SELECT
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(sce.course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(sce.course_id, '+', -1) as edition_code,
				nuem.employment_situation,
				count(gpcg.id) as approved,
				count(1) as enrolled
			FROM student_courseenrollment sce
			left join nau_openedx_extensions_nauuserextendedmodel nuem on nuem.user_id = sce.user_id
			left join grades_persistentcoursegrade gpcg on sce.course_id = gpcg.course_id and sce.user_id = gpcg.user_id and gpcg.passed_timestamp is not null
			GROUP BY sce.course_id, employment_situation
		""")

	def users(self):
		return self.data_link.query("""
			SELECT
				date_format(date_joined, "%Y-%m-%d") as register_date,
				au.is_active,
				aup.year_of_birth, 
				aup.gender, 
				aup.level_of_education, 
				aup.country, 
				nuem.employment_situation,
				(select count(1) from student_courseenrollment sce where sce.user_id = au.id) as enrollment_count
			FROM
				auth_user au
			left join auth_userprofile aup on aup.user_id = au.id
			left join nau_openedx_extensions_nauuserextendedmodel nuem on nuem.user_id = au.id
		""")

	def registered_users_by_day(self):
		return self.data_link.query("""
			SELECT register_date, sum(active) as total_active, sum(total) as total
			FROM 
			(
				(
					SELECT
						date_format(date_joined, "%Y-%m-%d") as register_date,
						0 as active,
						count(1) as total
					FROM
						auth_user au
					GROUP BY date_format(date_joined, "%Y-%m-%d"), au.is_active
				) UNION (
					SELECT
						date_format(date_joined, "%Y-%m-%d") as register_date,
						count(1) as active,
						0 as total
					FROM
						auth_user au
					WHERE is_active=true
					GROUP BY date_format(date_joined, "%Y-%m-%d"), au.is_active
				)
			) as t
			GROUP BY register_date
			ORDER BY register_date ASC
		""")

	def distinct_users_by_day(self):
		"""
		This gives the number of users that have learn by day
		"""
		return self.data_link.query("""
	 		SELECT DATE_FORMAT(created, "%Y-%m-%d") date, COUNT(distinct user_id) as users
			FROM completion_blockcompletion cbc
			GROUP BY date
		""")
	
	def distinct_users_by_month(self):
		"""
		Number of users that have learn on the platform by month
		"""
		return self.data_link.query("""
	 		SELECT DATE_FORMAT(created, "%Y-%m") date, COUNT(distinct user_id) as users
			FROM completion_blockcompletion cbc
			GROUP BY date
		""")
