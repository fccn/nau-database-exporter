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
	
	def sheets_data(self):
		available_data = {
			"summary": { 
				'title': "Summary", 
				'data': lambda: self.summary() 
			},
			
			# Global - Now
			"organizations": { 
				'title': "Organizations", 
				'data': lambda: self.organizations() 
			},
			"courses": { 
				'title': "Courses", 
				'data': lambda: self.courses() 
			},
			
			# Global - History
			"users": { 
				'title': "Users", 
				'data': lambda: self.global_enrollment_history() 
			},
			"certificates": { 
				'title': "Certificates", 
				'data': lambda: self.certificates_by_date() 
			},

			# Per Course - Now
			"course_metrics": { 
				'title': "Course Metrics", 
				'data': lambda: self.overall_course_metrics() 
			},

			# Per Course - History
			"enrollment": { 
				'title': "Enrollment", 
				'data': lambda: self.student_enrolled_by_course_by_date() 
			},
			"students_passed": { 
				'title': "Students Passed", 
				'data': lambda: self.student_passed_by_date() 
			},
			"blocks_completed": { 
				'title': "Blocks Completed", 
				'data': lambda: self.completed_blocks_by_date() 
			},

			# Usage - History
			"last_login_by_day": { 
				'title': "Last Login by Day", 
				'data': lambda: self.last_login_by_day() 
			},
			"distinct_users_by_day": { 
				'title': "Distinct Users by Day", 
				'data': lambda: self.block_access_distinct_user_per_day() 
			},
			"distinct_users_by_month": { 
				'title': "Distinct Users by Month", 
				'data': lambda: self.block_access_distinct_user_per_month() 
			},
			
			# Final Summary
			"final_summary": { 
				'title': "Final Summary", 
				'data': lambda: self.final_summary() 
			},
		}
		
		enabled_data_keys = self.config.get('sheets', 'enabled', fallback=','.join(available_data.keys())).split(',')
		enabled_data = {k:v for (k,v) in available_data.items() if k in enabled_data_keys}
		
		return list(map(lambda d: (d.get('title'), d.get('data')()), enabled_data.values()))


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
		# TODO: replace all the column names with just sufficient columns
		return self.data_link.query("SELECT id, created, modified, name, short_name, description, logo, active FROM organizations_organization")
	
	def courses(self):
		# TODO: replace all the column names with just sufficient columns
		return self.data_link.query("""
			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(id, '+', -1) as edition_code,
				created, modified, version, id, _location, display_name, display_number_with_default, display_org_with_default, start, end, advertised_start, course_image_url, social_sharing_url, end_of_course_survey_url, certificates_display_behavior, certificates_show_before_end, cert_html_view_enabled, has_any_active_web_certificate, cert_name_short, cert_name_long, lowest_passing_grade, days_early_for_beta, mobile_available, visible_to_staff_only, _pre_requisite_courses_json, enrollment_start, enrollment_end, enrollment_domain, invitation_only, max_student_enrollments_allowed, announcement, catalog_visibility, course_video_url, effort, short_description, org, self_paced, marketing_url, eligible_for_financial_aid, language, certificate_available_date, end_date, start_date FROM course_overviews_courseoverview
		""")
	
	def overall_course_metrics(self):
		return self.data_link.query("""
			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(coc.id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(coc.id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(coc.id, '+', -1) as edition_code,
				coc.id,
				coc.display_name,
				coc.display_org_with_default,
			    (select count(1) from student_courseenrollment sce WHERE sce.course_id = coc.id) AS enrolled,
			    (select count(1) from certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificates,
			    (select AVG(grade) from certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS average_grade
			FROM course_overviews_courseoverview coc
		""")
	
	def certificates_by_date(self):
		return self.data_link.query("""
			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(course_id, '+', -1) as edition_code,
				course_id, 
				DATE_FORMAT(created_date, "%Y-%m-%d") AS date, 
				count(1) AS cnt
			FROM certificates_generatedcertificate
			GROUP BY course_id, date
			"""
		)
	
	def current_enrollment_distribution(self):
		return self.data_link.query("""
			SELECT
				sce.user_id as user, count(1) as cnt
			FROM
				auth_user au,
				student_courseenrollment sce
			WHERE
				au.is_staff = 0
			AND
				au.id = sce.user_id
			GROUP BY
				sce.user_id
		  """)
	
	def global_enrollment_history(self):
		response = self.data_link.query("""
			SELECT
				date_format(date_joined, "%Y-%m-%d") as register_date,
				count(1) AS cnt,
				SUM(is_active) AS active
			FROM
				auth_user au
			GROUP BY register_date
			""")
		
		cnt_registered = 0
		cnt_active = 0
		for line in response:
			cnt_registered += line["cnt"]
			cnt_active += line["active"]
			line["sum_cnt"] = cnt_registered
			line["sum_active"] = cnt_active
		
		return response

	def last_login_by_day(self):
		return self.data_link.query("""
			SELECT
				date_format(last_login, "%Y-%m-%d") as last_login_date,
				count(1) AS last_logins
			FROM
				auth_user au
			GROUP BY last_login_date
			""")

	def studentmodule_history(self):
		return self.data_link.query("""
   			SELECT DATE_FORMAT(csm.created, "%Y-%m-%d") date, COUNT(csm.id) cnt
			FROM courseware_studentmodule csm
			GROUP BY date
		""")
		
	def courseenrollment_allowed(self):
		return self.data_link.query("""
   			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(course_id, '+', -1) as edition_code,
			    course_id,
			    count(1) as students
			FROM student_courseenrollmentallowed scea
			GROUP BY course_id
		""")
		
	def student_enrolled_by_course_by_date(self): #
		return self.data_link.query("""
   			SELECT 
			    SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(course_id, '+', -1) as edition_code,
				course_id, 
				DATE_FORMAT(sce.created, "%Y-%m-%d") date, 
				count(sce.user_id) as students
			FROM student_courseenrollment sce
			GROUP BY course_id, date
		""")

	def student_passed_by_date(self): #
		return self.data_link.query("""
   			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_id, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(course_id, '+', -1) as edition_code,
			    course_id, 
				date, 
				COUNT(user_id) AS test, 
				SUM(passou) AS pass
			FROM (
				SELECT course_id, DATE_FORMAT(gpg.course_edited_timestamp, "%Y-%m-%d") AS date, user_id, if(gpg.percent_grade > 0,1,0) AS passou
				FROM grades_persistentcoursegrade gpg
		    ) AS a
			GROUP BY course_id, date
		""")

	def completed_blocks_by_date(self): #
		return self.data_link.query("""
   			SELECT 
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_key, ':', -1), '+', 1) as org_code,
				SUBSTRING_INDEX(SUBSTRING_INDEX(course_key, '+', -2), '+', 1) as course_code,
				SUBSTRING_INDEX(course_key, '+', -1) as edition_code,
			    course_key,
				block_type,
				date_format(cbc.created, "%Y-%m-%d") as date,
				COUNT(user_id) as users
			FROM completion_blockcompletion cbc
			GROUP BY course_key, block_type, date
		""")
		
	def block_access_distinct_user_per_day(self):  #
		return self.data_link.query("""
	 		SELECT DATE_FORMAT(created, "%Y-%m-%d") date, COUNT(distinct user_id)
			FROM completion_blockcompletion cbc
			GROUP BY date
		""")
	
	def block_access_distinct_user_per_month(self):  #
		return self.data_link.query("""
	 		SELECT DATE_FORMAT(created, "%Y-%m") date, COUNT(distinct user_id)
			FROM completion_blockcompletion cbc
			GROUP BY date
		""")
	
	def course_metrics(self, course):
		return self.data_link.query("""
			select coc.id as id,
			(SELECT count(1) FROM student_courseenrollment sce WHERE sce.course_id = coc.id) AS enrollments,
  			(SELECT count(1) FROM certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificates
	    	from course_overviews_courseoverview coc
	    	where id = '{course_id}'
		""".format(course_id=course["id"]))[0]
	
	def invoice_data(self, course):
		return self.data_link.query("""
			select coc.id as id, coc.*,
			(SELECT count(1) FROM student_courseenrollment sce WHERE sce.course_id = coc.id) AS enrollments,
			(SELECT count(1) FROM certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificates
			from course_overviews_courseoverview coc
			where id = '{course_id}'
		""".format(course_id=course["id"]))[0]

