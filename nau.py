from datetime import datetime
import mysql.connector


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
	
	def __init__(self, settings):
		self.data_link = DataLink(settings)
	
	def summary(self):
		return [dict({
			"Version": "v1",
			"DataBase": (self.data_link.settings["host"] + ":" + self.data_link.settings["port"]),
			"Date": datetime.now(),
			"Organizations": self.data_link.get("SELECT count(*) FROM organizations_organization"),
			# Global
			"Courses": self.data_link.get("SELECT count(*) FROM course_overviews_courseoverview"),
			"Users": self.data_link.get("SELECT count(*) FROM auth_user"),
			"Enrollments": self.data_link.get("SELECT count(*) FROM student_courseenrollment"),
			"Certificates": self.data_link.get("SELECT count(*) FROM certificates_generatedcertificate"),
			# 7 Days
			"New Users - 7 days": self.data_link.get("SELECT count(*) FROM auth_user au WHERE au.date_joined > NOW() - INTERVAL 7 DAY"),
			"New Enrollments - 7 days": self.data_link.get("SELECT count(*) FROM student_courseenrollment sce WHERE sce.created > NOW() - INTERVAL 7 DAY"),
			"News Certificates - 7 days": self.data_link.get("SELECT count(*) FROM certificates_generatedcertificate cgc WHERE cgc.created_date > NOW() - INTERVAL 7 DAY"),
			# 15 Dayus
			"New Users - 15 days": self.data_link.get("SELECT count(*) FROM auth_user au WHERE au.date_joined > NOW() - INTERVAL 15 DAY"),
			"New Enrollments - 15 days": self.data_link.get("SELECT count(*) FROM student_courseenrollment sce WHERE sce.created > NOW() - INTERVAL 15 DAY"),
			"News Certificates - 15 days": self.data_link.get("SELECT count(*) FROM certificates_generatedcertificate cgc WHERE cgc.created_date > NOW() - INTERVAL 15 DAY"),
			# 30 Days
			"New Users - 30 days": self.data_link.get("SELECT count(*) FROM auth_user au WHERE au.date_joined > NOW() - INTERVAL 30 DAY"),
			"New Enrollments - 30 days": self.data_link.get("SELECT count(*) FROM student_courseenrollment sce WHERE sce.created > NOW() - INTERVAL 30 DAY"),
			"News Certificates - 30 days": self.data_link.get("SELECT count(*) FROM certificates_generatedcertificate cgc WHERE cgc.created_date > NOW() - INTERVAL 30 DAY"),
		})]
	
	def organizations(self):
		return self.data_link.query("SELECT * FROM organizations_organization")
	
	def courses(self):
		return self.data_link.query("SELECT * FROM course_overviews_courseoverview")
	
	def overall_course_metrics(self):
		return self.data_link.query("""
			SELECT coc.id, coc.display_name, coc.display_org_with_default,
			   (select COUNT(*) from student_courseenrollment sce WHERE sce.course_id = coc.id) AS enrolled,
			   (select COUNT(*) from certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificates,
			   (select AVG(grade) from certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS average_grade
			FROM course_overviews_courseoverview coc
		""")
	
	def certificates_by_date(self):
		return self.data_link.query("""
		  SELECT DATE_FORMAT(cgc.created_date, "%Y-%m-%d") AS date, COUNT(*) AS cnt
		  FROM certificates_generatedcertificate cgc
		  GROUP	BY date
		  """)
	
	def current_enrollment_distribution(self):
		return self.data_link.query("""
			SELECT
				sce.user_id as user, COUNT(*) as cnt
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
				date_format(date_joined, "%Y-%m-%d") as registo,
				COUNT(*) AS cnt,
				SUM(is_active) AS active
			FROM
				auth_user au
			GROUP BY registo
			""")
		
		cnt_registered = 0
		cnt_active = 0
		for line in response:
			cnt_registered += line["cnt"]
			cnt_active += line["active"]
			line["sum_cnt"] = cnt_registered
			line["sum_active"] = cnt_active
		
		return response

	def studentmodule_history(self):
		return self.data_link.query("""
   			SELECT DATE_FORMAT(csm.created, "%Y-%m-%d") data, COUNT(csm.id) cnt
			FROM courseware_studentmodule csm
			GROUP BY data
		""")
		
	def courseenrollment_allowed(self):
		return self.data_link.query("""
   			SELECT course_id, COUNT(*) as students
			FROM student_courseenrollmentallowed scea
			GROUP BY course_id
		""")
		
	def student_enrolled_by_course_by_date(self): #
		return self.data_link.query("""
   			SELECT  course_id, DATE_FORMAT(sce.created, "%Y-%m-%d") data, count(sce.user_id) as students
			FROM student_courseenrollment sce
			GROUP BY course_id, data
		""")

	def student_passed_by_date(self): #
		return self.data_link.query("""
   			SELECT course_id, data, COUNT(user_id) AS test, SUM(passou) AS pass
			FROM (
				SELECT course_id, DATE_FORMAT(gpg.created, "%Y-%m-%d") AS data, user_id, if(gpg.percent_grade > 0,1,0) AS passou
				FROM grades_persistentcoursegrade gpg
		    ) AS a
			GROUP BY course_id, data
		""")

	def completed_blocks_by_date(self): #
		return self.data_link.query("""
   			SELECT course_key, block_type, date_format(cbc.created, "%Y-%m-%d") as date, COUNT(user_id) as users
			FROM completion_blockcompletion cbc
			GROUP BY course_key, block_type, date
		""")
	
	def course_metrics(self, course):
		return self.data_link.query("""
			select coc.id as id,
			(SELECT COUNT(*) FROM student_courseenrollment sce WHERE sce.course_id = coc.id) AS inscritos,
  			(SELECT COUNT(*) FROM certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificados
	    	from course_overviews_courseoverview coc
	    	where id = '{course_id}'
		""".format(course_id=course["id"]))[0]
	
	def invoice_data(self, course):
		return self.data_link.query("""
			select coc.id as id, coc.*,
			(SELECT COUNT(*) FROM student_courseenrollment sce WHERE sce.course_id = coc.id) AS inscritos,
			(SELECT COUNT(*) FROM certificates_generatedcertificate cgc WHERE cgc.course_id = coc.id) AS certificados
			from course_overviews_courseoverview coc
			where id = '{course_id}'
		""".format(course_id=course["id"]))[0]

