= DatabaseExporter =

Exports data from MySQL database into an Excel Spreedsheet. On the NAU project it is
used as a gateway for PowerBI data exploration.

Should be run once a day and the .xls file available to the PowerBI cloud engine
in order for NAU Managers to be able drill down data.

The queries should NEVER have reference to individual users. This means that the .xls
file should never have the fields "auth_user.*" or "user_id" directly available.

== Usage ==

* Setup a Virtual Environment
* Set the "config.ini" file based on the "config.init.sample".
* Execute "report.py"
