# DatabaseExporter

Exports Open edX data from MySQL database into a spreadsheet. 
It is possible to export the sheets to a single multi sheet xlsx file or alternatively 
update a Google Sheet file. On the NAU project it is used the second option.
The NAU dashboard, based on Google Data Studio, use that Google Sheet has one of
its data source.

This project requires an intermediate database on the same engine of the `edxapp`
openedx database. The mysql database user needs a read grant for the `edxapp` and
all grants for its own database. It produces precalculated tables/materialized views 
that are 1 to 1 with the xlsx file sheets or each sheet of the google spreadsheet
file, each relevant table is prefixed with the `DATA_` string.

Those scripts should be run at least once a day, preference after the midnight, so your
Google Sheet file always contain yesterday's data in full.

The queries don't have any reference to individual users, and don't have specific
identification numbers, like user id, emails or similar data. 

# Usage

 - Setup a Virtual Environment
 - Set the `config.ini` file based on the `config.init.sample`.
 - Execute `report_xlsx.py` or `report_google.py`.

### Activate virtual environment and install its dependencies
```bash
virtualenv venv --python=python3
source venv/bin/activate
pip install -r requirements.txt --upgrade
```

### Set the "config.ini" file based on the "config.init.sample".
```bash
cp config.init.sample config.ini
vim config.ini
```

### Update precalculated data
To update the precalculated data run:

```bash
python update_data.py
```

### Export has xlsx file
```bash
python report_xlsx.py
```

### Update a Google Sheet
```bash
python report_google.py
```
