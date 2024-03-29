# Data Export Script

This script fetches data from an Oracle database and inserts it into a MySQL database.

## Requirements

- Python 3.7 or above
- cx_oracle
- mysql-connector
- schedule

To install the required Python packages, run:

```bash
	pip install -r requirements.txt


Usage
	Configure the Oracle and MySQL database connection details in the script.
	Run the script using Python:
		python data_export.py


The script will connect to the Oracle database, fetch data from the HR.EMPLOYEES table, and insert it into the MySQL database. It will log the execution details in the data_exp.log file.

The script is scheduled to run every 5 minutes by default. You can adjust the scheduling interval as needed in the script.


Logging
	The script logs various events, including:

		Start and end of script execution
		Connection establishment and closure for Oracle and MySQL databases
		Number of records fetched from Oracle table
		Number of records inserted into MySQL table

