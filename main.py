import os
import cx_Oracle
import mysql.connector
from mysql.connector import errorcode
import logging
from datetime import datetime
import schedule
import time

# Function to create a log file with the current date if it doesn't exist
def create_log_file():
    # Create a directory named 'logs' if it doesn't exist
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)
    # Generate the log file path with the current date
    log_file_path = os.path.join(log_dir, f"data_exp_{datetime.now().strftime('%Y-%m-%d')}.log")
    # Create the log file if it doesn't exist and return its path
    if not os.path.exists(log_file_path):
        with open(log_file_path, 'a+') as f:
            f.write("")  # Create an empty file
    return log_file_path

# Configure logging for main log file
log_file_path = create_log_file()
logging.basicConfig(filename=log_file_path, filemode='a+', level=logging.INFO, format='%(asctime)s - %(levelname)s: %(message)s')

# Function to log a message with current timestamp
def log_message(message):
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logging.info(f"{current_time} - {message}")

# Function to run the script
def run_script():
    # Start script message
    start_msg = "Script started."
    print(start_msg)
    logging.info(start_msg)

    # Log Oracle database connection establishment
    oracle_conn_msg = "Establishing connection to Oracle database..."
    print(oracle_conn_msg)
    logging.info(oracle_conn_msg)

    # Oracle database connection details
    oracle_user = 'remote_user'
    oracle_password = 'remote_passwd'
    oracle_host = '10.0.193.147'
    oracle_service_name = 'en'
    oracle_dsn = f"{oracle_host}/{oracle_service_name}"

    try:
        # Connect to Oracle database
        oracle_connection = cx_Oracle.connect(user=oracle_user,
                                              password=oracle_password,
                                              dsn=oracle_dsn)

        # Log successful connection to Oracle database
        oracle_conn_success_msg = f"Connection to Oracle database established successfully. Connected to: {oracle_dsn}"
        print(oracle_conn_success_msg)
        logging.info(oracle_conn_success_msg)

        # Oracle cursor
        oracle_cursor = oracle_connection.cursor()

        # Fetch data from Oracle view
        oracle_fetch_msg = "Fetching records from Oracle table..."
        print(oracle_fetch_msg)
        logging.info(oracle_fetch_msg)
        
        oracle_cursor.execute("SELECT * FROM OCM_BUDGET_ADVANCE_SAMARTH")

        # Fetch all rows from the view
        rows = oracle_cursor.fetchall()

        # Print fetched records
        print("Fetched records:")
        for row in rows:
            print(row)

        # Log the number of records fetched from the Oracle table
        num_records_fetched_msg = f"Fetched {len(rows)} records from Oracle table."
        print(num_records_fetched_msg)
        logging.info(num_records_fetched_msg)
        
        # Close the Oracle connection
        oracle_cursor.close()
        oracle_connection.close()

        # Log Oracle connection closure
        oracle_conn_close_msg = "Oracle database connection closed."
        print(oracle_conn_close_msg)
        logging.info(oracle_conn_close_msg)

        if rows:
            # Log MySQL database connection establishment
            mysql_conn_msg = "Establishing connection to MySQL database..."
            print(mysql_conn_msg)
            logging.info(mysql_conn_msg)

            # MySQL database connection details
            mysql_user = 'report'
            mysql_password = 'test4321'
            mysql_host = '10.0.243.155'
            mysql_database = 'iew'
            # auth_plugin='mysql_native_password'  # Specify the authentication plugin
            mysql_connection_details = f"mysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_database}"


            # Connect to MySQL database
            mysql_connection = mysql.connector.connect(user=mysql_user,
                                                        password=mysql_password,
                                                        host=mysql_host,
                                                        database=mysql_database,
                                                        auth_plugin='mysql_native_password')

            # Log successful connection to MySQL database
            mysql_conn_success_msg = f"Connection to MySQL database established successfully. Connected to: {mysql_connection_details}"
            print(mysql_conn_success_msg)
            logging.info(mysql_conn_success_msg)

            # MySQL cursor
            mysql_cursor = mysql_connection.cursor()

            # Log the number of new records found
            new_records_msg = f"Found {len(rows)} records to insert into MySQL."
            print(new_records_msg)
            logging.info(new_records_msg)

            # Insert data into MySQL if rows exist
            total_inserted = 0
            total_skipped = 0

            for row in rows:
                # Check if the employee ID already exists in the MySQL table
                mysql_cursor.execute("SELECT EMPLOYEE_ID FROM OCM_BUDGET_ADVANCE_SAMARTH WHERE EMPLOYEE_ID = %s", (row[0],))
                result = mysql_cursor.fetchone()

                # If employee ID doesn't exist, insert the row into MySQL table
                if not result:
                    mysql_cursor.execute("INSERT IGNORE INTO OCM_BUDGET_ADVANCE_SAMARTH (EMPLOYEE_ID, FIRST_NAME, LAST_NAME, EMAIL, PHONE_NUMBER, HIRE_DATE, JOB_ID, SALARY, COMMISSION_PCT, MANAGER_ID, DEPARTMENT_ID) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)", row)
                    total_inserted += 1
                else:
                    # Log message for duplicate ID found
                    duplicate_msg = f"Duplicate ID {row[0]} found. Skipping insertion."
                    # print(duplicate_msg)
                    # logging.warning(duplicate_msg)
                    total_skipped += 1

            # Commit the transaction
            mysql_connection.commit()

            # Log the summary entry to the main log file
            summary_msg = f"{total_inserted} data inserted successfully into MySQL. {total_skipped} duplicate records skipped."
            print(summary_msg)
            logging.info(summary_msg)

            # Close MySQL connections
            mysql_cursor.close()
            mysql_connection.close()

            # Log MySQL connection closure
            mysql_conn_close_msg = "MySQL database connection closed."
            print(mysql_conn_close_msg)
            logging.info(mysql_conn_close_msg)

        else:
            print("No data found in Oracle table")
            logging.warning("No data found in Oracle table")

    except cx_Oracle.DatabaseError as e:
        print(f"Error connecting to Oracle database: {e}")
        logging.error(f"Error connecting to Oracle database: {e}")

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            error_message = "Error: Access denied. Invalid credentials."
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            error_message = "Error: Database does not exist."
        else:
            error_message = f"Error connecting to MySQL database: {err}"
        print(error_message)
        logging.error(error_message)


    except KeyboardInterrupt:
        print("Exiting script...")
        logging.info("User pressed Ctrl + C. Exiting script...")
        # Exiting script
        print("Exiting script...")
        logging.info("Exiting script...")
        sys.exit()

    except Exception as e:
        print(f"Error: {e}")
        logging.error(f"Error: {e}")

    finally:
        # End script message
        end_msg = "Script ended."
        print(end_msg)
        logging.info(end_msg)

        # Log the time script executed
        script_execution_time_msg = f"Script executed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} \n\n"
        print(script_execution_time_msg)
        logging.info(script_execution_time_msg)


# Run the script immediately
run_script()

# Schedule the script to run every 5 minutes
schedule.every(5).minutes.do(run_script)

while True:
    schedule.run_pending()
    time.sleep(1)
