import csv
import sqlite3

# Connect to the SQLite in-memory database
conn = sqlite3.connect(':memory:')

# A cursor object to execute SQL commands
cursor = conn.cursor()


def main():

    # users table
    cursor.execute('''CREATE TABLE IF NOT EXISTS users (
                        userId INTEGER PRIMARY KEY,
                        firstName TEXT,
                        lastName TEXT
                      )'''
                   )

    # callLogs table (with FK to users table)
    cursor.execute('''CREATE TABLE IF NOT EXISTS callLogs (
        callId INTEGER PRIMARY KEY,
        phoneNumber TEXT,
        startTime INTEGER,
        endTime INTEGER,
        direction TEXT,
        userId INTEGER,
        FOREIGN KEY (userId) REFERENCES users(userId)
    )''')

    # You will implement these methods below. They just print TO-DO messages for now.
    load_and_clean_users('../../resources/users.csv')
    load_and_clean_call_logs('../../resources/callLogs.csv')
    write_user_analytics('../../resources/userAnalytics.csv')
    write_ordered_calls('../../resources/orderedCalls.csv')

    # Helper method that prints the contents of the users and callLogs tables. Uncomment to see data.
    # select_from_users_and_call_logs()

    # Close the cursor and connection. main function ends here.
    cursor.close()
    conn.close()


# TODO: Implement the following 4 functions. The functions must pass the unit tests to complete the project.


# This function will load the users.csv file into the users table, discarding any records with incomplete data
def load_and_clean_users(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row

        inserted_rows = 0  # Track number of inserted rows
        for row in csv_reader:
            # Clean the data: trim whitespace and check the number of columns
            row = [r.strip() for r in row if r.strip()]  # Remove extra spaces and ignore empty values

            print(f"Processing cleaned row: {row}")  # Debugging: print cleaned row

            if len(row) == 2:  # We only need 2 columns (firstName, lastName)
                cursor.execute('''
                    INSERT INTO users (firstName, lastName)
                    VALUES (?, ?)
                ''', (row[0], row[1]))
                inserted_rows += 1  # Increment for each inserted row

        conn.commit()  # Commit the transaction after all rows are processed
        print(f"Total rows inserted: {inserted_rows}")  # Debugging: check number of rows inserted

    # Debugging: Check the contents of the users table after insertion
    cursor.execute("SELECT * FROM users")
    rows = cursor.fetchall()
    print(f"Rows in users table: {rows}")

    print("TODO: load_users")


# This function will load the callLogs.csv file into the callLogs table, discarding any records with incomplete data
def load_and_clean_call_logs(file_path):
    with open(file_path, 'r') as file:
        csv_reader = csv.reader(file)
        next(csv_reader)  # Skip header row
        for row in csv_reader:
            # Clean the data: remove leading/trailing whitespace and ignore empty columns
            row = [r.strip() for r in row if r.strip()]
            print(f"Processing cleaned row: {row}")  # Debugging: print cleaned row
            if len(row) == 5:  # Ensure all 5 fields are present
                try:
                    # Insert cleaned row into callLogs table
                    cursor.execute('''
                        INSERT INTO callLogs (phoneNumber, startTime, endTime, direction, userId)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (row[0], int(row[1]), int(row[2]), row[3], int(row[4])))
                except ValueError as e:
                    print(f"Skipping row due to error: {e}")  # Log any conversion errors
        conn.commit()  # Commit transaction after all rows are processed

    # Debugging: Check the contents of the callLogs table after insertion
    cursor.execute("SELECT * FROM callLogs")
    rows = cursor.fetchall()
    print(f"Rows in callLogs table: {rows}")


# This function will write analytics data to testUserAnalytics.csv - average call time, and number of calls per user.
# You must save records consisting of each userId, avgDuration, and numCalls
# example: 1,105.0,4 - where 1 is the userId, 105.0 is the avgDuration, and 4 is the numCalls.
def write_user_analytics(csv_file_path):
    # Query the database to get the required analytics data
    cursor.execute('''
        SELECT userId, AVG(endTime - startTime) AS avgDuration, COUNT(*) AS numCalls
        FROM callLogs
        GROUP BY userId
    ''')

    # Fetch the results
    results = cursor.fetchall()

    # Write the results to the CSV file
    with open(csv_file_path, 'w', newline='') as file:
        csv_writer = csv.writer(file)
        csv_writer.writerow(['userId', 'avgDuration', 'numCalls'])  # Header row
        csv_writer.writerows(results)

    
    print("TODO: write_user_analytics")


# This function will write the callLogs ordered by userId, then start time.
# Then, write the ordered callLogs to orderedCalls.csv
def write_ordered_calls(csv_file_path):
    cursor.execute('''
        SELECT * FROM callLogs
        ORDER BY userId ASC, startTime ASC
    ''')
    ordered_calls = cursor.fetchall()

    # Write ordered calls to CSV file
    with open(csv_file_path, 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['callId', 'phoneNumber', 'startTime', 'endTime', 'direction', 'userId'])  # CSV header
        for row in ordered_calls:
            writer.writerow(row)

    print("TODO: write_ordered_calls")



# No need to touch the functions below!------------------------------------------

# This function is for debugs/validation - uncomment the function invocation in main() to see the data in the database.
def select_from_users_and_call_logs():

    print()
    print("PRINTING DATA FROM USERS")
    print("-------------------------")

    # Select and print users data
    cursor.execute('''SELECT * FROM users''')
    for row in cursor:
        print(row)

    # new line
    print()
    print("PRINTING DATA FROM CALLLOGS")
    print("-------------------------")

    # Select and print callLogs data
    cursor.execute('''SELECT * FROM callLogs''')
    for row in cursor:
        print(row)


def return_cursor():
    return cursor


if __name__ == '__main__':
    main()
