# Robert Minkler
# CSD Assignment 7.2
# Movies: Table Queries
# Sep 19, 2024


import mysql.connector
from mysql.connector import errorcode

config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}

def print_records(records: list, first_item_name: str, second_item_name: str):
    for record in records:
        print(f"{first_item_name}: {record[0]}")
        print(f"{second_item_name}: {record[1]}")
        print()


try:
    db = mysql.connector.connect(**config)

    print("\n Database user {} connected to MySQL on host {}".format(config['user'],config['host'], config['database']))
    print()

    # Create a cursor to query the database
    cursor = db.cursor()


    # ********************************************
    # ***** Fetch and Display Studio Records *****
    # ********************************************

    cursor.execute("SELECT * FROM studio")  # Get records
    studio_records = cursor.fetchall()      # Convert to iterable list of tuples

    # Display header
    print("-- DISPLAYING Studio RECORDS --")

    # Display each record
    print_records(studio_records, "Studio ID", "Studio Name")


    # ********************************************
    # ***** Fetch and Display Genre Records ******
    # ********************************************

    cursor.execute("SELECT * FROM genre")  # Get records
    genre_records = cursor.fetchall()      # Convert to iterable list of tuples

    # Display header
    print("-- DISPLAYING Genre RECORDS --")

    # Display each record
    print_records(genre_records, "Genre ID", "Genre Name")


    # ********************************************************
    # ***** Fetch and Display Movies less than two hours *****
    # ********************************************************

    # Get films under 120 minutes
    cursor.execute("""SELECT film_name, film_runtime 
                        FROM film 
                        WHERE film_runtime < 120""")

    short_films = cursor.fetchall() # Convert to iterable list of tuples

    # Display header
    print("-- DISPLAYING Short Film RECORDS --")

    # Display each record
    print_records(short_films, "Film Name", "Runtime")


    # *********************************************************************
    # ***** Get list of film names and directors grouped by director *****
    # *********************************************************************

    # Get film names and directors grouped by director
    cursor.execute("""SELECT film_name, film_director
                        FROM film
                        ORDER BY film_director""")

    director = cursor.fetchall()  # Convert to iterable list of tuples

    # Display header
    print("-- DISPLAYING Director RECORDS in Order --")

    # Display each record
    print_records(director, "Film Name", "Director")


except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("The specified database does not exist")

    else:
        print(err)

else:
    cursor.close()
    db.close()

