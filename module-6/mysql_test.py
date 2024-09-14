# Robert Minkler
# Sep 11, 2024
# CSD 310 Module 6.2
# Movies Setup py


import mysql.connector
from mysql.connector import errorcode

# Configure db connection
config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}


try:
    # connect to db
    db = mysql.connector.connect(**config)

    print("\n Database user {} connected to MySQL on host {}".format(config['user'],config['host'], config['database']))

    # Wait for enter key press before ending the program
    input('\n\n Press enter/return key to continue...')


except mysql.connector.Error as err:
    # print connection errors

    # bad login
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("The supplied username or password are invalid")

    # db not found
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("The specified database does not exist")

    # all other errors
    else:
        print(err)

# close the db connection
finally:
    db.close()
