# Robert Minkler
# CSD Assignment 8.2
# Movies: Update and Delete
# Sep 21, 2024

def show_films(cursor, title):
    """
    Query the database for film data
    Then display the results
    """

    # inner join query to fetch movie data
    cursor.execute("""
                    SELECT film_name AS Name,
                        film_director AS Director,
                        genre_name AS Genre,
                        studio_name AS 'Studio Name'
                        FROM film
                        INNER JOIN genre ON film.genre_id = genre.genre_id
                        INNER JOIN studio ON film.studio_id = studio.studio_id;
                    """)

    # get the results from the cursor
    films = cursor.fetchall()

    # Print the title
    print(f"\n  -- {title} --")

    # iterate over and display each entry
    for film in films:
        print(f"Film Name: {film[0]}")
        print(f"Director: {film[1]}")
        print(f"Genre Name ID: {film[2]}")
        print(f"Studio Name: {film[3]}")
        print()


def add_film(cursor, film_name, release_date, runtime, director, studio_name, genre):
    """
    Add a film to the database
    Add genre and studio to their tables if necessary
    """

    # *************************************
    # Insert genre if not in genre table **
    # *************************************

    # There should be a unique constraint on genre_name, but since it does not exist, I will work around it.
    # Check if genre is in the genre table
    check_genre = f"SELECT EXISTS(SELECT * FROM genre WHERE genre_name = \"{genre}\");"
    cursor.execute(check_genre)
    genre_in_table = cursor.fetchall()[0][0]

    # if not in table add it
    if not genre_in_table:
        insert_genre = "INSERT INTO genre (genre_name) VALUES (%s);"
        genre_to_insert = (genre,)
        cursor.execute(insert_genre, genre_to_insert)


    # ********************************
    # Insert studio if not in table **
    # ********************************

    # Check if studio is in the studio table
    check_studio = f"SELECT EXISTS(SELECT * FROM studio WHERE studio_name = \"{studio_name}\");"
    cursor.execute(check_studio)
    studio_in_table = cursor.fetchall()[0][0]

    # if not in table add it
    if not studio_in_table:
        insert_studio = "INSERT INTO studio (studio_name) VALUES (%s);"
        studio_to_insert = (studio_name,)
        cursor.execute(insert_studio, studio_to_insert)


    # *******************************
    # Insert movie into film table **
    # *******************************

    insert_movie = ("INSERT INTO film (film_name, film_releaseDate, film_runtime, film_director, studio_id, genre_id) "
                    "VALUES (%s, %s, %s, %s, "
                    f"(SELECT studio_id FROM studio WHERE studio_name = \"{studio_name}\" LIMIT 1), "
                    f"(SELECT genre_id FROM genre WHERE genre_name = \"{genre}\" LIMIT 1));")
    movie_to_insert = (film_name, release_date, runtime, director)
    cursor.execute(insert_movie, movie_to_insert)



import mysql.connector
from mysql.connector import errorcode

# Database configuration
config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}

try:
    # connect to database
    db = mysql.connector.connect(**config)

    print("\n Database user {} connected to MySQL on host {}".format(config['user'],config['host'], config['database']))
    print()

    # Create a cursor to query the database
    movies_cursor = db.cursor()

    # Display all films in the database
    show_films(movies_cursor, "DISPLAYING FILMS")

    # Add Iron Man 3 to the database
    add_film(movies_cursor, "Iron Man 3", 2013, 130, "Shane Black", "Marvel Studios", "Superhero")

    # Display all films in the database after insert
    show_films(movies_cursor, "DISPLAYING FILMS AFTER INSERT - Added Iron Man 3")

    # Update the film Alien to being a Horror film.
    update_alien = ("UPDATE film "
                    "SET genre_id = (SELECT genre_id FROM genre WHERE genre_name = 'Horror') "
                    "WHERE film_name = 'Alien';")
    movies_cursor.execute(update_alien)

    # Display all films in the database after update to Aliens to Horror
    show_films(movies_cursor, "DISPLAYING FILMS AFTER UPDATE - Make Alien a Horror Film")

    # Delete the movie Gladiator.
    delete_gladiator = "DELETE FROM film WHERE film_name = 'Gladiator';"
    movies_cursor.execute(delete_gladiator)

    # Display all films in the database after DELETE of Gladiator
    show_films(movies_cursor, "DISPLAYING FILMS AFTER DELETE - Gladiator")


# Error messages on failure
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        print("The supplied username or password are invalid")

    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        print("The specified database does not exist")

    else:
        print(err)

finally:
    # Close cursor, commit changes, and close the database
    movies_cursor.close()
    db.commit()
    db.close()

