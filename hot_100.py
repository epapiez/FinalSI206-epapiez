from bs4 import BeautifulSoup
import requests
import re
import os
import csv
import sqlite3
import json

def get_songs_and_artists():
    """No inputs. Returns a list of tuples in the format (song, artist, weeks). Uses BeautifulSoup to read the top 100 songs, along with their artist and the weeks it has been on the chart"""
    #Empty lists to collect the names of songs, artists, and number of weeks each song has been on the list.
    song_list = []
    artist_list = []
    week_list = []
    #Empty to list collect tuples in the format (song, artist, weeks on chart)
    tup_list = []
    #Using beautiful soup to get data from billboard.com
    base_url = 'https://www.billboard.com/charts/hot-100'
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    songs = soup.find_all("span", class_="chart-element__information__song text--truncate color--primary")
    artists = soup.find_all("span", class_="chart-element__information__artist text--truncate color--secondary")
    weeks = soup.find_all("span", class_= "chart-element__information__delta__text text--week")
    #Goes through the results from the beautiful soup object to find songs, artists, and weeks on chart
    for song in songs:
        song_list.append(song.text)
    for artist in artists:
        artist_list.append(artist.text)
    for week in weeks:
        week_text = week.text
        week_count = re.findall("\d{1,3}", week_text)
        week_num = week_count[0]
        week_list.append(int(week_num))
    #Adds each tuple in the format (song, artist, weeks on chart) to the list
    x = 0
    for x in range(len(song_list)):
        tup_list.append((song_list[x], artist_list[x], week_list[x]))
        x = x + 1
    return tup_list

def setUpDatabase(db_name):
    """Takes the name of a database, a string, as an input. Returns the cursor and connection to the database."""
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def fillup_hot_100_table(cur, conn):
    """Takes the database cursor and connection as inputs. Does not return anything. Fills in the Hot100 table with songs and their artist_ids and how long they've been on the charts. The creation_id is each song's unique identification number."""
    #Calls get_songs_and_artists() to get the songs off of the Billboard Hot 100.
    top_100_list = get_songs_and_artists()
    #Selects songs that are already in the Hot100 table and puts them in a list so that we know which songs should not be repeated.
    cur.execute('SELECT song FROM Hot100')
    song_list = cur.fetchall()
    x = 1
    count = len(song_list)
    #Adds songs to the Hot100 list that are not already in it, 20 at a time.
    for x in range(20):
        x = count
        #The creation_id is the unique id that identifies a song.
        creation_id = count + 1
        song = top_100_list[count][0]
        weeks = top_100_list[count][2]
        x = x + 1
        #In order to save storage space, we are adding in ArtistIds (integers) instead of artists.
        cur.execute('SELECT artist_id, artist FROM ArtistIds')
        artist_ids = cur.fetchall()
        for artist_tup in artist_ids:
            if artist_tup[1] == top_100_list[count][1]:
                artist = artist_tup[0]
                cur.execute("INSERT OR IGNORE INTO Hot100 (creation_id, song, artist_id, weeks_on_chart) VALUES (?, ?, ?, ?)", (creation_id, song, int(artist), weeks))
        count = count + 1
    conn.commit()

def set_up_tables(cur, conn):
    """ Takes the database cursor and connection as inputs. Returns nothing. Creates two tables, one that will hold artists and their artist_ids, and another that holds the top 100 songs, along with their artist_ids and weeks on chart."""
    cur.execute("CREATE TABLE IF NOT EXISTS ArtistIds (artist_id INTEGER PRIMARY KEY, artist TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS Hot100 (creation_id INTEGER PRIMARY KEY, song TEXT, artist_id INTEGER, weeks_on_chart INTEGER)")
    conn.commit()

def set_up_artist_id_table(cur, conn):
    """Takes the database cursor and connection as inputs. Returns nothing. Fills in the ArtistIds table with the artists in the Billboard Hot 100 table and their unique artist_id."""
    cur.execute("CREATE TABLE IF NOT EXISTS ArtistIds (artist_id INTEGER PRIMARY KEY, artist TEXT)")
    #Calls the get_songs_and_artists() function and saves it into a list
    top_100_list = get_songs_and_artists()
    #Empty list to store artist names in
    artist_list = []
    #Loops through the top_100_list and adds in the artist and their id number
    count = 0
    for x in range(len(top_100_list)):
        #Only adds in the artist to the artist_list if it is not already in to artist_list
        if top_100_list[x][1] not in artist_list:
            artist_id = count
            artist = top_100_list[x][1]
            artist_list.append(artist)
            cur.execute("INSERT OR IGNORE INTO ArtistIds (artist_id, artist) VALUES (?, ?)", (artist_id, artist))
            count = count + 1
        x = x + 1
    conn.commit()


def find_top_three_artists():
    """Takes nothing as an input. Returns a list of strings (the three artists with the most songs on the Billboard Hot 100). Utilizes the function get_songs_and_artists(). """
    #Empty lists to store the artists, the top three artists, and a dictionary to store the number of times an artist is on the Billboard Hot 100 list.
    artist_list = []
    top_three = []
    artist_count_dict = {}
    top_100_list = get_songs_and_artists()
    sorted_items = sorted(top_100_list, key=lambda t: t[1])
    for song in sorted_items:
        artist = song[1]
        if artist not in artist_count_dict:
            artist_count_dict[artist] = 1
        else:
            artist_count_dict[artist] = artist_count_dict[artist] + 1
    artist_items = artist_count_dict.items()
    #Sorts the artist dictionary based on most times on list to least times on list
    sorted_artists = sorted(artist_items, key = lambda t : t[1], reverse=True)
    #Appends the first three numbers in the sorted_artists list to the top_three list.
    top_three.append(sorted_artists[0][0] + " has " + str(sorted_artists[0][1]) + " songs on the Billboard Hot 100.")
    top_three.append(sorted_artists[1][0] + " has " + str(sorted_artists[1][1]) + " songs on the Billboard Hot 100.")
    top_three.append(sorted_artists[2][0] + " has " + str(sorted_artists[2][1]) + " songs on the Billboard Hot 100.")
    return top_three
    
def get_average_weeks(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a string with the average number of weeks a song is on the chart."""
    #Empty list to store the number of weeks that each song is on the Billboard Hot 100.
    weeks_list = []
    cur.execute('SELECT weeks_on_chart FROM Hot100')
    weeks = cur.fetchall()
    for week in weeks:
        weeks_list.append(week[0])
    #Finds the average number of weeks a song is on the Billboard Hot 100 by dividing the sum of the numbers in the list by the length of the list.
    average_weeks = sum(weeks_list) / len(weeks_list)
    return("The average amount of weeks spent on the Billboard Hot 100 based on songs currently on it is " + str(average_weeks) + " weeks.")


def write_data_to_file(filename, cur, conn):
    """Takes in a filename (string), the database cursor and connection as inputs. Returns nothing. Creates a file and writes return values of the functions find_top_three_artists() and get_average_weeks() to the file. """
    #Once the table is done being filled (once it reaches 100 rows), the calculations are written to a file.
    cur.execute('SELECT weeks_on_chart FROM Hot100')
    weeks = cur.fetchall()
    if len(weeks) == 100:

        path = os.path.dirname(os.path.abspath(__file__)) + os.sep

        outFile = open(path + filename, "w")
        outFile.write("Average Weeks a Song has Spent on the Billboard 100\n")
        outFile.write("=====================================================\n\n")
        weeks_output = str(get_average_weeks(cur, conn))
        outFile.write(weeks_output + "\n\n")
        outFile.write("Top Three Artists on the Billboard 100\n")
        outFile.write("======================================================\n\n")
        artist_output = find_top_three_artists()
        outFile.write("1. " + artist_output[0] + "\n")
        outFile.write("2. " + artist_output[1] + "\n")
        outFile.write("3. " + artist_output[2] + "\n")
        outFile.close()



def main():
    """Takes nothing as an input and returns nothing. Calls the functions setUpDatabase(), set_up_tables(), set_up_artist_id_table(), fill_up_hot_100_table(), and write_data_to_file(). Closes the database connection."""
    cur, conn = setUpDatabase('music.db')
    set_up_tables(cur, conn)
    set_up_artist_id_table(cur, conn)
    fillup_hot_100_table(cur, conn)

    write_data_to_file("music_data.txt", cur, conn)
    conn.close()

if __name__ == "__main__":
    main()