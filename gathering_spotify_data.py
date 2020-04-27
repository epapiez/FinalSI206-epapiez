import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import pandas as pd
import requests
import re
import os
import csv
import sqlite3
import json

cid = 'b40e6e0c49044ef08c8b05c0a6cf1c65'
secret = '558600c5de1e402b9380fd2af76121c5'

client_credentials_manager = SpotifyClientCredentials(client_id=cid, client_secret=secret)
sp = spotipy.Spotify(client_credentials_manager
=
client_credentials_manager)

def join_tables(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns a list of tuples in the format (song, artist) where the artist ids are the same."""
    #Joins the song column from the Hot 100 and the artist column from the Artist Ids tables where the artist ids are equal. 
    cur.execute("SELECT Hot100.song, ArtistIds.artist FROM Hot100 LEFT JOIN ArtistIds ON Hot100.artist_id = ArtistIds.artist_id")
    results = cur.fetchall()
    conn.commit()
    return results

def find_artists(artist_name):
    """Takes in an artist name as it is listed in the Hot100 table, then formats it so that it matches Spotify. Returns a list of strings of each artist that worked on the given song."""
    #An empty list to hold the artists that created a song. Takes into account any seperators used by the Billboard Hot 100.
    artist_list = []

    #If 'Featuring' is in the artist_name, only the name before 'Featuring' is appended to the artist_list.
    if "Featuring" in artist_name:
        new_name = artist_name.split("Featuring")
        artist_list.append(new_name[0].strip())

    #If 'DJ' is in the artist_name, only the word after 'DJ' is appended to the artist_list.
    elif "DJ" in artist_name:
        new_name = artist_name.split("DJ")
        artist_list.append(new_name[1].strip())

    #If '&' is in the artist name, it must be determined whether there are two or three artists in the list.
    elif "&" in artist_name:
        and_list = []
        new_name = artist_name.split("&")
        and_list.append(new_name[1].strip())
        #If there is a ',', it means there are more than two artists.
        if "," in new_name[0]:
            add = new_name[0].split(",")
            for x in range(len(add)):
                artist_list.append(add[x].strip())
                artist_list.append(and_list[0].strip())
        #If there is not a ',', it means there are only two artists.
        else:
            artist_list.append(new_name[0].strip())
            artist_list.append(and_list[0].strip())

    #If there is an 'X' in the artist_name, both artists on either side of the X are appended to the list.
    elif " X " in artist_name:
        new_name = artist_name.split("X")
        artist_list.append(new_name[1].strip())
        artist_list.append(new_name[0].strip())     

    #If there is an 'x' in the artist_name, both artists on either side of the X are appended to the list.
    elif " x " in artist_name:
        new_name =  artist_name.split("x")
        artist_list.append(new_name[0].strip())
        artist_list.append(new_name[1].strip())

    #If there is a 'Duet With' in the artist_name, both artists on either side of the 'Duet With' are appended to the list.
    elif "Duet With" in artist_name:
        new_name = artist_name.split("Duet With")
        artist_list.append(new_name[0].strip())
        artist_list.append(new_name[1].strip())
    
    #If 'Presents' is in the artist_name, only the word before 'Presents' is appended to the artist_list.
    elif "Presents" in artist_name:
        new_name = artist_name.split("Presents")
        artist_list.append(new_name[0].strip())

    #If there is only one artist, it appends it to the list.
    else:
        artist_list.append(artist_name)

    return artist_list

    


def set_up_spotify_table(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns nothing. Creates a table called Spotify_Data. Searches the Spotify popularity of each song and inserts it into the table."""
    #Creates a new table called Spotify_Data
    cur.execute("CREATE TABLE IF NOT EXISTS Spotify_Data (song TEXT, popularity INTEGER)")
    #Calls the join_tables() functions and assigns its return value to a variable
    tup_list = join_tables(cur, conn)
    #Goes through each artist and calls find_artists() to reformat the artist name to match Spotify's formatting.
    for tup in tup_list:
        #This bool checks whether the Billboard Hot 100 song was found or not
        if_found = False
        artists_name = tup[1]
        artist_name = find_artists(artists_name)
        #Goes through each artist in the list return by the find_artists() function.
        for x in range(len(artist_name)):
            if if_found == False:
                name = artist_name[x]
                #Searches for the artist in the list in Spotipy
                results = sp.search(q="artist:"+name, limit = 50)
                for result in results['tracks']['items']:
                    name_song = result['name']
                    hot100_name = tup[0]
                    #Reformats the songs so they fit Spotify's formatting.
                    if "(feat." in name_song:
                        new_name = name_song.split("(feat.")
                        name_song = new_name[0].strip()
                    if "(D" in name_song:
                        new_name = name_song.split("(D")
                        name_song = new_name[0].strip()
                    if "(with" in name_song:
                        new_name = name_song.split("(with")
                        name_song = new_name[0].strip()
                    if "(" in name_song and "(" in hot100_name:
                        tup_name = hot100_name.split("(")
                        song_name = name_song.split("(")
                        hot100_name = tup_name[0].strip()
                        name_song = song_name[0].strip()
                    if "(" in name_song and "(" not in hot100_name:
                        song_name = name_song.split("(")
                        name_song = song_name[0].strip()
                    if "Remix" in name_song:
                        tup_name = hot100_name.split(" ")
                        song_name = name_song.split(" ")
                        hot100_name = tup_name[0]
                        name_song = song_name[0]
                    #If the song found in Spotify matches the song found in Hot100, it's popularity is added into the Spotify_Data table.
                    if name_song.lower() in hot100_name.lower():
                        popularity = result['popularity']
                        cur.execute("INSERT INTO Spotify_Data (song, popularity) VALUES (?, ?)", (name_song, popularity))
                        #The bool is changed to True so that the song cannot be added twice.
                        if_found = True
                        break
        #If the loop is completed and the bool is still False, we search by song name and then check to see if the artist name matches.
        if if_found==False:
            hot100_name = tup[0]
            results = sp.search(q="song:"+hot100_name, limit = 50)
            for result in results['tracks']['items']:
                if result['artists'][0]['name'] in artist_name[0]:
                    popularity = result['popularity']
                    cur.execute("INSERT INTO Spotify_Data (song, popularity) VALUES (?, ?)", (hot100_name, popularity))
                    if_found = True
                    break
                elif artist_name[0] in result['artists'][0]['name'] :
                    popularity = result['popularity']
                    cur.execute("INSERT INTO Spotify_Data (song, popularity) VALUES (?, ?)", (hot100_name, popularity))
                    if_found = True
                    break

    conn.commit()


def average_popularity(cur, conn):
    """Take the database cursor and connection as inputs. Returns an integer, which is the average popularity of Billboard Hot 100 songs on Spotify."""
    pop_list = []
    cur.execute('SELECT popularity FROM Spotify_Data')
    popularities = cur.fetchall()
    for pop in popularities:
        pop_list.append(pop[0])
    #Finds the average number of weeks a song is on the Billboard Hot 100 by dividing the sum of the numbers in the list by the length of the list.
    average_popularity = sum(pop_list) / len(pop_list)
    return average_popularity

def write_data_to_file(filename, cur, conn):
    """Takes in a filename (string) as an input and the database cursor/connection. Returns nothing. Creates a file and writes return value of the function average_popularity() to the file. """

    path = os.path.dirname(os.path.abspath(__file__)) + os.sep

    outFile = open(path + filename, "w")
    outFile.write("Average Popularity of a BillBoard Hot 100 Song on Spotify\n")
    outFile.write("=============================================================\n\n")
    pop_output = (average_popularity(cur, conn))
    #This line rounds the average popularity to one decimal place.
    rounded_pop = int(pop_output*100) / 100
    outFile.write("The average popularity of a Billboard Hot 100 song on Spotify is " + str(rounded_pop) + "." + "\n\n")
    outFile.close()


def main():
    """Takes no inputs and returns nothing. Calls the function set_up_spotify_table()."""
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/music.db')
    cur = conn.cursor()

    set_up_spotify_table(cur, conn)

    write_data_to_file("spotify_data.txt", cur, conn)

    conn.close()



if __name__ == "__main__":
    main()
