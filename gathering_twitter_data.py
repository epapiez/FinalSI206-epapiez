import requests
import json
import tweepy 
import sqlite3			
import os

consumer_key = "TsunQx1jhGMUye670SVDXRwQK"
consumer_secret = "kjpPtVqLpec4bbc9KdqjwtI2Xd32Ycz2Qt8qMiGnH63S4dGrZA"
access_token = "3894619752-jJu1Sp35n5qEnGRAdXDLEu4i28J4gVrAYR4zRIq"
access_token_secret = "ue8Gbd2J4IUEeObnbeSsCwTFC3GDBfyJdUmEzv70BxSXR"
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth, parser=tweepy.parsers.JSONParser(), wait_on_rate_limit=True)


def set_up_twitter_table(cur, conn):
    """Takes in the database cursor and connection as inputs. Returns nothing. Creates Twitter_Data and Follower_Data tables. Uses the song column from the Hot100 table to find tweets containing the song. Adds number of favorites per search, followers, and number of mentions per search."""
    #Creates two new tables to add to the existing music.db database.
    cur.execute("CREATE TABLE IF NOT EXISTS Twitter_Data (key INTEGER PRIMARY KEY, song TEXT, song_mentions INTEGER, song_favorites INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS Follower_Data (key INTEGER PRIMARY KEY, song TEXT, follower_count INTEGER)")
    cur.execute('SELECT song FROM Hot100')
    song_list = cur.fetchall()
    cur.execute('SELECT song FROM Twitter_Data')
    existing_song_names = cur.fetchall()
    existing_songs = []
    #Gets existing song names from the table so that we know not to add them again.
    for name in existing_song_names:
        existing_songs.append(name[0])
    key = len(existing_song_names)
    x = 0
    #Goes through each song in the Hot100 table, then adds 20 of those songs to the Twitter_Data and Follower_Data tables.
    for songs in song_list:
        if x < 20:
            song = songs[0]
            if song not in existing_songs:
                results = api.search(q=str(song), count=100)
                count = 0
                favorite_count = 0
                follower_count = 0
                for result in results['statuses']:
                    fav_count = result['favorite_count']
                    favorite_count = favorite_count + fav_count
                    fol_count = result['user']['followers_count']
                    follower_count = follower_count + fol_count
                    count = count + 1
                key = key + 1
                cur.execute("INSERT INTO Twitter_Data (key, song, song_mentions, song_favorites) VALUES (?, ?, ?, ?)", (key, song, count, favorite_count))
                cur.execute("INSERT INTO Follower_Data (key, song, follower_count) VALUES (?, ?, ?)", (key, song, follower_count))
                x = x + 1
                existing_songs.append(song)
            #If there are two songs with the same name but are by two different artists, we check to see if the artists are different and then the song is added.
            if song in existing_songs:
                cur.execute('SELECT artist_id FROM Hot100 WHERE song = ?', (song, ))
                artists = cur.fetchall()
                if len(artists) >= 2:
                    if artists[0] is not artists[1]:
                        cur.execute('SELECT creation_id FROM Hot100 WHERE song = ?', (song, ))
                        keys = cur.fetchall()
                        num = len(keys) - 1
                        idd = keys[num][0]
                        if int(idd) == (key + 1):
                            results = api.search(q=str(song), count=100)
                            count = 0
                            favorite_count = 0
                            follower_count = 0
                            for result in results['statuses']:
                                fav_count = result['favorite_count']
                                favorite_count = favorite_count + fav_count
                                fol_count = result['user']['followers_count']
                                follower_count = follower_count + fol_count
                                count = count + 1
                            key = key + 1
                            cur.execute("INSERT INTO Twitter_Data (key, song, song_mentions, song_favorites) VALUES (?, ?, ?, ?)", (key, song, count, favorite_count))
                            cur.execute("INSERT INTO Follower_Data (key, song, follower_count) VALUES (?, ?, ?)", (key, song, follower_count))
                            x = x + 1
                            existing_songs.append(song)

    conn.commit()

    
def join_tables(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a list of tuples with the number of song mentions and folower count where the songs are the same using a left join."""
    #Using the JOIN to use in calculations later on.
    cur.execute("SELECT Twitter_Data.song_mentions, Follower_Data.follower_count, Twitter_Data.song FROM Twitter_Data LEFT JOIN Follower_Data ON Twitter_Data.song = Follower_Data.song")
    results = cur.fetchall()
    conn.commit()
    return results


def average_followers_per_song(cur, conn):
    """Takes the database cursor and connection as inputs. Returns a list of strings specifying the average amount of followers of a Twitter user tweeting about a certain song. If the song had no mentions, that is specified. """
    #Empty list to store the average followers of each song.
    average_list = []
    results = join_tables(cur, conn)
    for result in results:
        #If the result has more than 0 mentions, the average is calculated and appended to the list.
        if result[0] > 0:
            average = result[1] / result[0]
            average_list.append("The average number of followers of a Twitter user tweeting about " + result[2] + " is " + str(average))
        #If the song does not have any mentions on Twitter, it is specified in the list.
        else:
            average_list.append("The song " + result[2] + " did not have any mentions on Twitter.")
    return average_list

def write_data_to_file(filename, cur, conn):
    """Takes the name of a file (string), database cursor and connection as inputs. Returns nothing. Writes the result of the function average_followers_per_song() to a file. """

    path = os.path.dirname(os.path.abspath(__file__)) + os.sep
    #Writes the results of the average_followers_per_song() function to a file.
    outFile = open(path + filename, "w")
    outFile.write("Average Followers of a Twitter User Based on the Song They Tweet About\n")
    outFile.write("=======================================================================\n\n")
    average_output = average_followers_per_song(cur, conn)
    for data in average_output:
        outFile.write(str(data) + '\n' + '\n')
    outFile.close()

def main():
    """Takes nothing as an input and returns nothing. Calls the functions set_up_twitter_table() and write_data_to_file(). Closes the database connection. """

    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/music.db')
    cur = conn.cursor()

    set_up_twitter_table(cur, conn)
    write_data_to_file("twitter_data.txt", cur, conn)

    conn.close()

if __name__ == "__main__":
    main()


