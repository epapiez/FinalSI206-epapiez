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
    cur.execute("CREATE TABLE IF NOT EXISTS Twitter_Data (song TEXT PRIMARY KEY, song_mentions INTEGER, song_favorites INTEGER)")
    cur.execute("CREATE TABLE IF NOT EXISTS Follower_Data (song TEXT PRIMARY KEY, follower_count INTEGER)")
    cur.execute('SELECT song FROM Hot100')
    song_list = cur.fetchall()
    cur.execute('SELECT song FROM Twitter_Data')
    existing_song_names = cur.fetchall()
    existing_songs = []
    for name in existing_song_names:
        existing_songs.append(name[0])
    x = 0
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
                cur.execute("INSERT INTO Twitter_Data (song, song_mentions, song_favorites) VALUES (?, ?, ?)", (song, count, favorite_count))
                cur.execute("INSERT INTO Follower_Data (song, follower_count) VALUES (?, ?)", (song, follower_count))
                x = x + 1
    conn.commit()

    
def join_tables(cur, conn):
    cur.execute("SELECT Twitter_Data.song_favorites, Follower_Data.follower_count FROM Twitter_Data LEFT JOIN Follower_Data ON Twitter_Data.song = Follower_Data.song")
    conn.commit()


def main():
    

    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/music.db')
    cur = conn.cursor()

    set_up_twitter_table(cur, conn)
    join_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()


