from bs4 import BeautifulSoup
import requests
import re
import os
import csv
import sqlite3
import json

def get_songs_and_artists():
    song_list = []
    artist_list = []
    week_list = []
    tup_list = []
    base_url = 'https://www.billboard.com/charts/hot-100'
    r = requests.get(base_url)
    soup = BeautifulSoup(r.text, 'html.parser')
    songs = soup.find_all("span", class_="chart-element__information__song text--truncate color--primary")
    artists = soup.find_all("span", class_="chart-element__information__artist text--truncate color--secondary")
    weeks = soup.find_all("span", class_= "chart-element__information__delta__text text--week")
    for song in songs:
        song_list.append(song.text)
    for artist in artists:
        artist_list.append(artist.text)
    for week in weeks:
        week_text = week.text
        week_count = re.findall("\d{1,3}", week_text)
        week_num = week_count[0]
        week_list.append(int(week_num))
    x = 0
    for x in range(len(song_list)):
        tup_list.append((song_list[x], artist_list[x], week_list[x]))
        x = x + 1
    return tup_list

def setUpDatabase(db_name):
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/'+db_name)
    cur = conn.cursor()
    return cur, conn

def setup_hot_100_table(cur, conn):
    top_100_list = get_songs_and_artists()
    cur.execute("CREATE TABLE IF NOT EXISTS Hot100 (creation_id INTEGER PRIMARY KEY, song TEXT, artist TEXT, weeks_on_chart INTEGER)")
    x = 0
    for x in range(len(top_100_list)):
        creation_id = x
        song = top_100_list[x][0]
        artist = top_100_list[x][1]
        weeks = top_100_list[x][2]
        x = x + 1
        cur.execute("INSERT OR IGNORE INTO Hot100 (creation_id, song, artist, weeks_on_chart) VALUES (?, ?, ?, ?)", (creation_id, song, artist, weeks))
    conn.commit()

def find_top_three_artists():
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
    sorted_artists = sorted(artist_items, key = lambda t : t[1], reverse=True)
    top_three.append(sorted_artists[0][0] + " has " + str(sorted_artists[0][1]) + " songs on the Billboard Hot 100.")
    top_three.append(sorted_artists[1][0] + " has " + str(sorted_artists[1][1]) + " songs on the Billboard Hot 100.")
    top_three.append(sorted_artists[2][0] + " has " + str(sorted_artists[2][1]) + " songs on the Billboard Hot 100.")
    return top_three
    
def get_average_weeks(cur, conn):
    weeks_list = []
    cur.execute('SELECT weeks_on_chart FROM Hot100')
    weeks = cur.fetchall()
    for week in weeks:
        weeks_list.append(week[0])
    average_weeks = sum(weeks_list) / len(weeks_list)
    print(average_weeks)
    




def main():
    cur, conn = setUpDatabase('music.db')
    setup_hot_100_table(cur, conn)
    get_average_weeks(cur, conn)
    conn.close()

if __name__ == "__main__":
    main()