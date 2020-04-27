import plotly.graph_objects as go
import plotly.express as px
import numpy as np
from bs4 import BeautifulSoup
import requests
import re
import os
import csv
import sqlite3
import json


def main():
    """Takes no inputs and returns nothing. Selects data from the database in order to create visualizations (two dot plots, a scatterplot, and two bar charts.) """
    path = os.path.dirname(os.path.abspath(__file__))
    conn = sqlite3.connect(path+'/music.db')
    cur = conn.cursor()

    #Creates a list of tuples with the songs and the follower counts from the Follower_Data table
    cur.execute('SELECT follower_count, song FROM Follower_Data')
    follower_counts_list = cur.fetchall()
    follower_counts = []
    song_names = []
    for tup in follower_counts_list:
        follower_counts.append(tup[0])
        song_names.append(tup[1])

    #Creates a list of the Twitter favorites each song has per about 100 mentions.
    cur.execute('SELECT song_favorites FROM Twitter_Data')
    favorite_counts_list = cur.fetchall()
    favorite_counts = []
    for tup in favorite_counts_list:
        favorite_counts.append(tup[0])

    #Creates a list of the weeks each song has been on the Billboard Hot 100 chart.
    cur.execute('SELECT weeks_on_chart FROM Hot100')
    week_counts_list = cur.fetchall()
    week_counts = []
    for tup in week_counts_list:
        week_counts.append(tup[0])

    #Creates a list of the rankings of each song. The unique id of each song is the same as its ranking on the Billboard Hot 100.
    cur.execute('SELECT creation_id FROM Hot100')
    rank_list = cur.fetchall()
    rank_on_chart = []
    for tup in rank_list:
        rank_on_chart.append(tup[0])

    #Creates a three lists by using a LEFT JOIN.
    cur.execute("SELECT Spotify_Data.popularity, Spotify_Data.song, Hot100.creation_id FROM Spotify_Data LEFT JOIN Hot100 ON Hot100.song = Spotify_Data.song")
    results = cur.fetchall()
    #Lists of the popularity, ranks, and songs.
    popularity_list = []
    rank_list = []
    song_name_list = []
    for res in results:
        popularity_list.append(res[0])
        rank_list.append(res[2])
        song_name_list.append(res[1])



    #Creates a scatter plot of each song's ranking on the chart that shows the number of favorites and their ranking to see if there is a correlation.
    fig2 = go.Figure()
    fig2.add_trace(go.Scatter(
        x=rank_on_chart,
        y=song_names,
        marker=dict(color="rgb(204,255,255)", size=12),
        mode="markers",
        name="Rank on chart",
    ))
    fig2.add_trace(go.Scatter(
        x=favorite_counts,
        y=song_names,
        marker=dict(color="rgb(0, 153, 153)", size=12),
        mode="markers",
        name="Favorites on Twitter per 100 Tweets",
    ))

    fig2.update_layout(title = "Popularity of songs on Twitter vs. Billboard 100",
                        xaxis_title="Number of favorites/rank", yaxis_title="Songs", xaxis=dict(range=[0, 250]))
    
    fig2.show()




    #Creates a dot plot comparing the number of weeks a song has been on the Billboard Hot 100 with the number of weeks it has been on the chart.
    fig3 = go.Figure()
    fig3.add_trace(go.Scatter(
        x=rank_on_chart,
        y=song_names,
        marker=dict(color="rgb(204,255,255)", size=12),
        mode="markers",
        name="Rank on chart",
    ))
    fig3.add_trace(go.Scatter(
        x=week_counts,
        y=song_names,
        marker=dict(color="rgb(0, 153, 153)", size=12),
        mode="markers",
        name="Weeks on chart",
    ))

    fig3.update_layout(title = "Weeks vs Ranking of Songs on Billboard Hot 100",
                        xaxis_title="Number of weeks/rank", yaxis_title="Songs")
    
    fig3.show()




    #Creates a scatterplot of comparing popularity of a song on Spotify versus its ranking on the Billboard Hot 100.
    fig = go.Figure(data=go.Scatter(x=popularity_list,
                                y=rank_list,
                                mode='markers',
                                text=song_name_list, marker_color = "rgb(153, 0, 153)"))
    fig.update_traces(mode='markers', marker_line_width=2, marker_size=15)
    fig.update_layout(title='Song Ranking on Billboard Hot 100 vs Popularity on Spotify', xaxis_title="Popularity on Spotify", yaxis_title="Ranking on Billboard Hot 100")
    fig.show()




    #Creates a bar chart demonstrating the popularity of songs on Twitter in the order of their ranking on the Billboard Hot 100.
    barfig = go.Figure([go.Bar(y=popularity_list, x=song_name_list)])
    barfig.update_traces(marker_color="rgb(204, 153, 255)", marker_line_color="rgb(51, 0, 102)", marker_line_width=3, opacity=.7)
    barfig.update_layout(title_text = "Popularity of Songs on Spotify", xaxis_title="Songs in order of Hot100 Ranking", yaxis_title="Popularity", yaxis=dict(range=[0, 100]))
    barfig.show()



    #Creates a bar chart comparing the song favorites on Twitter versus their ranking on the Hot 100 chart
    barfig1 = go.Figure([go.Bar(y=favorite_counts, x=song_name_list)])
    barfig1.update_traces(marker_color="rgb(0, 153, 153)", marker_line_color="rgb(153, 153, 255)", marker_line_width=3, opacity=.7)
    barfig1.update_layout(title_text = "Song Favorites on Twitter", xaxis_title="Songs in order of Hot100 Ranking", yaxis_title="Favorites on Twitter per Tweepy search (appx. 100 Tweets)", yaxis=dict(range=[0, 250]))
    barfig1.show()



if __name__ == "__main__":
    main()


