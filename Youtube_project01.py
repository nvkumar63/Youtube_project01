from googleapiclient.discovery import build
import pandas as pd
import iso8601
import mysql.connector
import sqlalchemy
from sqlalchemy import create_engine, values
from datetime import datetime
import ssl
import requests
import re
import streamlit as st
from googleapiclient.errors import HttpError

#UCFzaYF8EQfaUUie1fnz8ojQ- i18nSolutions
#UCQaz4McWslVxjzjL8BmEnjA - nvkumar63
#UCf1d6qxLbFErWlET2Fe1new - I'm Keman
#http://localhost:8501
#streamlit run /Users/knk_macbookair/Downloads/Python/Capstone01/Youtube_project01.py

st.set_page_config(page_title='Capstone Project01')

# Initialize the YouTube API service
api_service_name = "youtube"
api_version = "v3"
api_key="AIzaSyBMs5lNxnT9xaGHrIo9FB0CI7-DjQB8KUg"

youtube = build('youtube', 'v3', developerKey=api_key)


# Function to fetch data from MySQL database
def fetch_data(query):
    mydb = mysql.connector.connect(
    host="localhost",
    user="root",
    password="",
    database="YouTube_Project1")

    mycursor = mydb.cursor()
    df = pd.read_sql(query, mydb)
    mydb.close()
    return df

# Function to execute predefined queries
def execute_query(question):
    query_mapping = {
        "1. What are the names of all the videos and their corresponding channels?": """
            SELECT Video_Name, Channel_Name from YouTube_Project1.Video ORDER BY Channel_Name DESC;
        """,
        "2. Which channels have the most number of videos, and how many videos do they have?": """
            SELECT Channel_Name, Total_Videos from YouTube_Project1.Channels ORDER BY Total_Videos DESC LIMIT 1;
        """,
        "3. What are the top 10 most viewed videos and their respective channels?": """
            SELECT View_Count, Video_Name, Channel_Name from YouTube_Project1.Video ORDER BY View_Count DESC LIMIT 10;
        """,
        "4. How many comments were made on each video, and what are their corresponding video names?": """
            SELECT Video_Name, Comment_Count from Video ORDER BY Comment_Count DESC;
        """,
        "5. Which videos have the highest number of likes, and what are their corresponding channel names?": """
            SELECT Like_Count, Video_Name, Channel_Name from YouTube_Project1.Video ORDER BY Like_Count DESC;
        """,
        "6. Which videos have the highest number of likes, and Favorite_Count what are their corresponding channel names?": """
            SELECT Like_Count, Favorite_Count, Video_Name from YouTube_Project1.Video ORDER BY Like_Count DESC LIMIT 20;
        """,
        "7. What is the total number of views for each channel, and what are their corresponding channel names?": """
            SELECT Channel_views, Channel_Name from YouTube_Project1.Channels GROUP BY Channel_views DESC;
        """,
        "8. What are the names of all the channels that have published videos in the year 2022?": """
            SELECT DISTINCT Channel_Name from YouTube_Project1.Video WHERE YEAR(Published_Date) = 2022 GROUP BY Channel_Name;
        """,
        "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?": """
            SELECT Channel_Name, AVG(Duration) from YouTube_Project1.Video GROUP BY Channel_Name;
        """,
        "10. Which videos have the highest number of comments, and what are their corresponding channel names?": """
            SELECT Comment_Count, Video_Name, Channel_Name from YouTube_Project1.Video ORDER BY Comment_Count DESC LIMIT 1;
        """
    }

    query = query_mapping.get(question)
    if query:
        return fetch_data(query)
    #else:

        #return pd.DataFrame()    

#Function to fetch channel data using YouTube API

#Function to fetch channel data using YouTube API

def fetch_channel_data(new_channel_id):
    try:
        # Check if the channel ID already exists in the database
        mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
        mycursor = mydb.cursor()
        mycursor.execute("SELECT * FROM Channels WHERE channel_id = %s", (new_channel_id,))
        existing_channel = mycursor.fetchone()
        mydb.close()

        if existing_channel:
            # Show error message if the channel ID already exists
            st.error("Channel ID already exists in the database.")
            return pd.DataFrame()
        

        request = youtube.channels().list(
            part="snippet,contentDetails,statistics,status",
            id=new_channel_id
        )
        response = request.execute()
        
        for item in response["items"]:

        # Parse the response and return relevant channel data
            data = dict(Channel_ID=item["id"],
                Channel_Name=item["snippet"]["title"], 
                Subscribes_Count=item['statistics']['subscriberCount'],
                Channel_views=item["statistics"]["viewCount"],
                Total_Videos=item["statistics"]["videoCount"],
                Channel_Description=item["snippet"]["description"],
                Channel_Status=item["status"]["privacyStatus"])

            channel_df=pd.DataFrame(data, index=[0])
            # CREATE the channel data into MySQL database
            mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
            mycursor = mydb.cursor()
            mycursor.execute("""CREATE TABLE if not exists Channels(Channel_Id VARCHAR(255) PRIMARY KEY, 
                                                                    Channel_Name VARCHAR(100),
                                                                    Subscribes_Count BIGINT,
                                                                    Total_Videos BIGINT,
                                                                    Channel_Description TEXT, 
                                                                    Channel_views BIGINT, 
                                                                    Channel_Status VARCHAR(255))""")
            mydb.commit()
            mydb.close()

            # Insert the channel data into MySQL database
            mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
            mycursor = mydb.cursor()
            for index,Channels in channel_df.iterrows():
                insert_query=("""INSERT INTO Channels (Channel_ID, Channel_Name, Subscribes_Count, Total_Videos, Channel_Description, Channel_views, Channel_Status)
                                    values(%s, %s, %s, %s, %s, %s, %s)""")
                values=(Channels['Channel_ID'], Channels['Channel_Name'], Channels['Subscribes_Count'], Channels['Total_Videos'], Channels['Channel_Description'], Channels['Channel_views'], Channels['Channel_Status'])
                mycursor.execute(insert_query,values)
                mydb.commit()
                mydb.close()

            return channel_df
        else:
            st.error("No items found in the response.")
            return pd.DataFrame()
    except HttpError as e:
        st.error(f"HTTP Error: {e}")
        return pd.DataFrame()
    except KeyError as e:
        st.error(f"KeyError: {e}. Please make sure the channel ID is correct.")
        return pd.DataFrame()

# Function to fetch video data using YouTube API
def get_video_Ids(channel_Id):
    get_video_ids=[]
    for new_channel_id in channel_Id:
        video_ids = []
        
        response=youtube.channels().list(id=new_channel_id,
                                    part='contentDetails').execute()
        #if 'items' in response and len(response['items']) > 0:
        Playlist_Id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']                             

        next_Page_Token=None

        while True:
            response1=youtube.playlistItems().list(
                                        part='snippet',
                                        playlistId=Playlist_Id,
                                        maxResults=100,
                                        pageToken=next_Page_Token).execute()
            for i in range(len(response1['items'])):
                video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])                                    
                next_Page_Token=response1.get('nextPageToken')

            if next_Page_Token is None:
                break

            else:
                st.error(f"No channels found for ID: {new_channel_id}")
        #except HttpError as e:
                #st.error(f"HTTP Error: {e}")
        #except KeyError as e:
                #st.error(f"KeyError: {e}")
    
    get_video_ids.extend(video_ids)   
    return get_video_ids

def iso8601_duration_to_seconds(duration):
    match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
    if not match:
        return None

    hours = int(match.group(1)) if match.group(1) else 0
    minutes = int(match.group(2)) if match.group(2) else 0
    seconds = int(match.group(3)) if match.group(3) else 0

    total_seconds = (hours * 3600) + (minutes * 60) + seconds
    return total_seconds




# Function to fetch video data using YouTube API
def get_the_video_info(video_ids):
        video_data=[]

        for video_id in video_ids:
                request = youtube.videos().list(
                        part = 'snippet,contentDetails,statistics,status,player,topicDetails',
                        id = video_id
                )

                response = request.execute()

                for item in response["items"]:
                        data = dict(Channel_Name=item['snippet']['channelTitle'],
                                Channel_ID=item['snippet']['channelId'],
                                Video_Id=item['id'],
                                Video_Name=item['snippet']['title'],
                                Video_Description=item['snippet']['description'],
                                Published_Date=item['snippet']['publishedAt'].replace('T'," ").replace('Z',""),
                                View_Count=item['statistics']['viewCount'],
                                Like_Count=item['statistics'].get('likeCount'),
                                Favorite_Count=item['statistics']['favoriteCount'],
                                Comment_Count=item['statistics'].get('commentCount'),
                                Duration=iso8601_duration_to_seconds(item['contentDetails']['duration']),
                                )
                        
                        video_data.append(data)
        
        
        # Create video data into MySQL database
        Video_df=pd.DataFrame(data, index=[0])                
        mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
        mycursor = mydb.cursor()
        mycursor.execute('''CREATE TABLE if not exists Video(Channel_Name VARCHAR(100),
                                                                Channel_ID VARCHAR(255) PRIMARY KEY,
                                                                Video_Id VARCHAR(255),
                                                                Video_Name VARCHAR(255),        
                                                                Video_Description TEXT(100), 
                                                                Published_Date DATETIME, 
                                                                View_Count INT(100), 
                                                                Like_Count INT(100), 
                                                                Favorite_Count INT(100), 
                                                                Comment_Count INT(100), 
                                                                Duration INT(255))''')
        mydb.commit()
        mydb.close()

        # Insert video data into MySQL database
        mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
        mycursor = mydb.cursor()
        for index,Video in Video_df.iterrows():
            insert_query=("""INSERT INTO Video (Channel_Name, Channel_ID, Video_Id, Video_Name, Video_Description, Published_Date, View_Count, Like_Count, Favorite_Count, Comment_Count, Duration)
                                values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""")
            values=(Video['Channel_Name'], Video['Channel_ID'], Video['Video_Id'], Video['Video_Name'], Video['Video_Description'], Video['Published_Date'], Video['View_Count'], Video['Like_Count'], Video['Favorite_Count'], Video['Comment_Count'], Video['Duration'])
            mycursor.execute(insert_query,values)
        mydb.commit()
        mydb.close()

        return pd.DataFrame(video_data)


# Function to fetch comment data using YouTube API

def fetch_comment_data(new_channel_id):
        comment_data = []
        get_video_ids = get_video_Ids([new_channel_id])
        for video_id in get_video_ids:
            next_page_token = None
            while True:
                try:
                    request_comments = youtube.commentThreads().list(
                        part="snippet",
                        videoId=video_id,
                        maxResults=100,
                        pageToken=next_page_token)
                    response_comments = request_comments.execute()

                    for comment in response_comments["items"]:
                        data = {
                            'comment_id': comment['snippet']['topLevelComment']['id'],
                            'video_id': comment['snippet']['topLevelComment']['snippet']['videoId'],
                            'channel_id': comment['snippet']['channelId'],
                            'author_name': comment['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            'text_display': comment['snippet']['topLevelComment']['snippet']['textDisplay'],
                            'published_date': comment['snippet']['topLevelComment']['snippet']['publishedAt'].replace('T'," ").replace('Z',"")
                        }
                        comment_data.append(data)

                    next_page_token = response_comments.get('nextPageToken')

                    if next_page_token is None:
                        break
                except HttpError as e:
                    if e.resp.status == 403:
                        st.warning(f"Comments are disabled for some videos in channel ID: {new_channel_id}")
                        break
                    else:
                        raise

                #Create Comment data into MySQL database
                Comments_df=pd.DataFrame(data, index=[0])
                mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
                mycursor = mydb.cursor()
                mycursor.execute('''CREATE TABLE if not exists Comments(Comment_Id VARCHAR(255) PRIMARY KEY, 
                                        Video_Id VARCHAR(100),
                                        Comment_Text TEXT, 
                                        Comment_Author VARCHAR(255), 
                                        Comment_Published DATETIME)''')       
                mydb.commit()
                mydb.close()

                # Insert comment data into MySQL database
                mydb = mysql.connector.connect(host="localhost", user="root", password="", database="YouTube_Project1")
                mycursor = mydb.cursor()
                for index,Comments in Comments_df.iterrows():
                    insert_query=("""INSERT INTO Comments (Comment_Id, Video_Id, Comment_Text, Comment_Author, Comment_Published)
                                        values(%s, %s, %s, %s, %s)
                    values=(Comments['Comment_Id'], Comments['Video_Id'], Comments['Comment_Text'], Comments['Comment_Author'], Comments['Comment_Published'])""")
                mycursor.execute(insert_query,values)                
                mydb.commit()
                mydb.close()

        return pd.DataFrame(comment_data)

def iso8601_duration_to_seconds(duration):
        match = re.match(r'^PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?$', duration)
        if not match:
            return None

        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0

        total_seconds = (hours * 3600) + (minutes * 60) + seconds
        return total_seconds



def main():
    st.title("YouTube Data Harvesting and Warehousing using SQL and Streamlit")
    st.sidebar.header("Tables")

    selected_option = st.sidebar.radio("Select Option", ("Channels", "Videos", "Comments", "Queries", "Enter YouTube Channel ID"))

    if selected_option == "Channels":
        st.header("Channels")
        channels_df = fetch_data("SELECT * FROM Channels;")
        channels_df.index += 1
        st.dataframe(channels_df)

    elif selected_option == "Videos":
        st.header("Videos")
        Video_df = fetch_data("SELECT * FROM Video;")
        Video_df.index += 1
        st.dataframe(Video_df)

    elif selected_option == "Comments":
        st.header("Comments")
        Comments_df = fetch_data("SELECT * FROM Comments;")
        Comments_df.index += 1
        st.dataframe(Comments_df)

    elif selected_option == "Queries":
        st.header("Queries")
        query_question = st.selectbox("Select Query", [
            "1. What are the names of all the videos and their corresponding channels?",
            "2. Which channels have the most number of videos, and how many videos do they have?",
            "3. What are the top 10 most viewed videos and their respective channels?",
            "4. How many comments were made on each video, and what are their corresponding video names?",
            "5. Which videos have the highest number of likes, and what are their corresponding channel names?",
            "6. Which videos have the highest number of likes, and Favorite_Count what are their corresponding channel names?",
            "7. What is the total number of views for each channel, and what are their corresponding channel names?",
            "8. What are the names of all the channels that have published videos in the year 2022?",
            "9. What is the average duration of all videos in each channel, and what are their corresponding channel names?",
            "10. Which videos have the highest number of comments, and what are their corresponding channel names?"
        ])
        if query_question:
            query_result_df = execute_query(query_question)
            st.write(query_result_df)
            query_result_df.index += 1
            st.dataframe(query_result_df)

    elif selected_option == "Enter YouTube Channel ID":
        st.header("Enter YouTube Channel ID")
        channel_id = st.text_input("Channel ID")

        if st.button("Fetch Channel Data") and channel_id:
            channel_df = fetch_channel_data(channel_id)
            channel_df.index+=1
            st.subheader("Channel Data")
            st.write(channel_df)

        if st.button("Fetch Video Data"):
            get_video_ids = get_video_Ids([channel_id])
            Video_df = get_the_video_info(get_video_ids)
            Video_df.index+=1
            st.subheader("Video Data")
            st.write(Video_df)

        if st.button("Fetch Comment Data"):
            Comments_df = fetch_comment_data([channel_id])
            Comments_df.index+1
            st.subheader("Comment Data")
            st.write(Comments_df)


if __name__ == "__main__":
    main()

