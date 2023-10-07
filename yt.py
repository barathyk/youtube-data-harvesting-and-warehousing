import pymongo
from googleapiclient.discovery import build
import mysql.connector
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu
import re
import plotly.express as px


# streamlit page 
st.set_page_config(page_title="youtube Data Harvesting",
                   layout="wide",
                   initial_sidebar_state="expanded")


#option page
selected = option_menu(menu_title="Data Harvesting And Warehousing ",
                       options=["HOME","DATA EXTRACTION","DATA MIGRATION","DATA ANALYSIS"],
                       icons=["house","cloud-arrow-down","cloud-upload","file-earmark-bar-graph"],
                       orientation='horizontal',)
    
#mongo connection
myclient=pymongo.MongoClient("mongodb://127.0.0.1:27017/")
mydb=myclient.youtube

#connect to mysql connector
mycon=mysql.connector.connect(host='localhost',
                              user='root',
                              password='Barathy@06',
                              database='youtube'
)
mycursor=mycon.cursor(buffered=True)

#youtube api connection
api_key ='AIzaSyDaEsJoCohdyZr3XckwjCin370Jlde5N6o'
youtube =build('youtube','v3',developerKey=api_key)


#function to get channel datas
def channel_details(channel_id):
    
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id)
    response = request.execute()
    
 
    channel_info =dict(channel_name=response['items'][0]['snippet']['title'],
                channel_id=response['items'][0]['id'],
                description=response['items'][0]['snippet']['description'],
                subcriber_count=response['items'][0]['statistics']['subscriberCount'],
                total_videos=response['items'][0]['statistics']['videoCount'],
                total_views=response['items'][0]['statistics']['viewCount'],
                playlist_id=response['items'][0]['contentDetails']['relatedPlaylists']['uploads'])
    return channel_info

#function to get playlist id
def playlistid(channel_id):
    
    request = youtube.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id)
    response = request.execute()
    
    playlist_id =response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    
    return playlist_id


#function to video ids from the channel using playlist id
def playlist_details(channel_id):
    
    video_ids=[]
    
    request = youtube.playlistItems().list(
        part='contentDetails,snippet,id',
        maxResults=50,
        playlistId=playlistid(channel_id))
    response = request.execute()
    
    while response:
        for item in response['items']:
            video_info=item['contentDetails']['videoId']
                            # playlist_id=item['snippet']['playlistId'],
                            # channel_id=item['snippet']['channelId'],
                            # channel_name=item['snippet']['channelTitle'])
            
            video_ids.append(video_info)
            
        if 'nextPageToken' in response:
            request = youtube.playlistItems().list(
                    part='contentDetails,snippet,id',
                    maxResults=50,
                    playlistId=playlistid(channel_id),
                    pageToken=response['nextPageToken'])
            response = request.execute()
                
        else:
            break
    
    return video_ids

#function to get video data using video id
def video_details(v_ids):
    
    video_detail=[]
    for id in v_ids:
        request =youtube.videos().list(
                    part ='snippet,contentDetails,statistics',
                    id =id)
        response=request.execute()

        for video in response['items']:
            video_info=dict( Title=video['snippet']['title'],
                             video_id=video['id'],
                             channel_id=video['snippet']['channelId'],
                             channel_name=video['snippet']['channelTitle'],
                             published_date=video['snippet']['publishedAt'],
                             duration=video['contentDetails'].get('duration'),
                             view_count=video['statistics']['viewCount'],
                             like_count=video['statistics'].get('likeCount'),
                             comments_count=video['statistics'].get('commentCount') )

            video_detail.append(video_info)

    return pd.DataFrame(video_detail)

def get_duration(v_ids):
    
    duration_list=[]
    for i in v_ids:
        request =youtube.videos().list(
                    part ='snippet,contentDetails,statistics',
                    id =i)
        response=request.execute()
        
        for video in response['items']:
            durations=video['contentDetails'].get('duration')
            duration_list.append(durations)
            
    return duration_list

def convert_duration(duration):
    
    duration_sec=[]
    for i in duration:
        duration_pattern = re.compile(r'PT(\d+H)?(\d+M)?(\d+S)?')
        match = duration_pattern.match(i)

        if match:
            hours = int(match.group(1)[:-1]) if match.group(1) else 0
            print(hours)
            minutes = int(match.group(2)[:-1]) if match.group(2) else 0
            print(minutes)
            seconds = int(match.group(3)[:-1]) if match.group(3) else 0
            print(seconds)

            total_seconds = (hours * 3600) + (minutes * 60) + seconds
            duration_sec.append(total_seconds)
    return duration_sec



#function to get comment data using video ids
def comment_details(v_ids):
    comment_detail=[]
    try:
        for i in v_ids:
            request = youtube.commentThreads().list(
                        part="snippet,replies",
                        videoId=i,
                        maxResults=20)
            response = request.execute()

            
            for comment in response['items']:  
                comment_info=dict(video_id=comment['snippet'].get('videoId','disabled'),
                                comments_Id=comment['snippet']['topLevelComment'].get('id','disabled'),
                                comments_Text=comment['snippet']['topLevelComment']['snippet'].get('textDisplay','disabled'),
                                comments_Author=comment['snippet']['topLevelComment']['snippet'].get('authorDisplayName','disabled'),
                                comments_PublishedAt=comment['snippet']['topLevelComment']['snippet'].get('publishedAt','disabled'))
                comment_detail.append(comment_info)
        
    except:
        pass
            
    return pd.DataFrame(comment_detail)

# function to get the channel names from mongoDB to start transfer to mysql
def channel_names():
    ch_name =[]
    for i in mydb.channel_details.find():
        ch_name.append(i['channel_name'])
    return ch_name

if selected == "HOME":
   st.write("Explanation about the project:")
     


if selected =="DATA EXTRACTION":
   st.write(" Enter youtube channel id below :")
   channel_id = st.text_input('Enter Channel Id', )
   scrape= st.button("Submit")
    
   if scrape:
        ch_data = channel_details(channel_id)
        v_ids = playlist_details(channel_id)
        v_data = video_details(v_ids)
        v_data['published_date'] =pd.to_datetime(v_data['published_date'])
        duration=get_duration(v_ids)
        d=convert_duration(duration)
        v_data['convert_duration']=d
        cmnt = comment_details(v_ids)
        cmnt['comments_PublishedAt'] = pd.to_datetime(cmnt['comments_PublishedAt'])


        v_dict =v_data.to_dict(orient='records')
        cmnt_dict =cmnt.to_dict(orient='records')

        col1 = mydb.channel_details
        col1.insert_one(ch_data)

        col2 = mydb.video_details
        col2.insert_many(v_dict)

        col3 =mydb.comment_details
        col3.insert_many(cmnt_dict)

        st.success("Upload to MongoDB successfull")              


if selected == "DATA MIGRATION":
    st.markdown('Select a channel name to tranform into mysql database  ')
    
    ch_names = channel_names()
    user_input = st.selectbox('Select Channel',options=ch_names)
    
    def insert_channels():
        for i in mydb.channel_details.find({'channel_name':user_input},{'_id':0}):
            insert_ch=tuple(i.values())
            query1 ="insert into channels values(%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(query1,insert_ch)
            mycon.commit()

    def insert_videos():
        for i in mydb.video_details.find({'channel_name':user_input},{'_id':0}):
            insert_v=tuple(i.values())
            query2 ="insert into videos values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            mycursor.execute(query2,insert_v)
            mycon.commit()

    def insert_comments():
        for vid in mydb.video_details.find({'channel_name':user_input},{'_id':0}):
            for i in mydb.comment_details.find({'video_id':vid['video_id']},{'_id':0}):
                insert_cmt=tuple(i.values())
                query3 ='insert into comments values(%s,%s,%s,%s,%s)'
                mycursor.execute(query3,insert_cmt)
                mycon.commit()

    if st.button('submit'):
        try:
            insert_channels()
            insert_videos()
            insert_comments()
            st.success('Transform to mysql completed successfully')
        except:
            st.error('Transform already done')

if selected =="DATA ANALYSIS":
    st.write("select any question to get informations about the Channels,Videos and their Comments")
    questions = st.radio('Questions',
    ['1.What are the names of all the videos and their corresponding channels?',
    '2.Which channels have the most number of videos and how many videos do they have?',
    '3.What are the top 10 most viewed videos and their respective channels?',
    '4.How many comments were made on each video and what are their corresponding channel names?',
    '5.Which videos have the highest number of likes and what are their corresponding channel names?',
    '6.What is the total number of likes for each video and what are their corresponding video names?',
    '7.What is the total number of views for each channel and what are their corresponding channel names?',
    '8.What are the names of all the channels that have published videos in the year 2022?',
    '9.What is the average duration of all videos in each channel and what are their corresponding channel names?',
    '10.which videos have the highest number of comments and what are their corresponding channel names?'])


    if questions == '1.What are the names of all the videos and their corresponding channels?':
        qry1 ="select Title ,channel_name from videos order by channel_name"
        mycursor.execute(qry1)
        mycon.commit()
        df = pd.DataFrame(mycursor.fetchall(),columns=['Title','channel name'])
        st.write(df)

    elif questions ==  '2.Which channels have the most number of videos and how many videos do they have?':
         qry2 ="select channel_name ,total_videos from channels order by total_videos desc"
         mycursor.execute(qry2)
         mycon.commit()
         df =pd.DataFrame(mycursor.fetchall(),columns=['channel','total'])
         st.write(df)
         fig =px.bar(df,title='Channels & their videos' ,x='channel',y='total',orientation='h',color='total',
                     color_discrete_sequence=px.colors.qualitative.Dark24
                    )
    
         st.plotly_chart(fig,use_container_width=True)

    elif questions == '3.What are the top 10 most viewed videos and their respective channels?':
            qry3 ="select channel_name ,Title ,view_count from videos order by view_count desc limit 10"
            mycursor.execute(qry3)
            mycon.commit()
            df = pd.DataFrame(mycursor.fetchall(),columns=['channel','title','views'])
            st.write(df)
            fig =px.bar(df,title='Top 10 viewed videos',x='title',y='views',orientation='h',color='title',
                        color_discrete_sequence=px.colors.qualitative.Vivid)
            st.plotly_chart(fig,use_container_width=True)
        
    elif questions == '4.How many comments were made on each video and what are their corresponding video names?':
            qry4 """select a.video_id as video_id, a.title as Title,b.total_comments from videos
                    as a left join (select video_id ,count(comments_id) as total_comments from comments group by video_id) as b 
                    on a.video_id = b.video_id order by b.total_comments  desc;"""
            mycursor.execute(qry4)
            mycon.commit()
            df =pd.DataFrame(mycursor.fetchall(),columns=mycursor.column_names)
            st.write(df)
            fig =px.bar(df,x=mycursor.column_names[2],y=mycursor.column_names[1],orientation='h',color=mycursor.column_names[0])
            st.plotly_chart(fig,use_container_width=True)

    elif questions == '5.Which videos have the highest number of likes and what are their corresponding channel names?':
            qry5 ="select channel_name ,Title ,like_count from videos order by like_count desc limit 10"
            mycursor.execute(qry5)
            mycon.commit()
            df =pd.DataFrame(mycursor.fetchall(),columns=['Channel','Title','Likes',])
            st.write(df)
            fig =px.bar(df,title='Highest number of likes',x='Title',y='Likes',orientation='v',color='Channel',
                        color_discrete_sequence=px.colors.qualitative.Dark24)
            st.plotly_chart(fig,use_container_width=True)
            

    elif questions == '6.What is the total number of likes for each video and what are their corresponding video names?':
            qry6 ="select Title ,like_count from videos order by like_count desc"
            mycursor.execute(qry6)
            mycon.commit()
            df =pd.DataFrame(mycursor.fetchall(),columns=['Title','Likes'])
            st.write(df)
            fig =px.scatter(df,x='Likes',y='Title',orientation='v',color='Title',
                            color_discrete_sequence=px.colors.qualitative.G10_r)
            st.plotly_chart(fig,use_container_width=True)

    elif questions =='7.What is the total number of views for each channel and what are their corresponding channel names?':
            qry7 ="select channel_name ,total_views from channels order by total_views desc"
            mycursor.execute(qry7)
            mycon.commit()
            df =pd.DataFrame(mycursor.fetchall(),columns=['channel','views',])
            st.write(df)
            fig =px.bar(df,title='Total Views of Channel',x='channel',y='views',orientation='v',color='views',
                              color_discrete_sequence=px.colors.qualitative.Plotly)
            st.plotly_chart(fig,use_container_width=True)

    elif questions == '8.What are the names of all the channels that have published videos in the year 2022?':
            qry8 ="select channel_name  from videos where published_date like '2022%' group by channel_name order by channel_name"
            mycursor.execute(qry8)
            mycon.commit()
            df =pd.DataFrame(mycursor.fetchall(),columns=['channel'])
            st.write(df)
            fig =px.bar(df,title='Channels that published videos in year 2022',x='channel',orientation='v',color='channel',
                        color_discrete_sequence=px.colors.qualitative.G10)
            st.plotly_chart(fig,use_container_width=True)


    elif questions == '9.What is the average duration of all videos in each channel and what are their corresponding channel names?':
             qry9 = " select channel_name ,AVG(convert_duration)/60 as 'Average_video_duration' from videos group by channel_name order by AVG(convert_duration)/60 "
             mycursor.execute(qry9)
             mycon.commit()
             df =pd.DataFrame(mycursor.fetchall(),columns=['channel','Average duration'])
             st.write(df)
             fig =px.bar(df,title='Average video time in minutes',x='channel',y='Average duration',orientation='v',color='channel',
                         color_discrete_sequence=px.colors.qualitative.Dark24_r)
             st.plotly_chart(fig,use_container_width=True)


    elif questions =='10.which videos have the highest number of comments and what are their corresponding channel names?':
            qry10 ="select channel_name, Title ,comments_count from videos order by video_id  desc"
            mycursor.execute(qry10)
            mycon.commit()
            df =pd.DataFrame(mycursor.fetchall(),columns=['channel','Title','Comments'])
            st.write(df)
            fig =px.line(df,title='Highest number of comments',x='Comments',y='Title',orientation='v',color='Title',
                         color_discrete_sequence=px.colors.sequential.Plasma_r)
            st.plotly_chart(fig,use_container_width=True)
