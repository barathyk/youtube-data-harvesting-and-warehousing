# Youtube-Data-Harvesting-and-Warehousing
### Overview:
This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel,
stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and join tables to view data in the Streamlit app.

### Technologies Used:
+ Python (https://www.python.org/)
+ MongoDB (https://www.mongodb.com/)
+ MySQL (https://www.mysql.com/)
+ Streamlit (https://docs.streamlit.io/)
+ Pandas (https://pandas.pydata.org/docs/getting_started/intro_tutorials/index.html)
+ Plotly (https://plotly.com/)
+ Youtube Data API (https://developers.google.com/youtube/v3)

### Workflow:
+ Connect to the YouTube API this API is used to retrieve channel, videos, comments data. I have used the Google API client library for Python to make requests to the API.
+ The user will able to extract the Youtube channel's data using the Channel ID. Once the channel id is provided the data will be extracted using the API.
+ Once the data is retrieved from the YouTube API, I've stored it in a MongoDB as data lake. MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily.
+ After collected data for multiple channels,it is then migrated/transformed it to a structured MySQL as data warehouse.
+ Then used SQL queries to join the tables in the SQL data warehouse and retrieve data for specific channels based on user input.
+ With the help of SQL query I have got many interesting insights about the youtube channels.
+ Finally, the retrieved data is displayed in the Streamlit app. Also used Plotly's data visualization features to create charts and graphs to help users analyze the data.
+ Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing it in a MongoDB datalake, migrating it to a SQL data warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app.
