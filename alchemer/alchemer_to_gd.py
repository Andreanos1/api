import requests as r  # Importing requests library for making HTTP requests
import pandas as pd  # Importing pandas for data manipulation and analysis
import pandasql as psql  # Importing pandasql for SQL querying of pandas DataFrames
import gspread  # Importing gspread for Google Sheets interaction
from gspread_pandas import Spread  # Importing Spread for convenient handling of Google Sheets
from datetime import date  # Importing date from datetime module for date-related operations

# OAuth authentication for Google Sheets
gc = gspread.oauth()

# Opening Google Spreadsheet by URL and selecting worksheet
sh = gc.open_by_url('your_google_spreadsheet_url')  # Replace 'your_google_spreadsheet_url' with actual URL
worksheet = sh.worksheet("your_spreadsheet_sheet_name")  # Replace 'your_spreadsheet_sheet_name' with actual sheet name

# Initializing Spread object for Google Spreadsheet interaction
spread = Spread('your_google_spreadsheet_name')  # Replace 'your_google_spreadsheet_name' with actual spreadsheet name

# Fetching data from Google Spreadsheet
values_list = worksheet.col_values(2)  # Fetching data from the second column (B)
dttm = values_list[-1]  # Extracting the last timestamp from the fetched data
date_from = str(dttm[0:10]) + '+' + str(dttm[11:19])  # Formatting the timestamp for API request
cell = 'A' + str(len(values_list) + 1)  # Determining the cell to start writing new data

# Making API request to Alchemer to check for survey responses
check = r.get('https://api.alchemer.com/v5/survey/<survey_id>/surveyresponse?api_token=<token>&api_token_secret=<token_secret>&resultsperpage=1000&filter[field][0]=date_submitted&filter[operator][0]=>&filter[value][0]=' + date_from + '&filter[field][1]=status&filter[operator][1]==&filter[value][1]=Complete')

js = check.json()  # Parsing the JSON response from the API

pages_count = js['total_pages']  # Getting the total number of pages of survey responses
total_count = js['total_count']  # Getting the total count of survey responses

# Creating variables for iteration through pages
pages = list(range(1, pages_count + 1))

# Initializing an empty DataFrame to hold all survey responses
df = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)

# Iterating through each page of survey responses and appending them to the DataFrame
for page in pages:
    # Constructing the URL for API request based on page number and date filter
    url = ('https://api.alchemer.com/v5/survey/<survey_id>/surveyresponse?' + 
           'api_token=<token>&api_token_secret=<token_secret>' + 
           '&page=' + str(page) + 
           '&resultsperpage=500' + 
           '&filter[field][0]=date_submitted&filter[operator][0]=>=&filter[value][0]=' + date_from + 
           '&filter[field][1]=status&filter[operator][1]==&filter[value][1]=Complete')
    
    connection = r.get(url)  # Making the API request
    jason = connection.json()  # Parsing the JSON response
    
    df_from_page = pd.json_normalize(jason['data'])  # Creating DataFrame from the response data
    df = pd.concat([df, df_from_page])  # Appending the page data to the main DataFrame

# Selecting relevant columns from the DataFrame
df = df[['id', 'date_submitted', 'country', 'url_variables.ticket.value', 'survey_data.22.answer',
         'survey_data.28.answer', 'survey_data.31.answer', 'survey_data.30.answer']]

# Filtering the DataFrame to include only responses with a certain answer
df = df[df['survey_data.22.answer'] == 'Плохо']  # Filtering for responses where question 22 was answered 'Плохо'

# Extracting ticket number and agent from the URL variables
df['ticket number'] = df['url_variables.ticket.value'].str.split('_').str.get(0)
df['agent'] = df['url_variables.ticket.value'].str.split('_').str.get(1)

# Dropping the 'url_variables.ticket.value' column
df.drop('url_variables.ticket.value', axis=1, inplace=True)

# Reordering the columns in the DataFrame
df = df.reindex(columns=['id', 'date_submitted', 'country', 'ticket number', 'agent', 'survey_data.22.answer',
                         'survey_data.28.answer', 'survey_data.31.answer', 'survey_data.30.answer'])

# Adding a column with the current date
df['дата заливки'] = date.today()

# Writing the DataFrame to the Google Spreadsheet
spread.df_to_sheet(df, index=False, sheet=spread, start=cell, headers=False, replace=False)
