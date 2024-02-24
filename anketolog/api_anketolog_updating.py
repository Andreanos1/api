import requests as r  # Importing requests library for making HTTP requests
import pandas as pd  # Importing pandas for data manipulation and analysis
import pandasql as psql  # Importing pandasql for SQL querying of pandas DataFrames
import gspread  # Importing gspread for Google Sheets interaction
import json  # Importing json for JSON parsing
import cx_Oracle  # Importing cx_Oracle for Oracle database connection
import warnings  # Importing warnings to handle warnings
from datetime import datetime, date  # Importing datetime for date-related operations
from sqlalchemy import create_engine  # Importing create_engine from sqlalchemy for database connection
from sqlalchemy.types import Integer, String, DateTime  # Importing necessary types from sqlalchemy for database schema definition
from gspread_pandas import Spread  # Importing Spread for convenient handling of Google Sheets

warnings.filterwarnings('ignore')  # Suppressing warnings

# Variables for Oracle connection
username = 'username'  # Replace 'username' with actual Oracle username
password = 'password'  # Replace 'password' with actual Oracle password
dsn = cx_Oracle.makedsn("host", 'port_number', service_name='service_name')  # Creating DSN for Oracle connection
or_conn = cx_Oracle.connect(username, password, dsn, encoding='UTF-8')  # Establishing connection to Oracle
curs_or = or_conn.cursor()  # Creating a cursor for Oracle connection

# Fetching last survey date from Oracle
sql1 = '''
select 
    max (start_date) as dttm
from anketolog.anketolog_surveys
'''
curs_or.execute(sql1)
data = curs_or.fetchall()
last_row = pd.DataFrame(data, columns=['dttm'])

# Processing last survey date
last_row['datetime'] = pd.to_datetime(last_row['dttm'], unit='s')
last_row['date'] = last_row['datetime'].dt.date
last_row['time'] = last_row['datetime'].dt.time
dt_from = last_row['date'][0]
tm_from = last_row['time'][0]
date_from = str(dt_from) + ' ' + str(tm_from)

api_key = 'api_key'  # API key for accessing Anketolog API
url = 'https://apiv2.anketolog.ru/survey/report/detail'  # URL for Anketolog API
offset = 0

# Setting up data for Anketolog API request
data = {
    "survey_id": 'your_survey_id',  # Replace 'your_survey_id' with actual survey ID
    "offset": offset,
    "date_from": date_from,
    #"date_to": "2023-07-22",
    "answer_type": "v",
    "options_with_answers_only": True,
    "dir": "DESC"
}
headers = {"X-Anketolog-ApiKey": api_key,
           "Content-Type": "application/json"}

# Sending request to Anketolog API
response = r.post(url, json=data, headers=headers)
response

# Parsing response from Anketolog API
surveys = response.json()
df_raw = pd.json_normalize(surveys, record_path='answers')

answers_count = surveys['answer_count']
offset = offset + len(surveys['answers'])
print(answers_count)

# Fetching all survey data iteratively
while offset < answers_count:
    data = {
        "survey_id": 'your_survey_id',
        "offset": offset,
        "date_from": date_from,
        "answer_type": "v",
        "options_with_answers_only": True,
        "dir": "DESC"}
    response = r.post(url, json=data, headers=headers)
    surveys = response.json()
    df = pd.json_normalize(surveys, record_path='answers')
    df_raw = df_raw.append(df)
    offset = offset + len(surveys['answers'])

# Data processing
df_main = df_raw.drop(['additional_params', 'answer'], axis=1)

# Extracting information from nested JSON structures
expanded_rows = []
for _, row in df_raw.iterrows():    
    answer_id = row['id']
    json_list_data = row['answer'] 
    df_expanded = pd.json_normalize(json_list_data, record_path=None, meta=answer_id)
    df_expanded['answer_id'] = answer_id
    expanded_rows.append(df_expanded)

df_answers = pd.concat(expanded_rows)

df_answers_red = df_answers.drop(['question_answer.answer.options'], axis=1)

df_main_asnwer_not_nan = df_answers.dropna(subset=['question_answer.answer.options'])

df_main_answer = pd.DataFrame()
for _, row in df_main_asnwer_not_nan.iterrows():    
    answer_id = row['answer_id']
    question_id = row['question_id']
    json_list_data = row['question_answer.answer.options']
    df_expanded = pd.DataFrame(json_list_data)
    df_expanded['answer_id'] = answer_id
    df_expanded['question_id'] = question_id
    df_main_answer = df_main_answer.append(df_expanded)

df_main_answer = df_main_answer[df_main_answer['answer_value'] == True]

expanded_rows2 = []
for _, row in df_raw.iterrows():    
    answer_id = row['id']
    json_list_data = row['additional_params']
    df_expanded = pd.json_normalize(json_list_data, record_path=None, meta=answer_id)
    df_expanded['answer_id'] = answer_id
    expanded_rows2.append(df_expanded)

df_params = pd.concat(expanded_rows2)
df_params = df_params.groupby(['answer_id', 'name'], as_index=False)['value'].first().pivot(index='answer_id', columns='name')
df_params.columns = [f'{col[1]}_{col[0]}' for col in df_params.columns]

# SQL query for data manipulation
q='''
select
    df_main.id,
    df_main.survey_id,
    df_main.start_date,
    df_main.finish_date,
    df_main.status,
    df_params.ownerid_value,
    df_params.ticket_value,
    df_answers_red.question_id,
    df_answers_red.question_name,
    df_answers_red."question_answer.answer.answer_text" as answer_text,
    df_main_answer.option_name
from df_main
left join df_params
    on df_main.id = df_params.answer_id
left join df_answers_red
    on df_main.id = df_answers_red.answer_id
left join df_main_answer
    on df_main.id = df_main_answer.answer_id
    and df_answers_red.question_id = df_main_answer.question_id
'''
final_df = psql.sqldf(q, locals())

# Database connection variables
DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
HOST = 'your_host'  # Enter the Oracle DB host URL
PORT = 'your_port_number'  # Enter the Oracle port number
SERVICE = 'your_server'  # Enter the Oracle DB service name
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + username + ':' + password +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

# Engine setup for database connection
engine = create_engine(ENGINE_PATH_WIN_AUTH)

# Writing DataFrame to database
final_df.to_sql('anketolog_surveys', engine, index=False,
          dtype={"id": Integer(),
                'survey_id': Integer(),
                'start_date': Integer(),
                'finish_date': Integer(),
                'status': String(10),
                'ownerid_value': Integer(),
                'ticket_value': String(100),
                'question_id': Integer(),
                'question_name': String(1000),
                'answer_text': String(2000),
                'option_name': String(1000)},
          if_exists="append")
