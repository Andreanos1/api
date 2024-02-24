import requests as r
import pandas as pd
import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, DateTime

#переменные для подключения к Ораклу
username = 'username'
password = 'password'
dsn = cx_Oracle.makedsn("host_name", 'port_number', 'server_name')
or_conn = cx_Oracle.connect(username,password, dsn, encoding = 'UTF-8')
curs_or = or_conn.cursor()

#смотрим на дату-время последней оценки в таблице
sql1 = '''
select 
    max (mscw_dttm) as dttm
from alchemer.new_surveys
'''
curs_or.execute(sql1)
data = curs_or.fetchall()
last_row = pd.DataFrame(data, columns = ['dttm'])

#превращаем последнюю дату-время в перменную, чтобы передать ее в запрос к API
last_row['dttm'] = last_row['dttm'] - pd.Timedelta("8 hour")
last_row['date'] = last_row['dttm'].dt.date
last_row['time'] = last_row['dttm'].dt.time
dt_from = last_row['date'][0]
tm_from = last_row['time'][0]
date_from = str(dt_from)+'+'+str(tm_from)
date_from

#Обращаемся к API и смотрим сколько там страниц и строк всего
check = r.get('https://api.alchemer.com/v5/survey/<survey_id>/surveyresponse?api_token=<token>&api_token_secret=<token_secret>&resultsperpage=1000&filter[field][0]=date_submitted&filter[operator][0]=>&filter[value][0]='+date_from+'&filter[field][1]=status&filter[operator][1]==&filter[value][1]=Complete')
js = check.json()
#print(js)
pages_count = js['total_pages']
total_count = js['total_count']
print('страниц всего: ' + str(pages_count))
print('строк всего: ' + str(total_count))

#создаем еще немного переменных
pages = list(range(1,pages_count+1))#лист с номерами страниц для цикла
df = pd.DataFrame(data=None, index=None, columns=None, dtype=None, copy=False)#пустой датафрейм (в него будут заливаться датафреймы со страниц алхмера)
#Создаем цикл FOR чтобы пробежаться по каждой странице и взять оттуда данные в датафрейм
for page in pages:
    url = ('https://api.alchemer.com/v5/survey/<survey_id>/surveyresponse?'+ #тут номер опросника по примеру из Алхемера
       'api_token=<api_token>&api_token_secret=<token_secret>'+ #тут токен+секретный токен (оч конфиденциальная инфа)
       '&page='+ str(page) + #тут номер страницы
       '&resultsperpage=500'+ #тут количество оценок на страницу (больше 500 не выводит...)
       '&filter[field][0]=date_submitted&filter[operator][0]=>=&filter[value][0]='+ date_from + #тут дата с которой начинаем выгружать оценки
       '&filter[field][1]=status&filter[operator][1]==&filter[value][1]=Complete') #завершение, лучше прост не трогать особо
    connection = r.get(url)#подключаемся
    jason = connection.json()#сонвертируем в джейсон
    df_from_page = pd.json_normalize(jason['data'])#создаем датафрейм со страницы
    df = pd.concat([df,df_from_page])#заливаем в общий датафрейм
df.info()

df = df[['id','status','date_submitted','country','city','url_variables.ticket.value','survey_data.22.answer']]#убираем лишние колонки

#немного преобразований чтобы все было красиво
df[['ticket_id', 'agent_name']] = df['url_variables.ticket.value']. str.split('_', 1 , expand= True )
df[['date', 'time','zone']] = df['date_submitted']. str.split(' ', 2 , expand= True )
df["time"] = df['time'] + pd.Timedelta("8 hour")
df['mscw_dttm'] = df['date'].astype("datetime64") + df['time']
df1 = df.drop(['url_variables.ticket.value','date_submitted','date','time','zone'], axis=1)
df1 = df1.rename(columns={'survey_data.22.answer': "survey"})
df1.head()


#переменные для движка загрузки в бд (не заморачивался и скопировал у Даши)
DIALECT = 'oracle'
SQL_DRIVER = 'cx_oracle'
HOST = 'host' #enter the oracle db host url
PORT = 'port_number' # enter the oracle port number
SERVICE = 'service_name'  # enter the oracle db service name
ENGINE_PATH_WIN_AUTH = DIALECT + '+' + SQL_DRIVER + '://' + username + ':' + password +'@' + HOST + ':' + str(PORT) + '/?service_name=' + SERVICE

#движек для заливки
engine = create_engine(ENGINE_PATH_WIN_AUTH)
#заливка
df1.to_sql('new_surveys', engine, index=False,
          dtype={"id": Integer(),
                'status': String(100),
                'country': String(100),
                'city': String(100),
                'survey': String(100),
                'ticket_id': String(100),
                'agent_name': String(100),
                'mscw_dttm': DateTime()},
          if_exists="append")