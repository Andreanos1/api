import requests as r  # Importing requests library for making HTTP requests
import pandas as pd  # Importing pandas for data manipulation and analysis
import pandasql as psql  # Importing pandasql for SQL querying of pandas DataFrames
import json  # Importing json for JSON parsing

url = 'https://cloud-api.yandex.net/v1/disk/resources/download?path=test_table.xlsx'  # URL for downloading file from Yandex Disk
headers = {
    'Accept': 'application/json',
    'Authorization': 'your_api_token'  # Replace 'your_api_token' with actual Yandex Disk API token
}

response = r.get(url, headers=headers)  # Sending GET request to Yandex Disk API

# Printing status code and response data
print('Status Code:', response.status_code)
print('Response Data:', response.json())

text = response.json()
download_url = text['href']  # Extracting download URL from response

destination = r'C:\Users\Downloads\test_table2.xlsx'  # Destination path for downloaded file
response = r.get(download_url)  # Sending GET request to download the file

# Checking if the request was successful
if response.status_code == 200:
    with open(destination, 'wb') as file:
        file.write(response.content)  # Writing file content to the destination
    print('File downloaded successfully')
else:
    print('Failed to download the file. Status code:', response.status_code)
