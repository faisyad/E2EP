import requests
from bs4 import BeautifulSoup
import pandas as pd
import datetime as dt
from datetime import timedelta, date
from pyspark.sql import SparkSession

def get_data():
    url = "https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page"
    result = requests.get(url)
    soap = BeautifulSoup(result.content, 'html.parser')
    for data in soap.findAll('div', {'id':'faq2023'})[0].findAll('ul'):
        link = data.findAll('li')[0].a['href']  #yellow tinggal ganti [0]
        data = requests.get(link)
        name = link.split('/')[-1]
        with open('/mnt/c/Users/user/Documents/Airflow/dags/entuen/yellowtaxi/'+name, 'wb')as file:
            file.write(data.content)

# def mergeAll():
#     spark2 = SparkSession.builder \
#         .appName('merge job') \
#         .getOrCreate()
    
#     path = "/user/faisyadd/endtoend"
#     datacsv = spark2.read.option("header", "true").csv(path)
#     datacsv = datacsv.coalesce(1)