#!/usr/bin/env python3
#from elasticsearch import Elasticsearch
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
import numpy as np
import psycopg2

es = elasticsearch.Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_trace-2018*"]
db_in = raw_input("Enter database name ")
user_in = raw_input("Enter username ")
pass_in = raw_input("Enter user password ")
conn = psycopg2.connect(host="t3pers13.physics.lsa.umich.edu", database=db_in, user=user_in, password=pass_in)
cur = conn.cursor()
my_query = {}

now = datetime.utcnow()
curr_mon = now.month
curr_day = now.strftime("%d")
curr_year = now.strftime("%Y")
curr_hr = now.strftime("%H")
curr_min = now.strftime("%M")
curr_sec = now.strftime("%S")
end_date = curr_year + now.strftime("%m") + curr_day + 'T' + curr_hr + curr_min + curr_sec + 'Z'
if curr_mon >= 4:
  curr_mon -= 3
  curr_mon = '0' + str(curr_mon)
else:
  if curr_mon is 3:
    curr_mon = 12
  if curr_mon is 2:
    curr_mon = 11
  if curr_mon is 1:
    curr_mon = 10
cur.execute("SELECT * FROM rawtracedata limit 1")
if cur.fetchone() is None:
  start_date = '20180101T000000Z'
else:
  start_date = cur.execute("SELECT to_char(max(timestamp+interval '1 sec'),'YYYYMMDD\"T\"HHMISS\"Z\"') FROM rawtracedata")
my_query = {
    "size":1,
    "_source": {
        "include": [ 'src','dest','hops', 'n_hops', 'timestamp']
    },
    'query':{
        'bool':{
            'must':[
                {'range': {'timestamp': {'gte': start_date, 'lt': end_date}}},
            ]

        }
    },
}
results = elasticsearch.helpers.scan(es, query=my_query, index=my_index, request_timeout=100000, size=1000)
for item in results:
  rt_src = item['_source']['src']
  rt_dest = item['_source']['dest']
  rt_hops = item['_source']['hops']
  rt_num_hops = item['_source']['n_hops']
  rt_ts = item['_source']['timestamp']
  rt_ts = rt_ts / 1000
  format_ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(rt_ts))
  cur.execute("INSERT INTO rawtracedata (src, dest, hops, n_hops, timestamp) VALUES (%s, %s, %s, %s, %s)", (rt_src, rt_dest, rt_hops, rt_num_hops, format_ts))
  conn.commit()
cur.close()
conn.close()