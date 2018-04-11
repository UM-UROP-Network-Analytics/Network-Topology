#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
import numpy as np
import psycopg2

es = Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_trace-2018*"]
db_in = raw_input("Enter database name")
user_in = raw_input("Enter username")
pass_in = raw_input("Enter user password")
conn = psycopg2.connect(host="t3pers13.physics.lsa.umich.edu", database=db_in, user=user_in, password=pass_in)
cur = conn.cursor()
my_query = {}

# CHANGE SO IT RETREIVES ONLY UNIQUE hashes.

# sS='UC'
# srcSiteOWDServer = "192.170.227.160"
# srcSiteThroughputServer = "192.170.227.162"

sS='CERN-PROD'
srcSiteOWDServer = "128.142.223.247"
srcSiteThroughputServer = "128.142.223.246"

# dS='IU'
# destSiteOWDServer = "149.165.225.223"
# destSiteThroughputServer = "149.165.225.224"

# dS='UIUC'
# destSiteOWDServer = "72.36.96.4"
# destSiteThroughputServer = "72.36.96.9"

# dS='ICCN'
# destSiteOWDServer = "72.36.96.4"
# destSiteThroughputServer = "72.36.126.132"

dS='pic'
destSiteOWDServer = "193.109.172.188"
destSiteThroughputServer = "193.109.172.187"

now = datetime.utcnow()
curr_mon = now.month
curr_day = str(now.day)
curr_year = str(now.year)
curr_hr = str(now.hour)
curr_min = str(now.minute)
curr_sec = str(now.second)
end_date = curr_year + str(curr_mon) + curr_day + 'T' + curr_hr + curr_min + curr_sec + 'Z'
if curr_mon >= 4:
  curr_mon -= 3
else:
  if curr_mon is 3:
    curr_mon = 12
  if curr_mon is 2:
    curr_mon = 11
  if curr_mon is 1:
    curr_mon = 10
start_date = curr_year + str(curr_mon) + curr_day + 'T' + curr_hr + curr_min + curr_sec + 'Z'
#start_date = cur.execute("SELECT to_char(max(timestamp+interval '1 sec'),'YYYYMMDD\"T\"HHMISS\"Z\"') FROM rawtracedata")
my_src_query = {
    "size":1,
    "_source": {
        "include": [ 'src' ]
    },
    'query':{
        'bool':{
            'must':[
                {'range': {'timestamp': {'gte': start_date, 'lt': end_date}}},
                #{'term': {'_type': 'traceroute'}},
#                         {'bool':
#                             {'should':[
#                                 {'term': {'src': srcSiteOWDServer}},
#                                 {'term': {'src': srcSiteThroughputServer}},
#                                 {'term': {'src': destSiteOWDServer}},
#                                 {'term': {'src': destSiteThroughputServer}}
#                             ]}
#                         }
#                         ,
#                         {'bool':
#                             {'should':[
#                                 {'term': {'dest': destSiteOWDServer}},
#                                 {'term': {'dest': destSiteThroughputServer}},
#                                 {'term': {'dest': srcSiteOWDServer}},
#                                 {'term': {'dest': srcSiteThroughputServer}}
#                             ]}
#                         }
            ]

        }
    },
    "aggs": {
        "grouped_by_hash": {
          "terms": {  "field": "hash", "size":10000 }, #
          "aggs": {
              "top_hash_hits": {
                  "top_hits": {
                      "sort": [ { "_score": { "order": "desc" } } ],
                      "size": 1
                  }
              }
          }
       }
    }
}

src_results = es.search(body=my_src_query, index=my_index, request_timeout=12000)
    
src_dict = {}
src_data_size = len(src_results['aggregations']['grouped_by_hash']['buckets'])
src_lists = []
for i in range(0, src_data_size):
    rt_src = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src']
    if rt_src not in src_dict.keys():
        src_lists.append(rt_src)
        src_dict[rt_src] = 1

for x in range (0,len(src_lists)):
    my_query = {
        "size":1,
        "_source": {
            "include": [ 'src','dest','hops', 'n_hops', 'timestamp']
        },
        'query':{
            'bool':{
                'must':[
                    {'range': {'timestamp': {'gte': start_date, 'lt': end_date}}},
#                    {'term': {'_type': 'traceroute'}},
                             {'bool':
                                 {'should':[
                                     {'term': {'src': src_lists[x]}},
                                     #{'term': {'dest': '150.244.246.86'}},
#                                 {'term': {'src': srcSiteOWDServer}},
#                                 {'term': {'src': srcSiteThroughputServer}},
#                                 {'term': {'src': destSiteOWDServer}},
#                                 {'term': {'src': destSiteThroughputServer}}
                                 ]}
                             }
                        #,
                             #{'bool':
                                #{'should':[
                                  #  {'term': {'dest': '150.244.246.86'}},
#                                 {'term': {'dest': destSiteThroughputServer}},
#                                 {'term': {'dest': srcSiteOWDServer}},
#                                 {'term': {'dest': srcSiteThroughputServer}}
                            # ]}
                         #}
                ]

            }
        },
        "aggs": {
            "grouped_by_hash": {
              "terms": {  "field": "hash", "size":45000 }, #
              "aggs": {
                  "top_hash_hits": {
                      "top_hits": {
                          "sort": [ { "_score": { "order": "desc" } } ],
                          "size": 1
                      }
                  }
              }
           }
        }
    }
    results = es.search(body=my_query, index=my_index, request_timeout=100000)
    data_size = len(results['aggregations']['grouped_by_hash']['buckets'])

for i in range(0, data_size):
  rt_src = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src']
  rt_dest = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['dest']
  rt_hops = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['hops']
  rt_num_hops = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['n_hops']
  rt_ts = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['timestamp']
  cur.execute("INSERT INTO rawtracedata (src, dest, hops, n_hops, timestamp) VALUES (%s, %s, %s, %s, %s)", (rt_src, rt_dest, rt_hops, rt_num_hops, rt_ts))
  conn.commit()
cur.close()
conn.close()