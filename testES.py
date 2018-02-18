#!/usr/bin/env python3
#%matplotlib inline
from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan
from datetime import datetime, timedelta
#import math
#import matplotlib.pyplot as plt
#import matplotlib.mlab as mlab
#from matplotlib import gridspec
import numpy as np
#import pandas as pd
import psycopg2

es = Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = "network_weather-2017.11.*"
print("set es")
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

start_date = '20171115T000000Z'
end_date = '20171115T002059Z'
src_lists = ['141.34.200.29','134.158.123.184','204.99.128.11']
src_to_dest = {}
print("set sources")
for x in range (0,len(src_lists)):
  print("loop "x)
    my_query = {
        "size":1,
        "_source": {
            "include": [ 'src','dest','hops','hash' ]
        },
        'query':{
            'bool':{
                'must':[
                    {'range': {'timestamp': {'gte': start_date, 'lt': end_date}}},
                    {'term': {'_type': 'traceroute'}},
                             {'bool':
                                 {'should':[
                                     {'term': {'src': src_lists[x]}}
#                                 {'term': {'src': srcSiteOWDServer}},
#                                 {'term': {'src': srcSiteThroughputServer}},
#                                 {'term': {'src': destSiteOWDServer}},
#                                 {'term': {'src': destSiteThroughputServer}}
                                 ]}
                             }
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
              "terms": {  "field": "hash", "size":25000 }, #
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
    results = es.search(body=my_query, index=my_index, request_timeout=12000)
    data_size = len(results['aggregations']['grouped_by_hash']['buckets'])
    print(data_size)

    for i in range(0, data_size):
        rt_src = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src']
        rt_src_host = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src_host']
        rt_dest = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['dest']
        rt_dest_host = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['dest_host']
        rt_hops = results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['hops']
        dupe_rt = 0
        if rt_src in src_to_dest.keys():
            if rt_dest in src_to_dest[rt_src].keys():
                current_rt = 'rt1'
                for x in range(0, len(src_to_dest[rt_src][rt_dest])):
                    if src_to_dest[rt_src][rt_dest][current_rt]['hop_list'] == rt_hops:
                        src_to_dest[rt_src][rt_dest][current_rt]['count'] += 1
                        dupe_rt = 1
                    current_rt = 'rt' + str(x+2)
                if (dupe_rt == 0):
                    src_to_dest[rt_src][rt_dest][current_rt] = {}
                    src_to_dest[rt_src][rt_dest][current_rt]['count'] = 1
                    src_to_dest[rt_src][rt_dest][current_rt]['hop_list'] = rt_hops
                    #cur.execute("INSERT INTO test1 (src, dest, rtnum, count, hops) VALUES (%s, %s, %s, %s, %s)", (rt_src, rt_dest, current_rt[2:], 1, rt_hops))
            else:
                src_to_dest[rt_src][rt_dest] = {'rt1':{}}
                src_to_dest[rt_src][rt_dest]['rt1']['count'] = 1
                src_to_dest[rt_src][rt_dest]['rt1']['hop_list'] = rt_hops
                #cur.execute("INSERT INTO test1 (src, dest, rtnum, count, hops) VALUES (%s, %s, %s, %s, %s)", (rt_src, rt_dest, 1, 1, rt_hops))
            dupe_rt = 0
    #completely new source
        else:
            src_to_dest[rt_src] = {rt_dest:{'rt1':{}}}
            src_to_dest[rt_src][rt_dest]['rt1']['count'] = 1
            src_to_dest[rt_src][rt_dest]['rt1']['hop_list'] = rt_hops  
            #cur.execute("INSERT INTO test1 (src, dest, rtnum, count, hops) VALUES (%s, %s, %s, %s, %s)", (rt_src, rt_dest, 1, 1, rt_hops))    
