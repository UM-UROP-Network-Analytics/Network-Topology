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
start_date = '20180104T000000Z'
end_date = '20180404T000000Z'
my_src_query = {
    "size":1,
    "_source": {
        "include": [ 'src', 'dest', 'src_host', 'dest_host', 'src_site', 'dest_site' ]
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
src_data_size = len(src_results['aggregations']['grouped_by_hash']['buckets'])
src_lists = []
for i in range(0, src_data_size):
	rt_src = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src']
	rt_dest = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['dest']
	src_name = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src_host']
	dest_name = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['dest_host']
	if 'src_site' in src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source'].keys():
		src_site = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['src_site']
	else:
		src_site = 'missing'
	if 'dest_site' in src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source'].keys():
		dest_site = src_results['aggregations']['grouped_by_hash']['buckets'][i]['top_hash_hits']['hits']['hits'][0]['_source']['dest_site']
	else:
		dest_site = 'missing'
	if ':' in rt_src:
	    cur.execute("SELECT ipv6 FROM serverlookup WHERE ipv6 = (%s)", (rt_src,))
	    if cur.fetchone() is None:
	    	cur.execute("SELECT domain FROM serverlookup WHERE domain = (%s)", (src_name,))
	    	if cur.fetchone() is None:
	    		cur.execute("INSERT INTO serverlookup (domain, ipv6, sitename) VALUES (%s, %s, %s)", (src_name, rt_src, src_site))
	    		conn.commit()
	    	else:
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE domain = %s", (src_name, rt_src, src_name))
	    		conn.commit()
	    		cur.execute("SELECT FROM serverlookup WHERE domain = %s", (src_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (src_site, src_name))
	    			conn.commit()
	    else:
	    	cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE ipv6 = %s", (src_name, rt_src, rt_src))
	    	conn.commit()
	    	cur.execute("SELECT FROM serverlookup WHERE domain = %s", (src_name,))
	    	if cur == 'missing':
	    		cur.execute("UPDATE serverlookup SET sitename = %s WHERE ipv6 = %s", (src_site, rt_src))
	    		conn.commit()
	else:
	    cur.execute("SELECT ipv4 FROM serverlookup WHERE ipv4 = (%s)", (rt_src,))
	    if cur.fetchone() is None:
	    	cur.execute("SELECT ipv6 FROM serverlookup WHERE domain = (%s)", (src_name,))
	    	if cur.fetchone() is None:
	    		cur.execute("INSERT INTO serverlookup (domain, ipv4, sitename) VALUES (%s, %s, %s)", (src_name, rt_src, src_site))
	    		conn.commit()
	    	else:
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE domain = %s", (src_name, rt_src, src_name))
	    		conn.commit()
	    		cur.execute("SELECT FROM serverlookup WHERE domain = %s", (src_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (src_site, src_name))
	    			conn.commit()
	    else:
	    	cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE ipv4 = %s", (src_name, rt_src, rt_src))
	    	conn.commit()
	    	cur.execute("SELECT FROM serverlookup WHERE domain = %s", (src_name,))
	    	if cur == 'missing':
	    		cur.execute("UPDATE serverlookup SET sitename = %s WHERE ipv4 = %s", (src_site, rt_src))
	    		conn.commit()
	if ':' in rt_dest:
	    cur.execute("SELECT ipv6 FROM serverlookup WHERE ipv6 = (%s)", (rt_dest,))
	    if cur.fetchone() is None:
	    	cur.execute("SELECT ipv4 FROM serverlookup WHERE domain = (%s)", (dest_name,))
	    	if cur.fetchone() is None:
	    		cur.execute("INSERT INTO serverlookup (domain, ipv6, sitename) VALUES (%s, %s, %s)", (dest_name, rt_dest, dest_site))
	    		conn.commit()
	    	else:
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE domain = %s", (dest_name, rt_dest, dest_name))
	    		conn.commit()
	    		cur.execute("SELECT FROM serverlookup WHERE domain = %s", (dest_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (dest_site, dest_name))
	    			conn.commit()
	    else:
	    	cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE ipv6 = %s", (dest_name, rt_dest, rt_dest))
	    	conn.commit()
	    	cur.execute("SELECT FROM serverlookup WHERE domain = %s", (dest_name,))
	    	if cur == 'missing':
	    		cur.execute("UPDATE serverlookup SET sitename = %s WHERE ipv6 = %s", (dest_site, rt_dest))
	    		conn.commit()
	else:
	    cur.execute("SELECT ipv4 FROM serverlookup WHERE ipv4 = (%s)", (rt_dest,))
	    if cur.fetchone() is None:
	    	cur.execute("SELECT ipv6 FROM serverlookup WHERE domain = (%s)", (dest_name,))
	    	if cur.fetchone() is None:
	    		cur.execute("INSERT INTO serverlookup (domain, ipv4, sitename) VALUES (%s, %s, %s)", (dest_name, rt_dest, dest_site))
	    		conn.commit()
	    	else:
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE domain = %s", (dest_name, rt_dest, dest_name))
	    		conn.commit()
	    		cur.execute("SELECT FROM serverlookup WHERE domain = %s", (dest_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (dest_site, dest_name))
	    			conn.commit()
	    else:
	    	cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE ipv4 = %s", (dest_name, rt_dest, rt_dest))
	    	conn.commit()
	    	cur.execute("SELECT FROM serverlookup WHERE domain = %s", (dest_name,))
	    	if cur == 'missing':
	    		cur.execute("UPDATE serverlookup SET sitename = %s WHERE ipv4 = %s", (dest_site, rt_dest))
	    		conn.commit()

cur.close()
conn.close()