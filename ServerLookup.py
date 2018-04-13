#!/usr/bin/env python3
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
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

now = datetime.utcnow()
curr_mon = now.month
curr_day = now.strftime("%d")
curr_year = now.strftime("%Y")
curr_hr = now.strftime("%H")
curr_min = now.strftime("%M")
curr_sec = now.strftime("%S")
end_date = curr_year + now.strftime("%m") + curr_day + 'T' + curr_hr + curr_min + curr_sec + 'Z'
cur.execute("SELECT * FROM rawtracedata limit 1")
if cur.fetchone() is None:
  start_date = '20180101T000000Z'
else:
  start_date = cur.execute("SELECT to_char(max(timestamp+interval '1 sec'),'YYYYMMDD\"T\"HHMISS\"Z\"') FROM rawtracedata")
my_src_query = {
    "size":1,
    "_source": {
        "include": [ 'src', 'dest', 'src_host', 'dest_host', 'src_site', 'dest_site' ]
    },
    'query':{
        'bool':{
            'must':[
                {'range': {'timestamp': {'gte': start_date, 'lt': end_date}}},
            ]

        }
    },
}

src_results = elasticsearch.helpers.scan(es, query=my_src_query, index=my_index, request_timeout=12000, size=1000)
for item in src_results:
	rt_src = item['_source']['src']
	rt_dest = item['_source']['dest']
	src_name = item['_source']['src_host']
	dest_name = item['_source']['dest_host']
	if 'src_site' in item['_source'].keys():
		src_site = item['_source']['src_site']
	else:
		src_site = 'missing'
	if 'dest_site' in item['_source'].keys():
		dest_site = item['_source']['dest_site']
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
	    		cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (src_name,))
	    		if not str(cur).upper().isupper():
	    			cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE domain = %s", (src_name, rt_src, src_name))
	    			conn.commit()
	    		cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (src_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (src_site, src_name))
	    			conn.commit()
	    else:
	    	cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (src_name,))
	    	if not str(cur).upper().isupper():
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE ipv6 = %s", (src_name, rt_src, rt_src))
	    		conn.commit()
	    	cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (src_name,))
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
	    		cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (src_name,))
	    		if not str(cur).upper().isupper():
	    			cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE domain = %s", (src_name, rt_src, src_name))
	    			conn.commit()
	    		cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (src_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (src_site, src_name))
	    			conn.commit()
	    else:
	    	cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (src_name,))
	    	if not str(cur).upper().isupper():
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE ipv4 = %s", (src_name, rt_src, rt_src))
	    		conn.commit()
	    	cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (src_name,))
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
	    		cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (dest_name,))
	    		if not str(cur).upper().isupper():
	    			cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE domain = %s", (dest_name, rt_dest, dest_name))
	    			conn.commit()
	    		cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (dest_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (dest_site, dest_name))
	    			conn.commit()
	    else:
	    	cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (dest_name,))
	    	if not str(cur).upper().isupper():
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv6 = %s WHERE ipv6 = %s", (dest_name, rt_dest, rt_dest))
	    		conn.commit()
	    	cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (dest_name,))
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
	    		cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (dest_name,))
	    		if not str(cur).upper().isupper():
	    			cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE domain = %s", (dest_name, rt_dest, dest_name))
	    			conn.commit()
	    		cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (dest_name,))
	    		if cur == 'missing':
	    			cur.execute("UPDATE serverlookup SET sitename = %s WHERE domain = %s", (dest_site, dest_name))
	    			conn.commit()
	    else:
	    	cur.execute("SELECT domain FROM serverlookup WHERE domain = %s", (dest_name,))
	    	if not str(cur).upper().isupper():
	    		cur.execute("UPDATE serverlookup SET domain = %s, ipv4 = %s WHERE ipv4 = %s", (dest_name, rt_dest, rt_dest))
	    		conn.commit()
	    	cur.execute("SELECT sitename FROM serverlookup WHERE domain = %s", (dest_name,))
	    	if cur == 'missing':
	    		cur.execute("UPDATE serverlookup SET sitename = %s WHERE ipv4 = %s", (dest_site, rt_dest))
	    		conn.commit()

cur.close()
conn.close()