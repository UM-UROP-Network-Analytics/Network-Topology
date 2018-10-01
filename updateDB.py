#!/usr/bin/python
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
import numpy as np
from psycopg2-binary import psycopg2-binary
from psycopg2-binary import IntegrityError
from config import config
import os
import os.path 
from pathlib import Path

#checks to see if this process is currently running
lock_file = Path("/var/lock/updateDB")
if lock_file.is_file():
  print('Error: process already running - Check /var/lock/updateDB')
  print 'This error was detected at ' + str(datetime.now())
  quit()
else:
  print 'Starting to create lock at ' + str(datetime.now())
  lk_file = open('/var/lock/updateDB', "w")
  print 'Lock created at ' + str(datetime.now()) 
  lk_file.close()
  print 'File closed at ' + str(datetime.now())

#connect to the database
es = elasticsearch.Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_trace-2018*"]
params = config()
conn = psycopg2.connect(**params)
cur = conn.cursor()
my_query = {}
print 'Database connection established at ' + str(datetime.now())

#determine start and end times
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
  cur.execute("SELECT to_char(max(timestamp+interval '1 sec'),'YYYYMMDD\"T\"HHMISS\"Z\"') FROM rawtracedata")
  start_date = cur.fetchone()[0]
print 'Dates set at ' + str(datetime.now())

#build and run the query
my_query = {
    "size":1,
    "_source": {
        "include": [ 'src','dest','hops', 'n_hops', 'timestamp', 'src_host', 'dest_host', 'src_site', 'dest_site']
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
print 'Results compiled at ' + str(datetime.now())

#updates the raw traceroute data table
def updateRaw( item ):
    rt_src = item['_source']['src']
    rt_dest = item['_source']['dest']
    rt_hops = item['_source']['hops']
    rt_num_hops = item['_source']['n_hops']
    rt_ts = item['_source']['timestamp']
    rt_ts = rt_ts / 1000
    format_ts = time.strftime("%Y-%m-%d %H:%M:%S-0000", time.gmtime(rt_ts))
    try:
        cur.execute("INSERT INTO rawtracedata (src, dest, hops, n_hops, timestamp) VALUES (%s, %s, %s, %s, %s)", (rt_src, rt_dest, rt_hops, rt_num_hops, format_ts))
        conn.commit()
    except IntegrityError:
        conn.rollback()
        pass

#updates the server lookup table
def updateLookup ( item ):
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
        cur.execute("UPDATE serverlookup SET bandwidth = %s WHERE ipv6 = %s", ('1', rt_src))
        conn.commit()
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
        cur.execute("UPDATE serverlookup SET bandwidth = %s WHERE ipv4 = %s", ('1', rt_src))
        conn.commit()
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

#updates the unique count table as well as the summary table
def updateSummary( item ):
    rt_src = item['_source']['src']
    rt_dest = item['_source']['dest']
    rt_hops = item['_source']['hops']
    rt_num_hops = item['_source']['n_hops']
    rt_ts = item['_source']['timestamp']
    rt_ts = rt_ts / 1000
    format_ts = time.strftime("%Y-%m-%d %H:%M:%S-0000", time.gmtime(rt_ts))
    if rt_hops is not None:
        if any(x is None for x in rt_hops):
            rt_hops = ['None' if v is None else v for v in rt_hops]
        my_hops = '{' + ','.join(rt_hops) + '}'
        if rt_num_hops >= 1:
            if rt_hops[rt_num_hops-1] == rt_dest:
                try:
                    cur.execute("SELECT max(rtnum) FROM traceroute WHERE src = %s AND dest =%s", (rt_src, rt_dest))
                    last_rt = cur.fetchone()[0]
                    if last_rt is None:
                        last_rt = 0
                    cur.execute("INSERT INTO traceroute (src, dest, hops, cnt, n_hops, rtnum, min_ts, max_ts) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)", (rt_src, rt_dest, rt_hops, 1, rt_num_hops, last_rt+1, format_ts, format_ts))
                    conn.commit()
                    try:
                        cur.execute("INSERT INTO routesummary (src, dest, count) VALUES (%s, %s, %s)", (rt_src, rt_dest, 1))
                        conn.commit()
                    except IntegrityError:
                        conn.rollback()
                        cur.execute("SELECT count FROM routesummary WHERE src = %s AND dest = %s", (rt_src, rt_dest))
                        fullcount = cur.fetchone()[0]
                        if fullcount is None:
                            cur.execute("UPDATE routesummary SET count = %s WHERE src = %s AND dest = %s", (1, rt_src, rt_dest))
                            conn.commit()
                        else:
                            cur.execute("UPDATE routesummary SET count = %s WHERE src = %s AND dest = %s", (fullcount+1, rt_src, rt_dest))
                            conn.commit()
                except IntegrityError:
                    conn.rollback()
                    cur.execute("SELECT cnt FROM traceroute WHERE src = %s AND dest = %s AND hops = %s", (rt_src, rt_dest, my_hops))
                    current_count = cur.fetchone()[0]
                    cur.execute("UPDATE traceroute SET cnt = %s, max_ts = %s WHERE src = %s AND dest = %s AND hops = %s", (current_count+1, format_ts, rt_src, rt_dest, my_hops))
                    conn.commit()
                    try:
                        cur.execute("INSERT INTO routesummary (src, dest, count) VALUES (%s, %s, %s)", (rt_src, rt_dest, 1))
                        conn.commit()
                    except IntegrityError:
                        conn.rollback()
                        cur.execute("SELECT count FROM routesummary WHERE src = %s AND dest = %s", (rt_src, rt_dest))
                        fullcount = cur.fetchone()[0]
                        if fullcount is None:
                            cur.execute("UPDATE routesummary SET count = %s WHERE src = %s AND dest = %s", (1, rt_src, rt_dest))
                            conn.commit()
                        else:
                            cur.execute("UPDATE routesummary SET count = %s WHERE src = %s AND dest = %s", (fullcount+1, rt_src, rt_dest))
                            conn.commit()
            else:
                try:
                    cur.execute("INSERT INTO routesummary (src, dest, pcount) VALUES (%s, %s, %s)", (rt_src, rt_dest, 1))
                    conn.commit()
                except IntegrityError:
                    conn.rollback()
                    cur.execute("SELECT pcount FROM routesummary WHERE src = %s AND dest = %s", (rt_src, rt_dest))
                    partialcount = cur.fetchone()[0]
                    if partialcount is None:
                        cur.execute("UPDATE routesummary SET pcount = %s WHERE src = %s AND dest = %s", (1, rt_src, rt_dest))
                        conn.commit()
                    else:    
                        cur.execute("UPDATE routesummary SET pcount = %s WHERE src = %s AND dest = %s", (partialcount+1, rt_src, rt_dest))
                        conn.commit()
        else:
            print 'nhops count of ' + str(rt_num_hops) + ' found at src = ' + str(rt_src) + ' and dest = ' + str(rt_dest) + ' with hops list'
            print my_hops   
    else:
        print 'NoneType found at src = ' + str(rt_src) + ' and dest = ' + str(rt_dest)

#remove lock
def rm_lock():
    print 'Starting to remove lock at ' + str(datetime.now())
    os.remove('/var/lock/updateDB')
    print 'Lock removed at ' + str(datetime.now())

#loops through everything in results and then calls all update functions on each item
print 'The main loop of this run started at ' + str(datetime.now())
for item in results:
    updateRaw(item)
    updateLookup(item)
    updateSummary(item)

import atexit
atexit.register(rm_lock)

print 'This run finished at ' + str(datetime.now())
cur.close()
conn.close()
