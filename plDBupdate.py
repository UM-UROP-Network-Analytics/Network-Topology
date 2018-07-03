#!/usr/bin/python
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
import numpy as np
import psycopg2
from psycopg2 import IntegrityError
from config import config
import os
import os.path 
from pathlib import Path

#checks to see if this process is currently running
lock_file = Path("/var/lock/plDBupdate")
if lock_file.is_file():
  print('Error: process already running - Check /var/lock/plDBupdate')
  quit()
else:
  open('/var/lock/plDBupdate', "w+")

#connect to the database
es = elasticsearch.Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_packetloss-2018*"]
params = config()
conn = psycopg2.connect(**params)
cur = conn.cursor()
my_query = {}

#determine start and end times
now = datetime.utcnow()
curr_mon = now.month
curr_day = now.strftime("%d")
curr_year = now.strftime("%Y")
curr_hr = now.strftime("%H")
curr_min = now.strftime("%M")
curr_sec = now.strftime("%S")
end_date = curr_year + now.strftime("%m") + curr_day + 'T' + curr_hr + curr_min + curr_sec + 'Z'
cur.execute("SELECT * FROM rawpacketdata limit 1")
if cur.fetchone() is None:
  start_date = '20180101T000000Z'
else:
  cur.execute("SELECT to_char(max(timestamp+interval '1 sec'),'YYYYMMDD\"T\"HHMISS\"Z\"') FROM rawpacketdata")
  start_date = cur.fetchone()[0]

#build and run the query
my_query = {
    "size":1,
    "_source": {
        "include": [ 'src','dest', 'packet_loss', 'timestamp', 'src_host', 'dest_host', 'src_site', 'dest_site']
    },
    'query':{
        'bool':{
            'must':[
                {'range': {'timestamp': {'gte': start_date, 'lt': end_date}}},
            ]

        }
    },
}
results = elasticsearch.helpers.scan(es, query=my_query, index=my_index, raise_on_error = True, request_timeout=100000, size=1000)

#updates the raw traceroute data table
def updateRaw( item ):
    print 'Updating raw list'
    rt_src = item['_source']['src']
    rt_dest = item['_source']['dest']
    rt_loss = item['_source']['packet_loss']
    rt_ts = item['_source']['timestamp']
    rt_ts = rt_ts / 1000
    format_ts = time.strftime("%Y-%m-%d %H:%M:%S-0000", time.gmtime(rt_ts))
    try:
        cur.execute("INSERT INTO rawpacketdata (src, dest, loss, timestamp) VALUES (%s, %s, %s, %s)", (rt_src, rt_dest, rt_loss, format_ts))
        conn.commit()
    except IntegrityError:
        conn.rollback()
        pass

#updates the server lookup table
def updateLookup ( item ):
    print 'Updating lookup table'
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
        cur.execute("UPDATE serverlookup SET bandwidth = %s WHERE ipv6 = %s", ('0', rt_src))
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
        cur.execute("UPDATE serverlookup SET bandwidth = %s WHERE ipv4 = %s", ('0', rt_src))
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
    print 'Updating summary table'
    rt_src = item['_source']['src']
    rt_dest = item['_source']['dest']
    try:
        cur.execute("INSERT INTO losscount (src, dest, count) VALUES (%s, %s, %s)", (rt_src, rt_dest, 1))
        conn.commit()
    except IntegrityError:
        conn.rollback()
        cur.execute("SELECT count FROM losscount WHERE src = %s AND dest = %s", (rt_src, rt_dest))
        current_count = cur.fetchone()[0]
        if current_count is None:
            cur.execute("UPDATE losscount SET count = %s WHERE src = %s AND dest = %s", (1, rt_src, rt_dest))
            conn.commit()
        else:
            cur.execute("UPDATE losscount SET count = %s WHERE src = %s AND dest = %s", (current_count+1, rt_src, rt_dest))
            conn.commit()

#loops through everything in results and then calls all update functions on each item
for item in results:
    updateRaw(item)
    updateLookup(item)
    updateSummary(item)

#remove lock
print('Removing lock')
os.remove('/var/lock/plDBupdate')

print 'This run finished at ' + str(datetime.utcnow())
cur.close()
conn.close()