#!/usr/bin/python
import numpy as np
import psycopg2
from psycopg2 import IntegrityError
from config import config

#connect to the database
params = config()
conn = psycopg2.connect(**params)
cur = conn.cursor()
cur2 = conn.cursor()

#grab all data from traceroute table to start filling in min timestamps
cur.execute("SELECT * FROM traceroute")

#loop through every row in the table
for row in cur:
	#tr table columns go src > dest > hops > nhops > count > rtnum
	tbl_src = row[0]
	tbl_dst = row[1]
	tbl_hop = row[2]
	tbl_rt_num = row[5]

	#find our current minimum timestamp from all of the rawtracedata
	cur2.execute("SELECT min(timestamp) FROM rawtracedata WHERE src = %s AND dest = %s AND hops = %s", (tbl_src, tbl_dst, tbl_hop))
	tr_min_ts = cur2.fetchone()[0]
	#add this to our traceroute table 
	cur2.execute("UPDATE traceroute SET min_ts = %s WHERE src = %s AND dest = %s AND rtnum = %s", (tr_min_ts, tbl_src, tbl_dst, tbl_rt_num))
	conn.commit()
	#and then repeat for an inital max timestamp for each route
	cur2.execute("SELECT max(timestamp) FROM rawtracedata WHERE src = %s AND dest = %s AND hops = %s", (tbl_src, tbl_dst, tbl_hop))
	tr_max_ts = cur2.fetchone()[0]
	cur2.execute("UPDATE traceroute SET max_ts = %s WHERE src = %s AND dest = %s AND rtnum = %s", (tr_max_ts, tbl_src, tbl_dst, tbl_rt_num))
	conn.commit()

cur.close()
cur2.close()
conn.close()