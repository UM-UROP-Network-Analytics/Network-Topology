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

#grab all data from pl summary table to start filling in min timestamps
cur.execute("SELECT * FROM losscount")

#loop through every row in the table
for row in cur:
	#tr table columns go src > dest
	tbl_src = row[0]
	tbl_dst = row[1]
  #find our current minimum timestamp from all of the rawpacketdata
	cur2.execute("SELECT min(timestamp) FROM rawpacketdata WHERE src = %s AND dest = %s", (tbl_src, tbl_dst))
	tr_min_ts = cur2.fetchone()[0]
	#add this to our traceroute table 
	cur2.execute("UPDATE losscount SET min_ts = %s WHERE src = %s AND dest = %s", (tr_min_ts, tbl_src, tbl_dst))
	conn.commit()
	#and then repeat for an inital max timestamp for each route
	cur2.execute("SELECT max(timestamp) FROM rawpacketdata WHERE src = %s AND dest = %s", (tbl_src, tbl_dst))
	tr_max_ts = cur2.fetchone()[0]
	cur2.execute("UPDATE losscount SET max_ts = %s WHERE src = %s AND dest = %s", (tr_max_ts, tbl_src, tbl_dst))
	conn.commit()

cur.close()
cur2.close()
conn.close()
