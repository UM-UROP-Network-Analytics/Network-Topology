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

#connect to the database
es = elasticsearch.Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_trace-2018*"]
params = config()
conn = psycopg2.connect(**params)
cur = conn.cursor()
my_query = {}

cur.execute("SELECT * FROM rawtracedata limit 10")

for row in cur:
	src = cur.fetchone()[0]
	dest = cur.fetchone()[1]
	print 'src is ' + str(src)
	print ' dest is ' + str(dest)


cur.close()
conn.close()
