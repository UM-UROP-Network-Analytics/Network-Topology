#!/usr/bin/env python3
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
import numpy as np
import psycopg2
from psycopg2 import IntegrityError
from config import config

conn = psycopg2.connect(host="psdb.aglt2.org", database="psdb_urop", user="postgres")
print('Connected to the database')
cur = conn.cursor()
conn.close()
print('Closed conn')