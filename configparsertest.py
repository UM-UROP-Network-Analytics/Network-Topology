#!/usr/bin/env python3
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
import numpy as np
import psycopg2
from psycopg2 import IntegrityError
from config import config

params = config()
print('Connecting')
conn = psycopg2.connect(**params)
print('Connected to the database')
conn.close()