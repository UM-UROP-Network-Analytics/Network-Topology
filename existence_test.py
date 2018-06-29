#!/usr/bin/python
import elasticsearch
from elasticsearch import helpers
from datetime import datetime, timedelta
import time
import numpy as np

es = elasticsearch.Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_packet_loss-2018*"]
results = elasticsearch.helpers.exists(es, index=my_index, request_timeout=100000, size=1000)
print(results)