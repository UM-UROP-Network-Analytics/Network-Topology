#!/usr/bin/python
import elasticsearch
from datetime import datetime, timedelta
import time
import numpy as np

es = elasticsearch.Elasticsearch(['atlas-kibana.mwt2.org:9200'],timeout=60)
my_index = ["ps_packet_loss-2018*"]
results = es.indices.exists(index=my_index)
print(results)