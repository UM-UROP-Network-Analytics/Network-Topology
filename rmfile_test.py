#!/usr/bin/python
import os
import os.path 
from pathlib import Path
print('Removing lock')
os.remove('/var/lock/updateDB')