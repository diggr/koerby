import pandas as pd 
import json
import string
import re
import os
from .config import CONFIG, SOURCE_DIR

def remove_html(t):
    if t:
        t = re.sub(r'<[^<]+?>', '', t)
        t = t.strip()
    return t

def source_files():
    for filename in os.listdir(SOURCE_DIR):
        if filename.endswith(".csv"):
            filepath = os.path.join(SOURCE_DIR, filename)
            yield filepath, filename
