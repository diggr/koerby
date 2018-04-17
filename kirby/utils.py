import pandas as pd 
import json
import string
import re
import os
from .config import CONFIG, SOURCE_DIR

def remove_html(s):
    """
    Removes html tags from string :s: .
    """
    if s:
        s = re.sub(r'<[^<]+?>', '', s)
        s = " ".join(s.split())
        s = s.strip()
    return s

def source_files():
    """
    Lists all .csv files in source directory.
    """
    for filename in os.listdir(SOURCE_DIR):
        if filename.endswith(".csv"):
            filepath = os.path.join(SOURCE_DIR, filename)
            yield filepath, filename
