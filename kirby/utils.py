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

PUNCT_TRANSTABLE = str.maketrans("","", string.punctuation)

def remove_punctuation(s):
    """
    Removes punctuation from string
    """
    if s:
        s = s.translate(PUNCT_TRANSTABLE)
    return s

def remove_bracketed_text(s):
    """
    Removes text in brackets from string :s: .
    """
    s = re.sub(r'\([^\()]+?\)', '', s)
    s = re.sub(r'\[[^\[]]+?\]', '', s)
    s = re.sub(r'\[[^\[]]+?\]', '', s)
    return s.strip()

def std_url(url):
    """
    Standardizes urls by removing protocoll and final slash.
    """
    if url:
        #remove protocoll
        url = url.split("//")[-1]
        #remove '/' at the end
        if url[len(url)-1] == "/":
            url = url[:len(url)-1]
    return url

def std(s, lower=True, rm_punct=True, rm_bracket=True, remove_strings=None):
    if s:
        if lower:
            s = s.lower()
        
        if rm_punct:
            s = remove_punctuation(s)
        
        if remove_strings:
            for form in remove_strings:
                s = s.replace(form.lower(), "")
        
        if rm_bracket:
            s = remove_bracketed_text(s)

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
