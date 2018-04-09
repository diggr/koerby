import pandas as pd 
import json
import string
import re
import os
from .config import CONFIG

def read_csv(filepath, sep=","):
    df = pd.read_csv(filepath, sep=sep)
    dataset = json.loads(df.to_json(orient="records")) 
    return dataset

def write_csv(dataset, filepath):
    df = pd.DataFrame(dataset)
    df.to_csv(filepath, index=False)
    return 



PUNCT_TRANSTABLE = str.maketrans("","",".,:-〔〕'’*/!&?+")
FORMS= [" sarl", " sa", " bv"," co ltd", " tec", " coltd", " limited", " ltd", " llc", " gmbh", " ltda", " corp", " inc", " srl"]


def remove_html(t):
    if t:
        t = re.sub(r'<[^<]+?>', '', t)
        t = t.strip()
    return t

def std(t):
    if t:
        t = t.lower()
        t = t.translate(PUNCT_TRANSTABLE)
        for form in FORMS:
            t = t.replace(form, "")
            
        t = re.sub(r'\([^\()]+?\)', '', t)
        t = re.sub(r'\[[^\[]]+?\]', '', t)

        t = t.replace(" 株式会社", "")
        t = t.strip()

    return t