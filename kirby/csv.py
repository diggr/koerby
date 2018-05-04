import os
import json
import pandas as pd
from tqdm import tqdm
from .config import SOURCE_DIR

def read_csv(filepath, sep=","):
    """
    Reads csv file from location :filepath: and returns it as a python dict.
    """
    df = pd.read_csv(filepath, sep=sep)
    dataset = json.loads(df.to_json(orient="records")) 
    return dataset

def write_csv(dataset, filepath):
    """
    Writes python dict :dataset: as .csv file to :filepath:
    """
    df = pd.DataFrame(dataset)
    df.to_csv(filepath, index=False, encoding="utf-8")
    return 
