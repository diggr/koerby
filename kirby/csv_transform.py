import os
import pandas as pd
from tqdm import tqdm
from .utils import read_csv, write_csv
from .config import CONFIG, SOURCE_DIR

def csv_transform(columns, transformation):
    for filename in os.listdir(SOURCE_DIR):
        source_filepath = os.path.join(SOURCE_DIR, filename)

        df = pd.read_csv(source_filepath)

        for column in columns:
            if column in df.columns:
                df[column] = df[column].apply(transformation)
        
        df.to_csv(source_filepath, index=False)
