import yaml
import os
import json
import pandas as pd
from config import DIR
from utils.wiki import identify
from tqdm import tqdm

def get_source_files():
    for filename in os.listdir(DIR["source"]):
        pre_filepath = os.path.join(DIR["pre"], filename)
        if not os.path.exists(pre_filepath):
            filepath = os.path.join(DIR["source"], filename)
            df = pd.read_csv(filepath)
            dataset = json.loads(df.to_json(orient="records"))
            yield dataset, filename

def validate(dataset):
    if not "name_en" in dataset[0] and not "name_ja" in dataset[0]:
        print("[ERROR] CSV File must contain column 'name_en' or 'name_ja'")
        return False
    return True

def save(dataset, filename):
    filepath = os.path.join(DIR["pre"], filename)
    df = pd.DataFrame(dataset)
    df.to_csv(filepath)

def main():
    for dataset, filename in get_source_files():
        print(filename)

        if validate(dataset):
            for row in tqdm(dataset):
                wiki_link, wkp = None, None
                if "name_en" in row:
                    wiki_link, wkp = identify(row["name_en"], "en")
                elif "name_ja" in row:
                    wiki_link, wkp = identify(row["name_ja"], "ja")
                
                row["wiki_link"] = wiki_link
                row["wkp"] = wkp
        
            save(dataset, filename)


if __name__ == "__main__":
    main()