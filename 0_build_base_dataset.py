import pandas as pd
import json
import uuid
from config import IDS

from kirby import init, Dataset, Entry, read_csv


IMPORT = "wiki-import.csv"

def build_base_dataset():
    wiki = read_csv(IMPORT)

    dataset = Dataset()
    for row in wiki:
        entry = Entry()
        entry.add("wkp", row["wkp"].split("/")[-1])
        entry.add("name_en", row["name_en"])
        entry.add("name_ja", row["name_ja"])
        entry.add("hq_loc_label", row["hq_loc_label"])    
        dataset.add(entry)
    dataset.save()
    dataset.to_json()

def main():
    init()
    build_base_dataset()

if __name__ == "__main__":
    main()