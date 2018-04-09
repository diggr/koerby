import yaml
import os
import json
import pandas as pd
from config import DIR
from utils.wiki import identify
from utils.helper import remove_html
from tqdm import tqdm

from kirby import CONFIG, CSVImporter

def wiki_identify(row):
    wiki_link, wkp = None, None
    if "name_en" in row:
        row["name_en"] = remove_html(row["name_en"])
        wiki_link, wkp = identify(row["name_en"], "en")
    if not wkp:
        if "name_ja" in row:
            row["name_ja"] = remove_html(row["name_ja"])
            wiki_link, wkp = identify(row["name_ja"], "ja")
    
    row["wiki_link"] = wiki_link
    row["wkp"] = wkp
    return row

def main():
    importer = CSVImporter(row_transform=wiki_identify)
    importer.import_datasets()


if __name__ == "__main__":
    main()