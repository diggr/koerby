import pandas as pd
import json
import uuid
from config import IDS

IMPORT = "wiki_import.csv"

def empty_entry():
    entry = {}
    entry["kirby_id"] = uuid.uuid4().hex
    for id_ in IDS:
        entry[id_] = None
    entry["name_en"] = []
    entry["name_ja"] = []
    entry["wiki_links"] = []
    entry["url"] = []
    entry["addresses"] = []
    entry["source_files"] = {}
    return entry

def main():
    wiki_df = pd.read_csv(IMPORT)
    wiki = json.loads(wiki_df.to_json(orient="records"))

    base_dataset = []

    for entry in wiki:
        new_entry = empty_entry()
        new_entry["wkp"] = entry["wkp"]
        if entry["name_en"]:
            new_entry["name_en"] = [ entry["name_en"] ]
        if entry["name_ja"]:
            new_entry["name_ja"] = [ entry["name_ja"] ]
        base_dataset.append(new_entry)
    
    with open("data/company_dataset.json", "w") as f:
        json.dump(base_dataset, f, indent=4)

if __name__ == "__main__":
    main()