import yaml
import json
import os
import pandas as pd
from config import IDS, DIR


def load_export_config():
    if os.path.exists("export.yml"):
        with open("export.yml") as f:
            config = yaml.load(f)
    else:
        config = None
    with open("datasets.yml") as f:
        datasets = yaml.load(f)
    return datasets, config

def save_export(export):
    df = pd.DataFrame(export)
    filepath = os.path.join(DIR["data"], "company_dataset.csv")
    df.sort_values(by="name_en").to_csv(filepath)

def main():
    datasets, config = load_export_config()

    company_dataset_filepath = os.path.join(DIR["data"], "company_dataset.json")
    with open(company_dataset_filepath) as f:
        company_dataset = json.load(f)

    export = []

    for company in company_dataset:

        export_row = {}

        #ids
        export_row["kirby_id"] = company["kirby_id"]

        for id_ in IDS:
            if id_ in company:
                export_row[id_] = company[id_]
            else:
                export_row[id_] = None

        #names
        if "name_en" in company:
            if company["name_en"]:
                export_row["name_en"] = company["name_en"][0]
            else:
                export_row["name_em"] = None
        else:
            export_row["name_en"] = None
        if "name_ja" in company:
            if company["name_ja"]:
                export_row["name_ja"] = company["name_ja"][0]
            else:
                export_row["name_em"] = None
        else:
            export_row["name_ja"] = None

        #urls
        if company["url"]:
            url = company["url"][0]
            if url["url"]:
                export_row["url"] = url["url"]
                export_row["tld"] = url["tld"]
                if url["geoip"]:
                    export_row["geoip"] = url["geoip"]["names"]["en"]
            else:
                export_row["url"] = None
                export_row["tld"] = None
                export_row["geoip"] = None
        else:
            export_row["url"] = None
            export_row["tld"] = None
            export_row["geoip"] = None            
        #additional info
        # 
        # 

        #source_info
        for source_file in datasets.keys():
            if source_file in company["source_files"]:
                export_row[source_file] = ";".join([ str(x) for x in company["source_files"][source_file] ])
            else:
                export_row[source_file] = ""

        export.append(export_row)

    save_export(export)
    #print(export)
    #print(config)


if __name__ == "__main__":
    main()