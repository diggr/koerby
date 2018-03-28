import yaml
import json
import os
import pandas as pd
from config import IDS, DIR


def load_export_config():
    with open("export.yml") as f:
        config = yaml.load(f)
    return config

def save_export(export):
    df = pd.DataFrame(export)
    filepath = os.path.join(DIR["data"], "company_dataset.csv")
    df.sort_values(by="name_en").to_csv(filepath)

def main():
    config = load_export_config()

    company_dataset_filepath = os.path.join(DIR["data"], "company_dataset.json")
    with open(company_dataset_filepath) as f:
        company_dataset = json.load(f)

    export = []

    for company in company_dataset:

        for source_file in config["datasets"]:
            #print(source_file)
            pass
        
        export_row = {}

        #ids
        
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

        #additional info
        # 
        # 
        export.append(export_row)

    save_export(export)
    #print(export)
    #print(config)


if __name__ == "__main__":
    main()