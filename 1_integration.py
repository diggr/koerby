import json
import os
import yaml
import uuid
import pandas as pd
from tqdm import tqdm
from config import DIR, IDS
from utils.helper import std, get_geoip, top_level_domain, DOMAIN_DICT

def load_company_dataset():
    filepath = os.path.join(DIR["data"], "company_dataset.json")
    if os.path.exists(filepath):
        with open(filepath) as f:
            dataset = json.load(f)
    else:
        dataset = []
    return dataset

def save_company_dataset(dataset):
    filepath = os.path.join(DIR["data"], "company_dataset.json")
    with open(filepath, "w") as f:
        json.dump(dataset, f, indent=4)

def load_pre_files():
    for filename in os.listdir(DIR["pre"]):
        if filename[0] != ".":
            filepath = os.path.join(DIR["pre"], filename)
            df = pd.read_csv(filepath)
            dataset = json.loads(df.to_json(orient="records"))
            yield dataset, filename

def get_all_names(entry):
    if not entry["name_en"]:
        name_en = []
    else:
        name_en = entry["name_en"]
    if not entry["name_ja"]:
        name_ja = []
    else:
        name_ja = entry["name_ja"]
    all_names = name_en + name_ja
    return [ std(x) for x in all_names ]

def std_url(url):
    if url:
        return url.replace("http://", "").replace("https://", "").replace("/","")
    else:
        return None



def get_match(company, company_dataset):

    for entry in company_dataset:

        #ids
        for id_ in IDS:
            if id_ in company and id_ in entry:
                if company[id_]:
                    if entry[id_] == company[id_]:
                        #print("id match")
                        return entry
        #url
        if "url" in company:
            if entry["url"]:
                all_entry_urls = [ std_url(x["url"]) for x in entry["url"] ]
                if company["url"]:
                    if std_url(company["url"]) in all_entry_urls:
                        #print("url match")
                        return entry

        #titlestring
        all_entry_names = get_all_names(entry)

        if "name_ja" in company:
            for name in all_entry_names:
                if std(company["name_ja"]) in all_entry_names:
                    return entry
                    
        if "name_en" in company:
            for name in all_entry_names:
                if std(company["name_en"]) in all_entry_names:
                    return entry
    
    return None

def get_url_information(url):
    #print("url ifno")
    geoip = get_geoip(url)
    tld = top_level_domain(url)
    if tld in DOMAIN_DICT:
        tld_country = DOMAIN_DICT[tld]
    else:
        tld_country = None
    rv = {
        "url": url,
        "geoip": geoip,
        "tld": tld,
        "tld_country": tld_country
    }
    return rv


def insert_entry(company_dataset, company, filename, row_id):
    id_ = uuid.uuid4().hex
    if "name_en" in company:
        name_en = [ company["name_en"] ]
    else:
        name_en = []
    if "name_ja" in company:
        name_ja = [ company["name_ja"] ]
    else:
        name_ja = []
    if "url" in company:
        url_info = get_url_information(company["url"])
        if url_info["url"]:
            url = [ url_info ]
        else:
            url = []
    else:
        url = []
    if "wiki_link" in company:
        wiki_links = [ company["wiki_link"] ]
    else:
        wiki_links = []
    if "wkp" in company:
        wkp = company["wkp"]
    else:
        wkp = None
    if "mobygames_id" in company:
        mobygames_id = company["mobygames_id"]
    else:
        mobygames_id = None

    source_files = {
        filename: [ row_id ]
    }

    company_dataset.append({
        "kirby_id": id_,
        "name_en": name_en,
        "name_ja": name_ja,
        "url": url,
        "wiki_links": wiki_links,
        "wkp": wkp,
        "mobygames_id": mobygames_id,
        "source_files": source_files
    })

def add_to_entry(entry, company, filename, row_id):
    if "name_en" in company:
        if company["name_en"] not in entry["name_en"]:
            entry["name_en"].append(company["name_en"]) 
    if "name_ja" in company:
        if company["name_ja"] not in entry["name_ja"]:
            entry["name_ja"].append(company["name_ja"])
    if "wiki_link" in company:
        if company["wiki_link"] not in entry["wiki_links"]:
            entry["wiki_links"].append(company["wiki_link"])
    if "wkp" in company:
        entry["wkp"] = company["wkp"]
    if "mobygames_od" in company:
        entry["mobygames_id"] = company["mobygames_id"]
    if "url" in company:
        if entry["url"]:
            all_urls = [x["url"] for x in entry["url"]]
        else:
            all_urls = []
        if company["url"] not in all_urls:
            entry["url"].append(get_url_information(company["url"]))
    if filename in entry["source_files"]:
        if row_id not in entry["source_files"][filename]:
            entry["source_files"][filename].append(row_id)
    else:
        entry["source_files"][filename] = [ row_id ]

def save_dataset_yaml(datasets):
    with open("datasets.yml", "w") as f:
        yaml.dump(datasets, f, default_flow_style=False)


def main():

    dataset_yaml = {}

    company_dataset = load_company_dataset()

    for dataset, filename in load_pre_files():

        print(filename)
        dataset_yaml[filename] = list ( set(dataset[0].keys()) - set(["wkp", "wiki_link", "url", "name_en", "name_ja", "mobygames_id", "id"]) )

        for row_id, company in tqdm(enumerate(dataset)):
            match = get_match(company, company_dataset)
            if not match:
                insert_entry(company_dataset, company, filename, row_id)
            else:
                add_to_entry(match, company, filename, row_id)
    
    save_company_dataset(company_dataset)

    save_dataset_yaml(dataset_yaml)

if __name__ == "__main__":
    main()