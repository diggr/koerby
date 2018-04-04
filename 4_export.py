import yaml
import json
import os
import pandas as pd
from config import IDS, DIR
from tqdm import tqdm
from collections import Counter
from utils.helper import is_country



class CSVExporter(object):
    """
    Builds a costum csv file from the kirby dataset.
    """

    def _load_yml_config(self, datasets_yml, export_yml):
        """
        Load config yml files needed for csv export
        """
        if os.path.exists(datasets_yml):
            with open("export.yml") as f:
                export = yaml.load(f)
        else:
            config = None
        if os.path.exists(datasets_yml):
            with open("datasets.yml") as f:
                datasets = yaml.load(f)
        else:
            datasets = None
        return datasets, export

    def _load_source_datasets(self, datasets):
        """
        Load all source datasets of the project (located in folder 'pre')
        """
        ds = {}
        for dataset in datasets.keys():
            filepath = os.path.join(DIR["pre"], dataset)
            df = pd.read_csv(filepath)
            ds[dataset] = json.loads(df.to_json(orient="records"))
        return ds

    def _load_kirby_dataset(self, kirby_dataset_filepath=None):
        """
        Load integrated kirby dataset of the project
        """
        kirby_filepath = os.path.join(DIR["data"], "company_dataset.json")
        with open(kirby_filepath) as f:
            kirby_dataset = json.load(f)
        return kirby_dataset

    def _get_source_field_data(self, kirby_entry, source, source_field):
        """
        Returns all values of the field :source_field: in the source files :source:
        for each associated entry in dataset entry :kirby_entry:
        """
        source_data = None
        if source in kirby_entry["source_files"]:
            source_data = []
            for source_id in kirby_entry["source_files"][source]:
                if source_id:
                    data = self.source_datasets[source][source_id][source_field]
                    if data:
                        if data[0] == "[" and data[len(data)-1] == "]":
                            data = json.loads(data.replace("'",'"'))
                        else:
                            data = [ data ]
                        source_data += data
        return source_data

    def _export_csv_columns(self):
        """
        Generates list with the columns of the export csv file. 
        The order in the list also equals the column position in the export file.
        """
        columns = [
            "kirby_id",
            "wkp",
            "mobygames_id",
            "name_en",
            "name_ja",
            "company_address",
            "url",
            "tld",
            "geoip"
        ]
        for source, fields in self.export_config["fields"].items():
            for source_field, target_field in fields.items():
                columns.append(target_field)
        for most_common in self.export_config["most_common"].keys():
            columns.append("most_common_"+most_common)

        for dataset in self.source_datasets.keys():
            columns.append(dataset)
        return columns

    def _save_export(self, export):
        """
        Saves export dataset to csv file
        """
        columns = self._export_csv_columns()
        df = pd.DataFrame(export)
        filepath = os.path.join(DIR["data"], "company_dataset.csv")
        df.sort_values(by="name_en").to_csv(filepath, columns=columns, index=False)
 
    def __init__(self, dataset_yml="datasets.yml", export_yml="export.yml"):
        """ 
        Builds export csv dataset for kirby project 
        """
        self.source_dataset_list, self.export_config = self._load_yml_config(dataset_yml, export_yml)
        self.source_datasets = self._load_source_datasets(self.source_dataset_list)
        self.kirby_dataset = self._load_kirby_dataset()

        export_dataset = []

        for entry in self.kirby_dataset:
            export_row = {}
            export_row["kirby_id"] = entry["kirby_id"]
            
            # export ids
            for id_ in IDS:
                if id_ in entry:
                    export_row[id_] = entry[id_]

            #integrated fields - names, urls, addresses

            #export names
            if "name_en" in entry:
                if entry["name_en"]:
                    export_row["name_en"] = entry["name_en"][0]
            if "name_ja" in entry:
                if entry["name_ja"]:
                    export_row["name_ja"] = entry["name_ja"][0]                    

            #urls
            if entry["url"]:
                url = entry["url"][0]
                if url["url"]:
                    export_row["url"] = url["url"]
                    export_row["tld"] = url["tld_country"]
                    if url["geoip"]:
                        export_row["geoip"] = url["geoip"]["names"]["en"]

            #addresses            
            if "addresses" in entry:
                if len(entry["addresses"]) > 0:
                    export_row["company_address"] = entry["addresses"][0] 

            #additional data fields
            for source, fields in self.export_config["fields"].items():
                for source_field, output_field in fields.items():
                    source_data = self._get_source_field_data(entry, source, source_field)
                    if source_data:
                        export_row[output_field] = ";".join(source_data)
            
            #source_info
            for source_file in self.source_datasets.keys():
                if source_file in entry["source_files"]:
                    export_row[source_file] = ";".join([ str(x) for x in entry["source_files"][source_file] ])
            
            #aggregate fields - most_common
            for target_field, source_fields in self.export_config["most_common"].items():
                all_data = []
                most_common = None
                for source, source_field in source_fields.items():
                    source_data = self._get_source_field_data(entry, source, source_field)
                    if source_data:
                        all_data += source_data
                                   
                if len(entry["url"]) > 0:
                    if entry["url"][0]["tld_country"]:
                        all_data.append(entry["url"][0]["tld_country"])
                    if entry["url"][0]["geoip"]:
                        all_data.append(entry["url"][0]["geoip"]["names"]["en"])
                all_data =  [ x for x in all_data if is_country(x)]

                if len(all_data) == 0:
                    most_common = None
                elif len(all_data) == 1:
                    most_common = all_data[0]
                else:
                    field_counter = Counter(all_data)
                    most = field_counter.most_common()[0][1]
                    top = [ field_counter.most_common()[0][0] ]
                    for term, count in field_counter.most_common()[1:]:
                        if count < most:
                            break
                        top.append(term)
                    most_common = "; ".join(top)

                export_row["most_common_"+target_field] = most_common            
           
            export_dataset.append(export_row)

        self._save_export(export_dataset)

def main():
    exporter = CSVExporter()


if __name__ == "__main__":
    main()