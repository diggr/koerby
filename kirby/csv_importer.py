import os
from tqdm import tqdm
from .utils import read_csv, write_csv
from .config import CONFIG

IMPORT_DIR = CONFIG.dir["import"]
SOURCE_DIR = CONFIG.dir["source"]

class CSVImporter(object):

    def _import_files(self):
        for filename in os.listdir(IMPORT_DIR):
            source_filepath = os.path.join(SOURCE_DIR, filename)
            if not os.path.exists(source_filepath):
                dataset = read_csv(os.path.join(IMPORT_DIR, filename))
                yield dataset, filename

    def __init__(self, row_transform=None):
        self.row_transform = row_transform

    def _save(self, dataset, filename):
        write_csv(dataset, os.path.join(SOURCE_DIR, filename))
    
    def import_datasets(self):
        for dataset, filename in self._import_files():
            print(filename)
            if self.row_transform:
                for row in tqdm(dataset):
                    row = self.row_transform(row)
            self._save(dataset, filename)
            
