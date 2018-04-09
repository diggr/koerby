import pandas as pd
import os
from .kirby_dataset import KirbyDataset, KirbyEntry
from .config import IDS, DATA_DIR, DATASET_NAME, SOURCE_DIR, DATAFIELDS, INTEGRATION_FIELDS
from .utils import read_csv

class CSVIntegrator(object):

    def _source_files(self):
        for filename in os.listdir(SOURCE_DIR):
            source_filepath = os.path.join(SOURCE_DIR, filename)
            dataset = read_csv(source_filepath)
            yield dataset, filename

    def base_integration(self, row, row_id, source_file):
        entry = None
        for id_ in IDS:
            if id_ in row:
                if row[id_]:
                    entry = self.dataset.find_entry(id_, row[id_])
                    break
        if entry:
            entry.integrate(row, source_file, row_id)
        else:
            self.dataset.add(KirbyEntry(row, source_file, row_id))

    def __init__(self, costum_integration=None):

        self.dataset = KirbyDataset()
        self.dataset.load(os.path.join(DATA_DIR, DATASET_NAME+".json"))

        for dataset, filename in self._source_files():
            for row_id, row in enumerate(dataset):

                self.base_integration(row, row_id, filename)

        self.dataset.save()