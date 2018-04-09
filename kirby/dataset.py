import json
import uuid
import os
import pickle
from collections import defaultdict
from .config import DATA_DIR, DATASET_NAME, IDS, DATAFIELDS

class Entry(dict):
    
    def __init__(self, data=None, source=None, source_row=None):
        self.id = uuid.uuid4().hex
        self._sources = defaultdict(list)

        if data:
            #add key values to kirby entry
            for key, value in data.items():
                if key in IDS or key in DATAFIELDS:
                    if type(value) != list:
                        if value:
                            value = [ value ]
                        else:
                            value = []
                    if key not in ["kirby_id", "sources"]:
                        self.update({key: value})

    def add_source(self, source, row):
        if row not in self._sources:
            self._sources[source].append(row)

    def serialize(self, sources=True):
        row = { key:value for key, value in self.items() }
        row["kirby_id"] = self.id
        if sources:
            row["sources"] = dict(self._sources)
        return row
    
    def add(self, key, value):
        if key not in self:
            if type(value) != list:
                if value:
                    value = [ value ]
                else:
                    value = []
            self.update({key: value})

    def contains(self, key, value):
        if type(key) != list:
            key = [ key ]
        for v, k in self.items():
            if v in key:
                if value in k:
                    return True
        return False

    def integrate(self, row, source, source_id):
        if source_id not in self._sources[source]:
            self._sources[source].append(source_id)

            for key, value in row.items():
                if key in IDS or key in DATAFIELDS:
                    current = self.get(key)
                    if not current:
                        current = []
                    if value not in current:
                        current.append(value)
                    self.update({key: current})        

class Dataset(object):

    def __init__(self):
        self._entries = []

    def add(self, entry):
        #check if Entry
        try:
            entry.serialize()
            self._entries.append(entry)
        except:
            raise TypeError("<entry> needs to be of class Entry")

    def save(self):
        filename = DATASET_NAME+".kirby"
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, "wb") as f:
            pickle.dump(self, f)
    
    def to_json(self):
        filename = DATASET_NAME+".json"
        filepath = os.path.join(DATA_DIR, filename)
        with open(filepath, "w") as f:
            json.dump( [ x.serialize() for x in self._entries ], f, indent=4 )
            
    def find_entry(self, key, value, standardize=None):
        for entry in self._entries:
            if entry.contains(key, value):
                return entry
        return None

def load():
    """
    load project kibry file and returns dataset
    """
    filename = DATASET_NAME+".kirby"
    filepath = os.path.join(DATA_DIR, filename)
    with open(filepath, "rb") as f:
        dataset = pickle.load(f)
    return dataset
    