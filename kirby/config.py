import yaml
import os

class Config(object):
    def __init__(self):
        if os.path.exists("config.yml"):
            with open("config.yml") as f:
                config = yaml.load(f)
            self.ids = config["ids"]
            self.dir = config["folders"]
            self.dataset = config["dataset"]
            self.datafields = config["datafields"]
            self.integration = config["integration"]


def init():
    for dir_ in CONFIG.dir.values():
        if not os.path.exists(dir_):
            os.makedirs(dir_)

CONFIG = Config()

IMPORT_DIR = CONFIG.dir["import"]
SOURCE_DIR = CONFIG.dir["source"]
DATA_DIR = CONFIG.dir["data"]

DATASET_NAME = CONFIG.dataset["name"]
IDS = CONFIG.ids
DATAFIELDS = CONFIG.datafields
INTEGRATION_FIELDS = CONFIG.integration