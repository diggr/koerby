import yaml
import os

__VERSION__ = 0.1
PROV_AGENT = "kirby_{}".format(__VERSION__)


class Config(object):
    def __init__(self):
        if os.path.exists("config.yml"):
            with open("config.yml") as f:
                config = yaml.load(f)
            self.dir = config["folders"]
            self.project = config["project"]

def init():
    for dir_ in CONFIG.dir.values():
        if not os.path.exists(dir_):
            os.makedirs(dir_)

CONFIG = Config()

EXPORT_DIR = CONFIG.dir["export"]
SOURCE_DIR = CONFIG.dir["source"]
DATA_DIR = CONFIG.dir["data"]
PROJECT_NAME = CONFIG.project["name"]

DATASET_FILEPATH = os.path.join(DATA_DIR, "{}.json".format(PROJECT_NAME))
MATCHES_FILEPATH = os.path.join(DATA_DIR, "{}_matches.json".format(PROJECT_NAME))
KIRBY_FILEPATH = os.path.join(DATA_DIR, "{}_kirby.json".format(PROJECT_NAME))

#IDS = CONFIG.ids
#DATAFIELDS = CONFIG.datafields
#INTEGRATION_FIELDS = CONFIG.integration