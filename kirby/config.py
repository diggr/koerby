import yaml
import os

__VERSION__ = 0.1
PROV_AGENT = "kirby_{}".format(__VERSION__)


def load_config():
    """ 
    Loads config yaml file. 
    """
    if os.path.exists("config.yml"):
        with open("config.yml") as f:
            config = yaml.load(f)
    else:
        raise IOError("config.yml does not exist!")
    return config

def init():
    """
    Creates directories specified in config.yml.
    """
    for dir_ in CONFIG["directories"].values():
        if not os.path.exists(dir_):
            os.makedirs(dir_)

CONFIG = load_config()

EXPORT_DIR = CONFIG["directories"]["export"]
SOURCE_DIR = CONFIG["directories"]["source"]
DATA_DIR = CONFIG["directories"]["data"]
PROJECT_NAME = CONFIG["project"]["name"]

DATASET_FILEPATH = os.path.join(DATA_DIR, "{}.json".format(PROJECT_NAME))
MATCHES_FILEPATH = os.path.join(DATA_DIR, "{}_matches.json".format(PROJECT_NAME))
KIRBY_FILEPATH = os.path.join(DATA_DIR, "{}_kirby.json".format(PROJECT_NAME))
