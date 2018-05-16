import yaml
import os
import hashlib
from rdflib import Namespace, RDF

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


class KirbyNamespace(object):
    """
    Wrapper for namespacing and uri generation
    """
    def __init__(self):
        self._core = Namespace(CONFIG["namespaces"]["core"])
        #self._entry = Namespace(CONFIG["namespaces"]["entry"])
        self._dataset = Namespace(CONFIG["namespaces"]["dataset"])
        self._property = Namespace(CONFIG["namespaces"]["property"])
        self._match = Namespace(CONFIG["namespaces"]["match"])
    
        self.context = {
            "rdf": str(RDF),
            "core": str(self._core),
            "dataset": str(self._dataset),
            "properties": str(self._property)
        }
        
    # def kirbyEntry(self):
    #     id_ = "{}_{}".format(PROJECT_NAME, uuid.uuid4().hex)
    #     return self._entry[id_]
    
    def dataset(self, name):
        return self._dataset[name]

    def entry(self, dataset_name, source_id):
        return self._dataset["{}/{}".format(dataset_name, source_id)]
    
    def prop(self, name):
        return self._property["p_{}".format(name)]

    def match(self, row, match):
        comb = sorted([str(row), str(match)])
        uid = hashlib.md5("".join(comb).replace(NS("Dataset"), "").encode("utf-8")).hexdigest()
        return self._match["m_{}".format(uid)]        

    def __call__(self, x):
        return self._core[x]

NS = KirbyNamespace()