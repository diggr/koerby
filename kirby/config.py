import yaml
import os

class Config(object):

    def __init__(self):
        if os.path.exists("config.yml"):
            with open("config.yml") as f:
                config = yaml.load(f)
            self.ids = config["ids"]
            self.data_fields = config["data"]
            self.dir = config["field"]