import os
import json
import yaml
from rdflib import Graph, RDF
from .namespaces import KirbyNamespace

def load_jsonld(filepath, context):
    with open(filepath, encoding="utf-8") as f:
        graph_data = json.load(f)
    g = Graph().parse(data=json.dumps(graph_data["@graph"]), format="json-ld", context=context)
    return g

def load_config_file(directory):
    config_filepath = os.path.join(directory, "config.yml")
    config = yaml.load(open(config_filepath))
    return config

class KirbyReader(object):
    def __init__(self, kirby_dir):
        config = load_config_file(kirby_dir)
        namespaces = config["namespaces"]
        self._matches_filepath = os.path.join(kirby_dir,
                                              config["directories"]["data"],
                                              "{}_matches.json".format(config["project"]["name"]))
        self._ns = KirbyNamespace(namespaces)

        print("load koerby matches ...")
        self._g = load_jsonld(self._matches_filepath, self._ns.context)
    
    def matches_file(self):
        return self._matches_filepath

    def _all_matches(self, source_uri):
        for result in self._g.triples( (source_uri, self._ns.prop("belongs_to_match"), None) ):
            match_uri = result[2]
            match_value = self._g.value(match_uri, self._ns.prop("has_match_value"))
            print(source_uri, match_uri, match_value)
            yield (match_uri, float(match_value))

    def _all_match_entries(self, match):
        for result in self._g.triples( (match, self._ns.prop("matched"), None) ):
            yield result[2]

    def _dataset(self, uri):
        ds_name = self._g.value(uri, self._ns.prop("is_part_of"))
        return str(ds_name)

    def get_matches(self, source_ds, source_id, target_ds, threshold):
        source_uri = self._ns.entry(source_ds, source_id)
        matches = []
        for match, match_value in self._all_matches(source_uri):
            for entry in self._all_match_entries(match):
                #print("\t{}".format(entry))
                if target_ds in entry and match_value >= threshold:
                    matches.append( (entry, match_value) )
        return sorted(matches, key=lambda x: -x[1])

