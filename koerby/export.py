"""
Koerby export functions

export_cluster - exports clusters as JSON file

"""

import json
import os
from rdflib import RDF
from collections import defaultdict
from .config import PROV_AGENT, NS, MATCHES_FILEPATH, CONFIG, DATASET_FILEPATH, CLUSTER_FILEPATH, NS_CLUSTER, EXPORT_DIR, PROJECT_NAME
from .rdf_dataset import RdfDataset

def export_cluster(export_config, filename=PROJECT_NAME):
    """
    Exports Koerby clusters as a JSON file to the export folder.

    :export_config: defines the fields/properties which should be exported
    """
    print("load datasets ...")
    datasets = RdfDataset(DATASET_FILEPATH, CONFIG["namespaces"])
    print("load matches ...")
    matches = RdfDataset(MATCHES_FILEPATH, CONFIG["namespaces"])
    print("load clusters ...")
    clusters = RdfDataset(CLUSTER_FILEPATH, CONFIG["namespaces"])

    export = []

    print("build export dataset ...")
    for cluster_triple in clusters.iter_by_type(NS_CLUSTER):
        cluster_uri = cluster_triple[0]
        cluster = defaultdict(list)
        cluster["cluster_uri"] = str(cluster_uri)
        print(cluster_uri)
        for match in [ x[2] for x in clusters.g.triples( (cluster_uri, NS.prop("contains_match"), None) ) ]:
            print("\t", match)
            for entry in [ x[2] for x in matches.g.triples( (match, NS.prop("matched"), None) ) ]:
                print("\t\t", entry)
                cluster["entry_uris"].append(entry)
                for field in export_config["fields"]:
                    field_values = [ str(x[2]) for x in datasets.g.triples( (entry, NS.prop(field), None) )]
                    cluster[field] = list(set(cluster[field]+field_values))
            cluster["entry_uris"] = list(set(cluster["entry_uris"]))

        export.append(cluster)

    out_file = os.path.join(EXPORT_DIR, "{}.json".format(filename))
    json.dump(export, open(out_file, "w"), indent=4)
