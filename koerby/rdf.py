import rdflib
import json
import os
import uuid
import hashlib
import timeit
import multiprocessing
import math
from collections import defaultdict
from tqdm import tqdm
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, FOAF, RDFS
from datetime import datetime
import random

from .config import CONFIG, DATA_DIR, PROJECT_NAME, DATASET_FILEPATH, MATCHES_FILEPATH, KIRBY_FILEPATH, EXPORT_DIR, PROV_AGENT
from .csv import read_csv, write_csv
from .utils import source_files
from pit.prov import Provenance

from .rdf_dataset import RdfDataset

PROCESS_COUNT = 8

# KIRBY NAMESPACES
KIRBY_ENTRY = "http://kirby.diggr.link/entry"
KIRBY_DATASET = "http://kirby.diggr.link/dataset"
KIRBY_PROP = Namespace("http://kirby.diggr.link/property/")
KIRBY_MATCH_NS = Namespace("http://kirby.diggr.link/match/")

# KIRBY ENTITY TYPES
KIRBY_CORE = Namespace("http://kirby.diggr.link/")
SOURCE_DATASET = KIRBY_CORE.Dataset
SOURCE_DATASET_ROW = KIRBY_CORE.DatasetRow
KIRBY_MATCH = KIRBY_CORE.Match

# JSON-LD CONTEXT
CONTEXT = {
    "rdf": str(RDF),
    "kirby": str(KIRBY_CORE),
    "kirby_ds": KIRBY_DATASET,
    "kirby_prop": str(KIRBY_PROP)
}




def build_kirby_entry_uri():
    uri = URIRef("{}/{}_{}".format(KIRBY_ENTRY, PROJECT_NAME, uuid.uuid4().hex))
    return uri

def build_dataset_uri(dataset_name):
    uri = URIRef("{}/{}".format(KIRBY_DATASET, dataset_name))
    return uri

def build_dataset_row_uri(dataset_name, row):
    uri = URIRef("{}/{}/{}".format(KIRBY_DATASET, dataset_name, row))
    return uri

def build_property_uri(property_name):
    uri = URIRef("{}p_{}".format(KIRBY_PROP, property_name))
    return uri



# def get_match_uri_value(graph, row, match):

#     query = """
#     SELECT ?match ?value
#     WHERE {{
#         ?match <http://kirby.diggr.link/property/matched> <{match_1}> ;
#                 <http://kirby.diggr.link/property/matched> <{match_2}> .
#         ?match <http://kirby.diggr.link/property/has_match_value> ?value .
#     }}
#     """

#     res = graph.query(query.format(match_1=str(row), match_2=str(match)))
#     if len(res) > 0:
#         print(res[0])
#         return res[0]
#     else:
#         return None, None

# def find_matches(graph, row, matches):
#     old_len = len(matches)
#     matches += get_values(graph, row, KIRBY_PROP.matched_with)
#     matches = list(set(matches))
#     if old_len == len(matches):
#         return matches
#     for match in matches:
#         matches = find_matches(graph, match, matches)
#     return matches


# def generate_kirby_dataset():
#     dataset_graph = load_rdf_dataset()
#     matches_graph = load_rdf_dataset(filepath=MATCHES_FILEPATH)
#     kirby_graph = Graph()

#     graph = dataset_graph + matches_graph

#     skip = set()

#     for row in iter_by_type(graph, SOURCE_DATASET_ROW):
#         if row not in skip:

#             kirby_entry_uri = build_kirby_entry_uri()
#             kirby_graph.add( (kirby_entry_uri, RDF.type, KIRBY_CORE.KirbyEntry ) )
#             kirby_graph.add( (kirby_entry_uri, KIRBY_PROP.contains, row ) )
#             kirby_graph.add( (row, KIRBY_PROP.is_part_of, kirby_entry_uri ) )
            
#             matches = []
#             #matches = get_values(graph, row, KIRBY_PROP.matched_with)    
#             matches = find_matches(graph, row, matches)
#             for match in matches:
#                 kirby_graph.add( (kirby_entry_uri, KIRBY_PROP.contains, match ) )
#                 kirby_graph.add( (match, KIRBY_PROP.is_part_of, kirby_entry_uri ) )
#             skip =  skip ^ set(matches)

#     with open(KIRBY_FILEPATH, "wb") as f:
#         f.write(kirby_graph.serialize(format="json-ld", context=CONTEXT))

#     #add provenacne information
#     prov = Provenance(KIRBY_FILEPATH)
#     prov.add(
#         agent=PROV_AGENT,
#         activity="generate_kirby_entries",
#         description="build kirby entries based on dataset matches"
#     )
#     prov.add_sources( [ MATCHES_FILEPATH ] )
#     prov.save()

# def export_kirby_dataset(export_config):

#     if "order_by" in export_config:
#         order_by = export_config["order_by"]
#     else:
#         order_by = None

#     dataset_graph = load_rdf_dataset()
#     matches_graph = load_rdf_dataset(filepath=MATCHES_FILEPATH)
#     kirby_graph = load_rdf_dataset(filepath=KIRBY_FILEPATH)

#     graph = dataset_graph + matches_graph + kirby_graph

#     export_dataset = []

#     for entry in iter_by_type(graph, KIRBY_CORE.KirbyEntry):

#         export_row = defaultdict(list)

#         dataset_rows = get_values(graph, entry, KIRBY_PROP.contains)

#         for row in dataset_rows:
#             for prop in export_config["properties"]:
#                 export_row[prop] += [ str(x) for x in get_values(graph, row, build_property_uri(prop)) ]

#         for x in export_row.keys():
#             export_row[x] = "; ".join(list(set(export_row[x])))

#         export_row["kirby_uri"] = str(entry)
#         export_dataset.append(export_row)
    
#     if order_by:
#         export_dataset = sorted(export_dataset, key=lambda x: x[order_by])
    
#     export_filepath = os.path.join(EXPORT_DIR, export_config["name"])
#     write_csv(export_dataset, export_filepath)
#     prov = Provenance(export_filepath)
#     prov.add(
#         agent=PROV_AGENT,
#         activity="export_dataset",
#         description="export csv with export config <{}>".format(export_config)
#     )
#     prov.add_sources( [ KIRBY_FILEPATH, MATCHES_FILEPATH, DATASET_FILEPATH ] )
#     prov.save()    
#     #with open("test.json", "w", encoding="utf-8") as f:
#     #    json.dump(export_dataset, f, indent=4)

# def iter_property_values(graph, prop):
#     results = graph.triples( (None, prop, None) )
#     for row in results:
#         yield row[0], row[2]


# def enrich_dataset(input, enrich_func, skip=None):

#     print("enrich dataset <{}> ...".format(input))
#     graph = load_rdf_dataset()
    
#     for sbj, obj in tqdm(iter_property_values(graph, build_property_uri(input))):
#         do_it = True
#         if skip:
#             if len(get_values(graph, sbj, build_property_uri(skip))) > 0:
#                 do_it = False
#         if do_it:
#             results = enrich_func(str(obj))
            
#             for new_prop, values in results.items():
#                 for value in values:
#                     if value:
#                         graph.add( (sbj, build_property_uri(new_prop), Literal(value)) )
    
#     with open(DATASET_FILEPATH, "wb") as f:
#         f.write(graph.serialize(format="json-ld", context=CONTEXT))    

def transform_source_property(prop, transform_func):

    print("transform property <{}> ...".format(prop))
    prop_uri = build_property_uri(prop)
    graph = RdfDataset(DATASET_FILEPATH)
    graph.transform_property(prop, tranform_func)

