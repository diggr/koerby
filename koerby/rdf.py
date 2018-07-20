"""
Contains dataset build functions with uses genertes an RDF graph from the files in the source folder




"""

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

from .config import CONFIG, DATA_DIR, PROJECT_NAME, DATASET_FILEPATH, MATCHES_FILEPATH, EXPORT_DIR, PROV_AGENT, NS
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

def generate_rdf_dataset():
    """
    Generate RDF graph from the csv and json files in the source directory
    """

    dataset = RdfDataset(namespace=CONFIG["namespaces"])
    sources = []
    filenames = []
    for filepath, filename, filetype in source_files():
        print("generate rdf dataset from <{}> ...".format(filename))
        if filename.endswith(".csv"):
            dataset.read_csv(filepath)
        elif filename.endswith(".json"):
            dataset.read_json(filepath)
        sources.append(filepath)
        filenames.append(filename)
    export_filepath = os.path.join(DATA_DIR, "{}.json".format(PROJECT_NAME))
    dataset.to_jsonld(export_filepath)

    prov = Provenance(export_filepath)
    prov.add(
        agent=PROV_AGENT,
        activity="rdf_dataset_generation",
        description="Build RDF dataset from source datasets <{}>".format(", ".join(filenames))
    )
    prov.add_sources(sources)
    prov.save()



def transform_source_property(prop, transform_func):

    print("transform property <{}> ...".format(prop))
    prop_uri = build_property_uri(prop)
    graph = RdfDataset(DATASET_FILEPATH)
    graph.transform_property(prop, tranform_func)

