import rdflib
import json
import os
import uuid
import hashlib
import timeit
import multiprocessing
from collections import defaultdict
from tqdm import tqdm
from rdflib import Graph, Namespace, URIRef, Literal
from rdflib.namespace import RDF, FOAF, RDFS
from datetime import datetime
import random

from .config import DATA_DIR, PROJECT_NAME, DATASET_FILEPATH, MATCHES_FILEPATH, KIRBY_FILEPATH, EXPORT_DIR, PROV_AGENT
from .csv import read_csv, write_csv
from .utils import source_files
from .pit import Provenance

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

def csv2rdf(filepath, name):
    """
    Convertes csv file into an rdf graph (using standard koerby namespace)
    """
    print("csv2rdf <{}> ...".format(name))
    graph = Graph()

    dataset = read_csv(filepath)
    dataset_uri = build_dataset_uri(name)
    graph.add( (dataset_uri, RDF.type, SOURCE_DATASET) )

    for row_nr, row in tqdm(enumerate(dataset)):
        row_uri = build_dataset_row_uri(name, row_nr)
        graph.add( (row_uri, RDF.type, KIRBY_CORE.DatasetRow) )
        graph.add( (dataset_uri, KIRBY_PROP.contains, row_uri) )
        graph.add( (row_uri, KIRBY_PROP.is_part_of, dataset_uri) )
        for column, value in row.items():
            if not "Unnamed" in column:
                property_uri = build_property_uri(column)
                if value:
                    graph.add( (row_uri, property_uri, Literal(value)) )
    return graph

def json2rdf(filepath, name):
    """
    Converts (flat) json file into an rdf graph (using standard koerby namespace)
    """
    print("json2rdf <{}> ...".format(name))
    graph = Graph()

    dataset = json.load(open(filepath))
    dataset_uri = build_dataset_uri(name)
    graph.add( (dataset_uri, RDF.type, SOURCE_DATASET) )

    for row_nr, row in tqdm(dataset.items()):
        row_uri = build_dataset_row_uri(name, row_nr)
        graph.add( (row_uri, RDF.type, KIRBY_CORE.DatasetRow) )
        graph.add( (dataset_uri, KIRBY_PROP.contains, row_uri) )
        graph.add( (row_uri, KIRBY_PROP.is_part_of, dataset_uri) )
        for key, value in row.items():
            property_uri = build_property_uri(key)
            if type(value) == list:
                for v in value:
                    if v:
                        graph.add( (row_uri, property_uri, Literal(v)) )
            else:
                if value:
                    graph.add( (row_uri, property_uri, Literal(value)) )

    return graph

def generate_rdf_dataset():
    """
    Builds an RDF dataset for all csv files in source directory. 
    Serializes result as json-ld file into data directory.
    """
    graph = Graph()

    for filepath, file_name, file_type in source_files():
        if file_type == "csv":
            graph += csv2rdf(filepath, file_name.replace(".csv", ""))
        elif file_type == "json":
            graph += json2rdf(filepath, file_name.replace(".json", ""))
    
    dataset_file = os.path.join(DATA_DIR, "{}.json".format(PROJECT_NAME))
    with open(dataset_file, "wb") as f:
        f.write(graph.serialize(format="json-ld", context=CONTEXT))

    #add provenance
    prov = Provenance(dataset_file)
    prov.add(
        agent=PROV_AGENT, 
        activity="build_dataset_rdf", 
        description="Build source rdf dataset from source csv files")
    prov.add_sources([ fp for fp, fn, ft in source_files() ])
    prov.save()


def load_rdf_dataset(filepath=DATASET_FILEPATH):
    """
    Loads RDF dataset and returns rdflib graph object.
    """
    #dataset_file = os.path.join(DATA_DIR, "{}.json".format(DATASET_NAME))
    with open(filepath, encoding="utf-8") as f:
        graph_data = json.load(f)
    graph = Graph().parse(data=json.dumps(graph_data["@graph"]), format="json-ld", context=CONTEXT)

    return graph

def match_literal(graph, prop, values, std=None):
    """
    Returns all URIs which have property :prop: of (one of) literal :values: .
    If std is set, literal and value will be standardized using function :std: .
    """
    prop_uri = build_property_uri(prop)    
    results = graph.triples ( (None, prop_uri, None ) )
    candidates = [ (x[0], str(x[2])) for x in results ]
    if not std:
        matches = set( [x[0] for x in candidates if x[1] in values ] )
    else:
        values = [ std(x) for x in values]
        matches = set( [x[0] for x in candidates if std(x[1]) in values] )
    return list(matches)

def match_cmp(graph, candidate, props, values, cmp=None):
    """
    Compares values of properties :props: of all candidates with :values: using a costum compare function.
    Returns match probability between 0 and 1.
    """
    raise NotImplementedError

def get_values(graph, subject, prop):
    """
    Return a list of all values of property :prop: for subject uri :subject:
    """
    #prop_uri = build_property_uri(prop)
    results = graph.triples( (subject, prop, None) )
    return [ str(x[2]) for x in results]


def iter_by_type(graph, rdf_type):
    results = graph.triples( (None, RDF.type, rdf_type) )
    for row in results:
        yield row[0]  

def get_uris_by_type(graph, rdf_type):
    results = graph.triples( (None, RDF.type, rdf_type) )
    return [ x[0] for x in results ]


def iter_deterministic_rules(ruleset):
    if "deterministic" in ruleset:
        for det_rule in ruleset["deterministic"]:
            yield det_rule

def build_match_uri(row, match):
    comb = sorted([str(row), str(match)])
    uid = hashlib.md5("".join(comb).replace(KIRBY_DATASET, "").encode("utf-8")).hexdigest()

    return KIRBY_MATCH_NS["m_{}".format(uid)]


def get_match_uri_value(graph, row, match):

    query = """
    SELECT ?match ?value
    WHERE {{
        ?match <http://kirby.diggr.link/property/matched> <{match_1}> ;
                <http://kirby.diggr.link/property/matched> <{match_2}> .
        ?match <http://kirby.diggr.link/property/has_match_value> ?value .
    }}
    """

    res = graph.query(query.format(match_1=str(row), match_2=str(match)))
    if len(res) > 0:
        print(res[0])
        return res[0]
    else:
        return None, None

def update_match_value(graph, match_uri, old_value, new_value):
    graph.remove( (match_uri, KIRBY_PROP.has_match_value, Literal(old_value)) )
    graph.add( (match_uri, KIRBY_PROP.has_match_value, Literal(new_value)) )


def add_match_to_graph(graph, row, match, value):

    match_uri = build_match_uri(row, match)
    graph.add( (match_uri, RDF.type, KIRBY_MATCH) ) 
    graph.add( (match_uri, KIRBY_PROP.matched, row) )
    graph.add( (match_uri, KIRBY_PROP.matched, match) )
    graph.add( (row, KIRBY_PROP.belongs_to_match, match_uri) )
    graph.add( (match, KIRBY_PROP.belongs_to_match, match_uri) )
    graph.add( (match_uri, KIRBY_PROP.has_match_value, Literal(value)) )


def start_generate_matches(match_config):

    step = 10000
    offset = 0
    print(datetime.now().isoformat())
    print("load dataset ...")
    graph = load_rdf_dataset()
    all_rows = get_uris_by_type(graph, SOURCE_DATASET_ROW)

    match_graph = Graph()


    print("start matching process ...")
    # threads=[]
    jobs = []
    pipe_list = []
    for i in range(8):
        recv_end, send_end = multiprocessing.Pipe(False)
        chunk = all_rows[offset:(offset+step)]
        p = multiprocessing.Process(target=generate_matches, args=(match_config, chunk, graph, i, send_end))
        jobs.append(p)
        pipe_list.append(recv_end)
        p.start()
        offset+=step
    
    for receiver in pipe_list:
        match_graph += receiver.recv()

    for proc in jobs:
        proc.join()
    
    dataset_file = os.path.join(DATA_DIR, "{}_matches.json".format(PROJECT_NAME))
    with open(MATCHES_FILEPATH, "wb") as f:
        f.write(match_graph.serialize(format="json-ld", context=CONTEXT))

    # #add provenance file
    # prov = Provenance(MATCHES_FILEPATH)
    # prov.add(agent=PROV_AGENT, activity="generate_matches", description="match datasets with config <{}>".format(match_config))
    # prov.add_sources([ DATASET_FILEPATH ])
    # prov.save()

    print(datetime.now().isoformat())
    print( len([x for x in match_graph.triples( (None, None, None) ) ]) )


# def wrapper(func, *args, **kwargs):
#     def wrapped():
#         return func(*args, **kwargs)
#     return wrapped

# def time_get_values():
#     print("load dataset graph")
#     graph = load_rdf_dataset()
#     #print("select random data row")
#     #all_rows = get_uris_by_type(graph, SOURCE_DATASET_ROW)
#     #row = random.choice(all_rows)
#     print("time get values function")
#     row = URIRef("http://kirby.diggr.link/dataset/giantbomb/3030-25546")
#     wrapped_func = wrapper(get_values, graph, row, build_property_uri("platforms"))
#     wrapped_func2 = wrapper(get_values2, graph, row, build_property_uri("platforms"))
#     print(timeit.timeit(wrapped_func, number=50))
#     print(timeit.timeit(wrapped_func2, number=50))


def generate_matches(match_config, dataset, graph, thread_no, send_end):
    """
    Builds a graph conataining matches based on the rules defined in :matching_config: .
    """
    skip = []

    #print("finding matches ...")
    match_graph = Graph()

    for i, row in enumerate(dataset):

        start_time = timeit.default_timer()

        #apply match rules
        for ruleset in match_config:

            match_candidates = []
            values = []

            #apply deterministic rules to find match candidates
            for deter_rule in iter_deterministic_rules(ruleset):

                for prop in deter_rule["fields"]:
                    prop_values = get_values(graph, row, build_property_uri(prop))
                    match_candidates += match_literal(graph, prop, prop_values, deter_rule["std_func"])
                    values += prop_values
            
            #apply probabilistic rules ot find match probability value
            matches = []
            match_candidates = [ x for x in match_candidates if x not in skip ]

            if "probabilistic" in ruleset:
                cmp_func = ruleset["probabilistic"]["cmp_func"]
 
            for candidate in set(match_candidates):
                if candidate != row:
                    #if no probabilitic rule is set, all candidtes receive match value 1 (perfect match)
                    if not "probabilistic" in ruleset:
                        add_match_to_graph(match_graph, row, candidate, 1)
                        matches.append(candidate)
                    else:
                        candidate_values = []
                        for prop in ruleset["probabilistic"]["fields"]:
                            candidate_values += get_values(graph, candidate, build_property_uri(prop))

                        cmp_value = cmp_func(values, candidate_values)

                        if cmp_value > 0.7:
                            add_match_to_graph(match_graph, row, candidate, cmp_value)
                            
            skip.append(row)

        stop_time = timeit.default_timer()

        #print("process {} >> iteration {} / {}: \t {} \t\t {} ".format(thread_no, i, len(dataset), stop_time-start_time), ", ".join(values))
        if stop_time - start_time > 10:
            print("process {} >> iteration {} / {}: \t {} \t\t {} ".format(thread_no, i, len(dataset), stop_time-start_time, ", ".join(values)))
        else:
            print("process {} >> iteration {} / {}: \t {}".format(thread_no, i, len(dataset), stop_time-start_time))
        #print(stop_time-start_time)

    send_end.send(match_graph)


def find_matches(graph, row, matches):
    old_len = len(matches)
    matches += get_values(graph, row, KIRBY_PROP.matched_with)
    matches = list(set(matches))
    if old_len == len(matches):
        return matches
    for match in matches:
        matches = find_matches(graph, match, matches)
    return matches


def generate_kirby_dataset():
    dataset_graph = load_rdf_dataset()
    matches_graph = load_rdf_dataset(filepath=MATCHES_FILEPATH)
    kirby_graph = Graph()

    graph = dataset_graph + matches_graph

    skip = set()

    for row in iter_by_type(graph, SOURCE_DATASET_ROW):
        if row not in skip:

            kirby_entry_uri = build_kirby_entry_uri()
            kirby_graph.add( (kirby_entry_uri, RDF.type, KIRBY_CORE.KirbyEntry ) )
            kirby_graph.add( (kirby_entry_uri, KIRBY_PROP.contains, row ) )
            kirby_graph.add( (row, KIRBY_PROP.is_part_of, kirby_entry_uri ) )
            
            matches = []
            #matches = get_values(graph, row, KIRBY_PROP.matched_with)    
            matches = find_matches(graph, row, matches)
            for match in matches:
                kirby_graph.add( (kirby_entry_uri, KIRBY_PROP.contains, match ) )
                kirby_graph.add( (match, KIRBY_PROP.is_part_of, kirby_entry_uri ) )
            skip =  skip ^ set(matches)

    with open(KIRBY_FILEPATH, "wb") as f:
        f.write(kirby_graph.serialize(format="json-ld", context=CONTEXT))

    #add provenacne information
    prov = Provenance(KIRBY_FILEPATH)
    prov.add(
        agent=PROV_AGENT,
        activity="generate_kirby_entries",
        description="build kirby entries based on dataset matches"
    )
    prov.add_sources( [ MATCHES_FILEPATH ] )
    prov.save()

def export_kirby_dataset(export_config):

    if "order_by" in export_config:
        order_by = export_config["order_by"]
    else:
        order_by = None

    dataset_graph = load_rdf_dataset()
    matches_graph = load_rdf_dataset(filepath=MATCHES_FILEPATH)
    kirby_graph = load_rdf_dataset(filepath=KIRBY_FILEPATH)

    graph = dataset_graph + matches_graph + kirby_graph

    export_dataset = []

    for entry in iter_by_type(graph, KIRBY_CORE.KirbyEntry):

        export_row = defaultdict(list)

        dataset_rows = get_values(graph, entry, KIRBY_PROP.contains)

        for row in dataset_rows:
            for prop in export_config["properties"]:
                export_row[prop] += [ str(x) for x in get_values(graph, row, build_property_uri(prop)) ]

        for x in export_row.keys():
            export_row[x] = "; ".join(list(set(export_row[x])))

        export_row["kirby_uri"] = str(entry)
        export_dataset.append(export_row)
    
    if order_by:
        export_dataset = sorted(export_dataset, key=lambda x: x[order_by])
    
    export_filepath = os.path.join(EXPORT_DIR, export_config["name"])
    write_csv(export_dataset, export_filepath)
    prov = Provenance(export_filepath)
    prov.add(
        agent=PROV_AGENT,
        activity="export_dataset",
        description="export csv with export config <{}>".format(export_config)
    )
    prov.add_sources( [ KIRBY_FILEPATH, MATCHES_FILEPATH, DATASET_FILEPATH ] )
    prov.save()    
    #with open("test.json", "w", encoding="utf-8") as f:
    #    json.dump(export_dataset, f, indent=4)

def iter_property_values(graph, prop):
    results = graph.triples( (None, prop, None) )
    for row in results:
        yield row[0], row[2]


def enrich_dataset(input, enrich_func, skip=None):

    print("enrich dataset <{}> ...".format(input))
    graph = load_rdf_dataset()
    
    for sbj, obj in tqdm(iter_property_values(graph, build_property_uri(input))):
        do_it = True
        if skip:
            if len(get_values(graph, sbj, build_property_uri(skip))) > 0:
                do_it = False
        if do_it:
            results = enrich_func(str(obj))
            
            for new_prop, values in results.items():
                for value in values:
                    if value:
                        graph.add( (sbj, build_property_uri(new_prop), Literal(value)) )
    
    with open(DATASET_FILEPATH, "wb") as f:
        f.write(graph.serialize(format="json-ld", context=CONTEXT))    

def transform_source_property(prop, transform_func):

    print("transform property <{}> ...".format(prop))
    prop_uri = build_property_uri(prop)
    graph = load_rdf_dataset()
    for sbj, obj in tqdm(iter_property_values(graph, build_property_uri(prop))):
        new_value = transform_func(str(obj))
        graph.remove( (sbj, prop_uri, obj ) )
        graph.add( (sbj, prop_uri, Literal(new_value)) )
    
    with open(DATASET_FILEPATH, "wb") as f:
        f.write(graph.serialize(format="json-ld", context=CONTEXT))    
