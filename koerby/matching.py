import math
import timeit
import multiprocessing
from rdflib import Graph, RDF, Literal
from datetime import datetime
from pit.prov import Provenance

#from .rdf import load_rdf_dataset, get_uris_by_type, get_values, match_literal
#from .rdf import SOURCE_DATASET_ROW, CONTEXT, MATCHES_FILEPATH, DATASET_FILEPATH
#from .rdf import KIRBY_PROP, KIRBY_MATCH
from .config import PROV_AGENT, NS, DATASET_FILEPATH, MATCHES_FILEPATH, CONFIG
from .rdf_dataset import RdfDataset

# match config
PROCESS_COUNT = 8

def match_datasets(match_config, processes=PROCESS_COUNT):    
    """
    Splits dataset into n (= :PROCESS_COUNT: ) chunks and starts
    a matching process for each count
    """
    match_graph = Graph()

    print(datetime.now().isoformat())
    
    print("load dataset ...")

    graph = RdfDataset(DATASET_FILEPATH, namespace=CONFIG["namespaces"])

    all_entries = graph.uris(rdf_type=NS("DatasetRow"))

    print("start matching process ...")

    #multiprocessing setup
    offset = 0
    step = math.ceil(len(all_entries) / PROCESS_COUNT)
    #step = 1000
    jobs = []
    pipe_list = []

    #start multiprocesses
    for i in range(PROCESS_COUNT):
        recv_end, send_end = multiprocessing.Pipe(False)
        chunk = all_entries[offset:(offset + step)]
        p = multiprocessing.Process(target=_generate_matches, args=(match_config, chunk, graph, i, send_end))
        jobs.append(p)
        pipe_list.append(recv_end)
        p.start()
        offset+=step
    
    #receive results from multiprocessing
    for receiver in pipe_list:
        match_graph += receiver.recv()

    #wait until all processes have finished
    for proc in jobs:
        proc.join()
    
    #write results
    with open(MATCHES_FILEPATH, "wb") as f:
        f.write(match_graph.serialize(format="json-ld", context=NS.context))

    #add provenance file
    prov = Provenance(MATCHES_FILEPATH)
    prov.add(agent=PROV_AGENT, activity="generate_matches", description="match datasets with config <{}>".format(match_config))
    prov.add_sources([ DATASET_FILEPATH ])
    prov.save()

    print(datetime.now().isoformat())

def _add_match_to_graph(graph, entry, match, value):
    """
    Write match results as rdf triples into :graph:
    """
    match_uri = NS.match(entry, match)
    graph.add( (match_uri, RDF.type, NS("Match")) ) 
    graph.add( (match_uri, NS.prop("matched"), entry) )
    graph.add( (match_uri, NS.prop("matched"), match) )
    graph.add( (entry, NS.prop("belongs_to_match"), match_uri) )
    graph.add( (match, NS.prop("belongs_to_match"), match_uri) )
    graph.add( (match_uri, NS.prop("has_match_value"), Literal(value)) )

def _iter_deterministic_rules(ruleset):
    if "deterministic" in ruleset:
        for det_rule in ruleset["deterministic"]:
            yield det_rule

def _generate_matches(match_config, dataset, graph, thread_no, send_end):
    """
    Builds a graph conataining matches based on the rules defined in :matching_config: .
    """

    match_graph = Graph()

    for i, row in enumerate(dataset):

        start_time = timeit.default_timer()

        #apply match rules
        for ruleset in match_config:
            match_candidates = []

            #apply deterministic rules to find match candidates
            for deter_rule in _iter_deterministic_rules(ruleset):

                for prop in deter_rule["fields"]:
                    prop_values = graph.values(row, NS.prop(prop))

                    #ignore specific values if defined in :match_config:co
                    if "ignore_values" in deter_rule:
                        prop_values = list(set(prop_values)-set(deter_rule["ignore_values"]))

                    match_candidates += graph.match_literal(prop, prop_values, deter_rule["std_func"])
            
            #apply probabilistic rules ot find match probability value
            matches = []
            
            if "probabilistic" in ruleset:
                cmp_func = ruleset["probabilistic"]["cmp_func"]

                values = []
                for prop in ruleset["probabilistic"]["fields"]:
                    prop_values = graph.values(row, NS.prop(prop))
                    values += prop_values

            for candidate in set(match_candidates):
                if candidate != row:
                    #if no probabilitic rule is set, all candidtes receive match value 1 (perfect match)
                    if not "probabilistic" in ruleset:
                        _add_match_to_graph(match_graph, row, candidate, 1)
                        matches.append(candidate)
                    else:
                        candidate_values = []
                        for prop in ruleset["probabilistic"]["fields"]:
                            candidate_values += graph.values(candidate, NS.prop(prop))

                        cmp_value = cmp_func(values, candidate_values)

                        if cmp_value > 0.7:
                            _add_match_to_graph(match_graph, row, candidate, cmp_value)

        # timing stuff
        stop_time = timeit.default_timer()
        if stop_time - start_time > 10:
            print("process {} >> iteration {} / {}: \t {} \t\t {} ".format(thread_no, i, len(dataset), stop_time-start_time, ", ".join(values)))
        else:
            print("process {} >> iteration {} / {}: \t {}".format(thread_no, i, len(dataset), stop_time-start_time))

    send_end.send(match_graph)