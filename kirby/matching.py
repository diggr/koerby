import math
import timeit
import multiprocessing
from rdflib import Graph, RDF
from datetime import datetime
from pit.prov import Provenance

from .rdf import load_rdf_dataset, get_uris_by_type, get_values, match_literal
from .rdf import SOURCE_DATASET_ROW, CONTEXT, MATCHES_FILEPATH
from .rdf import KIRBY_PROP, KIRBY_MATCH
from .config import PROV_AGENT

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
    graph = load_rdf_dataset()
    all_rows = get_uris_by_type(graph, SOURCE_DATASET_ROW)

    print("start matching process ...")

    #multiprocessing setup
    offset = 0
    step = math.ceil(len(all_rows)/PROCESS_COUNT)
    #step = 500

    jobs = []
    pipe_list = []

    #start multiprocesses
    for i in range(PROCESS_COUNT):
        recv_end, send_end = multiprocessing.Pipe(False)
        chunk = all_rows[offset:(offset+step)]
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
        f.write(match_graph.serialize(format="json-ld", context=CONTEXT))

    #add provenance file
    prov = Provenance(MATCHES_FILEPATH)
    prov.add(agent=PROV_AGENT, activity="generate_matches", description="match datasets with config <{}>".format(match_config))
    prov.add_sources([ DATASET_FILEPATH ])
    prov.save()

    print(datetime.now().isoformat())
    #print( len([x for x in match_graph.triples( (None, None, None) ) ]) )


def _add_match_to_graph(graph, row, match, value):
    """
    Write match results as rdf triples into :graph:
    """
    match_uri = build_match_uri(row, match)
    graph.add( (match_uri, RDF.type, KIRBY_MATCH) ) 
    graph.add( (match_uri, KIRBY_PROP.matched, row) )
    graph.add( (match_uri, KIRBY_PROP.matched, match) )
    graph.add( (row, KIRBY_PROP.belongs_to_match, match_uri) )
    graph.add( (match, KIRBY_PROP.belongs_to_match, match_uri) )
    graph.add( (match_uri, KIRBY_PROP.has_match_value, Literal(value)) )

def _generate_matches(match_config, dataset, graph, thread_no, send_end):
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


            #apply deterministic rules to find match candidates
            for deter_rule in iter_deterministic_rules(ruleset):

                for prop in deter_rule["fields"]:
                    prop_values = get_values(graph, row, build_property_uri(prop))
                    match_candidates += match_literal(graph, prop, prop_values, deter_rule["std_func"])
            
            #apply probabilistic rules ot find match probability value
            matches = []
            #match_candidates = [ x for x in match_candidates if x not in skip ]

            if "probabilistic" in ruleset:
                cmp_func = ruleset["probabilistic"]["cmp_func"]

                values = []
                for prop in ruleset["probabilistic"]["fields"]:
                    prop_values = get_values(graph, row, build_property_uri(prop))
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
                            candidate_values += get_values(graph, candidate, build_property_uri(prop))

                        cmp_value = cmp_func(values, candidate_values)

                        if cmp_value > 0.7:
                            _add_match_to_graph(match_graph, row, candidate, cmp_value)
                            
            skip.append(row)

        stop_time = timeit.default_timer()

        #print("process {} >> iteration {} / {}: \t {} \t\t {} ".format(thread_no, i, len(dataset), stop_time-start_time), ", ".join(values))
        if stop_time - start_time > 10:
            print("process {} >> iteration {} / {}: \t {} \t\t {} ".format(thread_no, i, len(dataset), stop_time-start_time, ", ".join(values)))
        else:
            print("process {} >> iteration {} / {}: \t {}".format(thread_no, i, len(dataset), stop_time-start_time))
        #print(stop_time-start_time)

    send_end.send(match_graph)