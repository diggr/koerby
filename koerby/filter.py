from pit.prov import Provenance
from tqdm import tqdm 

from .config import PROV_AGENT, NS, DATASET_FILEPATH, MATCHES_FILEPATH, CONFIG
from .rdf_dataset import RdfDataset


def _get_other_dataset(graph, matches_graph, match, row):
    other_row = [ x[2] for x in matches_graph.g.triples( (match, NS.prop("matched"), None) ) if x[2] != row ][0]
    other_dataset = [ x[2] for x in graph.g.triples( (other_row, NS.prop("is_part_of"), None) ) ][0] 
    return other_dataset

def _get_best_match_value(matches_graph, match):
    best_match_values = [ float(x) for x in matches_graph.values(match, NS.prop("has_match_value")) ]
    if len(best_match_values) > 0:
        return max(best_match_values)
    else:
        return None

def _filter_by_match_value(all_matches, matches_graph, threshold=1):
    for match in all_matches:
        best_match_value = _get_best_match_value(matches_graph, match)
        if best_match_value:
            if best_match_value >= threshold:
                yield match


def perfect_match_filter():
    """
    Implements perfect match filter
    """

    print("load source graph ...")
    #graph = load_rdf_dataset()
    graph = RdfDataset(DATASET_FILEPATH, namespace=CONFIG["namespaces"])
    print("load matches graph ...")
    matches_graph = RdfDataset(MATCHES_FILEPATH, namespace=CONFIG["namespaces"])
    #matches_graph = load_rdf_dataset(filepath=MATCHES_FILEPATH)

    print("applying perfect match filter ...")
    deleted_matches = []
    delete_count = 0
    for i, res in enumerate(graph.iter_by_type(NS("DatasetRow"))):
        row = res[0]
        print(i, row)
        #get all matches containing :row:
        all_matches = [ x[2] for x in matches_graph.g.triples( (row, NS.prop("belongs_to_match"), None) ) ]

        #iterate through perfect matches (match value = 1)
        for match in tqdm(_filter_by_match_value(all_matches, matches_graph, threshold=1)):
            #get other match row dataset name
            other_dataset = _get_other_dataset(graph, matches_graph, match, row)
            
            #remove all matches under 1 if other row is from the same dataset
            for match_candidate in all_matches:
                if match != match_candidate and match_candidate not in deleted_matches:

                    #get remove candidate dataset uri
                    candidate_other_dataset = _get_other_dataset(graph, matches_graph, match_candidate, row)    
                    
                    #check if remove candidate belongs to same dataset as other row uri from current match
                    if candidate_other_dataset == other_dataset:
                        candidate_best_match_value = _get_best_match_value(matches_graph, match_candidate)
                        
                        #delete remove candidate from graph if match value is lower than 1.0
                        if candidate_best_match_value < 1:
                            delete_count += 1
                            matches_graph.g.remove( (match_candidate, None, None) )
                            matches_graph.g.remove( (None, NS.prop("belongs_to_match"), match_candidate) )
                            deleted_matches.append(match_candidate)
    

    #save filtered graph + provenance 
    print("Removed matches: {}".format(delete_count))
    print("serialize matches graph ...")
    matches_graph.to_jsonld(MATCHES_FILEPATH)

    prov = Provenance(MATCHES_FILEPATH)
    prov.add(agent=PROV_AGENT, activity="filter_matches", description="applied perfect match filter")
    prov.save()