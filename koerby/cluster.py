"""
Build clusters form matches by grouping all matches connected by their entries

If a custer gets too big, a community detection algorithm can be applied in order
to split it up into smaller clusters
"""

import networkx as nx
import community
from pit.prov import Provenance
from tqdm import tqdm 
from rdflib import URIRef
from collections import defaultdict
from itertools import permutations

from .config import PROV_AGENT, NS, DATASET_FILEPATH, MATCHES_FILEPATH, CONFIG, RDF, CLUSTER_FILEPATH
from .rdf_dataset import RdfDataset

#community detection configuration
RESOLUTION = 0.4
MATCH_THRESHOLD = 1000

class ClusterBuilder(object):

    def __init__(self, threshold=0.0):
        print("load matches graph ...")
        self.match_graph = RdfDataset(MATCHES_FILEPATH, namespace=CONFIG["namespaces"])
        self.cluster_graph = RdfDataset(namespace=CONFIG["namespaces"])

        self.skip = []
        self.threshold = threshold

        # iterate through all matches
        for triple in tqdm(self.match_graph.iter_by_type(NS("Match"))):
            uri = str(triple[0])
            if uri not in self.skip:
                self.create_cluster(uri)
 
        self.cluster_graph.to_ttl(CLUSTER_FILEPATH)
        self.cluster_graph.to_jsonld(CLUSTER_FILEPATH)

    def get_related_matches(self, uri):
        """
        Returns the uris of related matches of match :uri: . 
        A related match is when at least one matched entry belongs to the match.
        """
        related_matches = set()
        entry_uris = [ x[2] for x in self.match_graph.g.triples( (URIRef(uri), NS.prop("matched"), None) ) ]
        
        #new_match_uris = set()

        for entry_uri in entry_uris:

            new_match_uris = { str(x[0]) for x in self.match_graph.g.triples( (None, NS.prop("matched"), entry_uri) ) }
            related_matches = related_matches.union(new_match_uris)
        
        return related_matches

    def find_cluster_matches(self, uris, found, n=0):
        """
        Recursevly iterate through graph to find all related match uris to :uri:
        """

        related = set()
        for uri in uris:
            related = related.union(self.get_related_matches(uri))

        new = related - found
        found = found.union(related)

        if len(new) > 0:
            found = self.find_cluster_matches(new, found)
        
        return found


    def add_cluster_to_graph(self, cluster):
        #print("create cluster")
        cluster_uri = NS.cluster()
        self.cluster_graph.g.add( (cluster_uri, RDF.type, NS("Cluster")) )
        #print("add cluster - match count: {}".format(len(cluster)))
        #print(len(cluster))
        for match in cluster:
            if str(match) in self.skip:
                print("   ")
                print(match)
                print("in skip!!!!")
                raise Error
            #self.match_dict[match] = cluster_uri

            self.cluster_graph.g.add( (cluster_uri, NS.prop("contains_match"), URIRef(match) ) )
            self.cluster_graph.g.add( (URIRef(match), NS.prop("belongs_to_cluster"), cluster_uri ) )

             
    def get_match_uris(self, community):
        match_uris = set()
        for entry1, entry2 in permutations(community,2):
            matches1 = set([ str(x[2]) for x in self.match_graph.g.triples( (URIRef(entry1), NS.prop("belongs_to_match"), None) ) ])
            matches2 = set([ str(x[2]) for x in self.match_graph.g.triples( (URIRef(entry2), NS.prop("belongs_to_match"), None) ) ])
            match_uris = match_uris.union(matches1.intersection(matches2))
        return  match_uris


    def detect_communities(self, cluster):

        #print("detect communities")
        # build network graph
        g = nx.Graph()

        for match in cluster:
            nodes = [ str(x[2]) for x in self.match_graph.g.triples( (URIRef(match), NS.prop("matched"), None) ) ]
            g.add_nodes_from( nodes )
            g.add_edge(nodes[0], nodes[1])

        # apply mudularity algorithm
        partition = community.best_partition(g, resolution=RESOLUTION)
        communities = defaultdict(list)
        for node in g.nodes:
            communities[partition[node]].append(node)

        # yield match uri for each community
        for com in communities.values():
            yield self.get_match_uris(com)


    def create_cluster(self, uri):
   
        #print("find matches")
        found_matches = self.find_cluster_matches([uri], set())

        print("add cluster")
        if len(found_matches) < MATCH_THRESHOLD:
            self.add_cluster_to_graph(found_matches)
        else:
            print("detect communities - cluster size: {}".format(len(found_matches)))
            for cluster_community in self.detect_communities(found_matches):
                print("\t", len(cluster_community))
                self.add_cluster_to_graph(cluster_community)

        self.skip += [ str(x) for x in found_matches ]   