import networkx as nx
import community
from pit.prov import Provenance
from tqdm import tqdm 
from rdflib import URIRef
from collections import defaultdict
from itertools import permutations

from .config import PROV_AGENT, NS, DATASET_FILEPATH, MATCHES_FILEPATH, CONFIG, RDF, CLUSTER_FILEPATH
from .rdf_dataset import RdfDataset

RELATED_MATCH_QUERY = """
SELECT ?m2
WHERE {{
 	<http://kirby.diggr.link/match/m_039f60280ba91cdb5b6f6b5a56db7a8c> <http://kirby.diggr.link/property/p_matched> ?m .
    ?m <http://kirby.diggr.link/property/p_belongs_to_match> ?m2 .
}} 
"""

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

        for entry_uri in entry_uris:
            new_match_uris = set()
            # for match in self.match_graph.g.triples( (None, NS.prop("matched"), entry_uri) ):
            #     ratio = self.match_graph.g.value(match[0], NS.prop("has_match_value"))
            #     if ratio >= self.threshold:
            #         new_match_uris.add(str(match[0]))

            new_match_uris = { str(x[0]) for x in self.match_graph.g.triples( (None, NS.prop("matched"), entry_uri) ) }
            related_matches = related_matches.union(new_match_uris)
        
        return related_matches

    def find_cluster_matches(self, uri, found):
        """
        Recursevly iterate through graph to find all related match uris to :uri:
        """

        self.done.add(uri)

        related = self.get_related_matches(uri)

        new = related - found
        #print(len(found), len(related), len(new))

        found = found.union(related)

        for match in new:
            if match not in self.done:
                found = found.union(self.find_cluster_matches(match, found))
        
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
                raise "Error"
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
        partition = community.best_partition(g, resolution=1)
        communities = defaultdict(list)
        for node in g.nodes:
            communities[partition[node]].append(node)

        # yield match uri for each community
        for com in communities.values():
            yield self.get_match_uris(com)


    def create_cluster(self, uri):
   
        #print("find matches")
        self.done = set()
        found_matches = self.find_cluster_matches(uri, set())
        print("add")
        if len(found_matches) < 500:
            self.add_cluster_to_graph(found_matches)
        else:
            print("detect communities {}".format(len(found_matches)))
            for cluster_community in self.detect_communities(found_matches):
                print(len(cluster_community))
                self.add_cluster_to_graph(cluster_community)

        self.skip += [ str(x) for x in found_matches ]   