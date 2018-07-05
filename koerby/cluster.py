from pit.prov import Provenance
from tqdm import tqdm 

from rdflib import URIRef

from .config import PROV_AGENT, NS, DATASET_FILEPATH, MATCHES_FILEPATH, CONFIG, RDF, CLUSTER_FILEPATH
from .rdf_dataset import RdfDataset

RELATED_MATCH_QUERY = """
SELECT ?m2
WHERE {{
 	<http://kirby.diggr.link/match/m_039f60280ba91cdb5b6f6b5a56db7a8c> <http://kirby.diggr.link/property/p_matched> ?m .
    ?m <http://kirby.diggr.link/property/p_belongs_to_match> ?m2 .
}} 
"""

class ClusterBuilder (object):

    def __init__(self, threshold):
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
            for match in self.match_graph.g.triples( (None, NS.prop("matched"), entry_uri) ):
                ratio = self.match_graph.g.value(match[0], NS.prop("has_match_value"))
                if ratio >= self.threshold:
                    new_match_uris.add(str(match[0]))

            #new_match_uris = { str(x[0]) for x in self.match_graph.g.triples( (None, NS.prop("matched"), entry_uri) ) }
            related_matches = related_matches.union(new_match_uris)
        
        return related_matches

    def find_cluster_matches(self, uri, found):
        """
        Recursevly iterate through graph to find all related match uris to :uri:
        """

        self.done.add(uri)

        related = self.get_related_matches(uri)

        new = related - found
        print(len(found), len(related), len(new))


        found = found.union(related)

        for match in new:
            if match not in self.done:
                found = found.union(self.find_cluster_matches(match, found))
        
        return found

    def create_cluster(self, uri):
   
        #print("find matches")
        self.done = set()
        found_matches = self.find_cluster_matches(uri, set())

        #print("create cluster")
        cluster_uri = NS.cluster()
        self.cluster_graph.g.add( (cluster_uri, RDF.type, NS("Cluster")) )

        for match in found_matches:
            if str(match) in self.skip:
                print("   ")
                print(match)
                print("in skip!!!!")
                raise "Error"
            #self.match_dict[match] = cluster_uri

            self.cluster_graph.g.add( (cluster_uri, NS.prop("contains_match"), URIRef(match) ) )
            self.cluster_graph.g.add( (URIRef(match), NS.prop("belongs_to_cluster"), cluster_uri ) )

        self.skip += [ str(x) for x in found_matches ]

