import json
from tqdm import tqdm
from rdflib import Graph, RDF, URIRef, Literal
from .namespaces import KirbyNamespace
#from .config import NS
from .csv import read_csv

#NS = KirbyNamespace()

class RdfDataset():
    """
    Wrapper class for rdflib graph; for convenience's sake
    """

    def __init__(self, filepath=None, namespace=None):
        self.g = Graph()
        self.ns = KirbyNamespace(namespace)
        if filepath:
            # load graph from file
            self.load(filepath)


    #
    # accessing and iterating graph data functions
    #
    
    def iter_by_type(self, type_):
        """
        Generator yielding all uris von of type :type_:
        """
        results = self.g.triples( (None, RDF.type, type_) )
        for entry in results:
            yield entry

    def iter_by_prop(self, prop):
        results = self.g.triples( (None, prop, None) )
        for entry in results:
            yield entry

    def values(self, subject, prop):
        """
        Returns a list of all values of property :prop: for subject :subject:
        """

        results = self.g.triples( (subject, prop, None) )
        return [ str(x[2]) for x in results]
    
    def uris(self, rdf_type):
        return [ x[0] for x in self.iter_by_type(rdf_type) ]
    
    def uris_with_prop(self, prop):
        return [ x[0] for x in self.iter_by_prop(prop)]

    def match_literal(self, prop, values, std=None):
        """
        Returns all URIs which have property :prop: of (one of) literal :values: .
        If std is set, literal and value will be standardized using function :std: .
        """
        prop_uri = self.ns.prop(prop)    
        results = self.g.triples ( (None, prop_uri, None ) )
        candidates = [ (x[0], str(x[2])) for x in results ]
        if not std:
            matches = set( [x[0] for x in candidates if x[1] in values ] )
        else:
            values = [ std(x) for x in values]
            matches = set( [x[0] for x in candidates if std(x[1]) in values] )
        return list(matches)
    
    def iter_property_vaues(self, prop):
        results = self.g.triples ( (None, prop, None) )
        for result in results:
            yield (result[0], result[2])


    #
    # transform graph
    #

    def transform_property(self, prop, transform_func):

        print("transform property <{}> ...".format(prop))
        prop_uri = self.ns.prop(prop)
    
        for sbj, obj in tqdm(iter_property_values(prop_uri)):
            new_value = transform_func(str(obj))
            self.g.remove( (sbj, prop_uri, obj ) )
            self.g.add( (sbj, prop_uri, Literal(new_value)) )
        
        with open(DATASET_FILEPATH, "wb") as f:
            f.write(graph.serialize(format="json-ld", context=CONTEXT))    


    #
    # i/o functions
    # 
    
    def load(self, filepath):
        with open(filepath, encoding="utf-8") as f:
            graph_data = json.load(f)
        self.g = Graph().parse(data=json.dumps(graph_data["@graph"]), format="json-ld", context=self.ns.context)


    def read_csv(self, filepath, name=None):
        """
        Reads csv file into rdf graph
        """
        print("loading csv into graph <{}> ...".format(name))        
        dataset = read_csv(filepath)

        if not name:
            name = filepath.split("/")[-1].split(".")[0]

        # add dataset
        dataset_uri = self.ns.dataset(name)
        self.g.add( (dataset_uri, RDF.type, self.ns("Dataset")) )

        # add dataset entries
        for row_nr, row in tqdm(enumerate(dataset)):
            row_uri = self.ns.entry(name, row_nr)
            self.g.add( (row_uri, RDF.type, self.ns("DatasetRow")) )
            self.g.add( (dataset_uri, self.ns.prop("contains"), row_uri) )
            self.g.add( (row_uri, self.ns.prop("is_part_of"), dataset_uri) )
            for column, value in row.items():
                if not "Unnamed" in column:
                    property_uri = self.ns.prop(column)
                    if value:
                        self.g.add( (row_uri, property_uri, Literal(value)) )
    

    def read_json(self, filepath, name=None):
        """
        Converts (flat) json file into an rdf graph (using standard koerby namespace)
        """

        if not name:
            name = filepath.split("/")[-1].split(".")[0]
        print("loading json into graph <{}> ...".format(name))

        dataset = json.load(open(filepath))


        # add dataset
        dataset_uri = self.ns.dataset(name)
        self.g.add( (dataset_uri, RDF.type, self.ns("Dataset")) )

        # add dataset entries
        for row_nr, row in tqdm(dataset.items()):
            row_uri = self.ns.entry(name, row_nr)
            self.g.add( (row_uri, RDF.type, self.ns("DatasetRow")) )
            self.g.add( (dataset_uri, self.ns.prop("contains"), row_uri) )
            self.g.add( (row_uri, self.ns.prop("is_part_of"), dataset_uri) )
            for key, value in row.items():
                property_uri = self.ns.prop(key)
                if type(value) == list:
                    for v in value:
                        if v:
                            self.g.add( (row_uri, property_uri, Literal(v)) )
                else:
                    if value:
                        self.g.add( (row_uri, property_uri, Literal(value)) )

    def to_jsonld(self, filepath):
        """
        Serializes graph as jsonld file in :filepath:
        """
        print("serializes graph as jsonld to <{}> ...".format(filepath))
        with open(filepath, "wb") as f:
            f.write(self.g.serialize(format="json-ld", context=self.ns.context))        
