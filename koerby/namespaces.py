import hashlib
import uuid
from rdflib import Namespace, RDF

class KirbyNamespace(object):
    """
    Wrapper for namespacing and uri generation
    """
    def __init__(self, namespaces):
        self._core = Namespace(namespaces["core"])
        #self._entry = Namespace(CONFIG["namespaces"]["entry"])
        self._dataset = Namespace(namespaces["dataset"])
        self._property = Namespace(namespaces["property"])
        self._match = Namespace(namespaces["match"])
        self._cluster = Namespace(namespaces["cluster"])
    
        self.context = {
            "rdf": str(RDF),
            "core": str(self._core),
            "dataset": str(self._dataset),
            "properties": str(self._property)
        }
        
    # def kirbyEntry(self):
    #     id_ = "{}_{}".format(PROJECT_NAME, uuid.uuid4().hex)
    #     return self._entry[id_]
    
    def cluster(self):
        return self._cluster["c_{}".format(uuid.uuid4().hex)]

    def dataset(self, name):
        return self._dataset[name]

    def entry(self, dataset_name, source_id):
        return self._dataset["{}/{}".format(dataset_name, source_id)]
    
    def prop(self, name):
        return self._property["p_{}".format(name)]

    def match(self, row, match):
        comb = sorted([str(row), str(match)])
        uid = hashlib.md5("".join(comb).replace(self._core["Dataset"], "").encode("utf-8")).hexdigest()
        return self._match["m_{}".format(uid)]        

    def __call__(self, x):
        return self._core[x]
