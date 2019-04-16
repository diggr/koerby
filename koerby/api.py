from flask import Flask
from flask_restful import Resource, Api

from .reader import KirbyReader

PORT = 6662

app = Flask(__name__)
koerby = Api(app)

links = KirbyReader(".")

class Links(Resource):
    def get(self, dataset_name, id_):
        l = links.all_matches(dataset_name, id_)
        return {
            "dataset": dataset_name,
            "entryId": id_,
            "links": l  
        }

class Cluster(Resource):
    def get(self, dataset_name, id_):
        c = links.get_cluster(dataset_name, id_)
        return c
        
koerby.add_resource(Links, '/<string:dataset_name>/<string:id_>/links')
koerby.add_resource(Cluster, '/<string:dataset_name>/<string:id_>/cluster')

def start_api():
    app.run(debug=True, port=PORT)