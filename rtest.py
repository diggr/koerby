from kirby.rdf_dataset import RdfDataset

ds = RdfDataset()

ds.read_csv("source/cesa_companies.csv", "cesa")
ds.read_json("/home/pmuehleder/data/game_metadata/daft_export/mediaartdb.json", "mediaart")

ds.to_jsonld("test.jsonld")