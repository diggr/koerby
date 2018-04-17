
from kirby.rdf import transform_source_property, enrich_dataset
from kirby.wiki import identify
from kirby.utils import remove_html
from kirby.config import PROV_AGENT, DATASET_FILEPATH
from std_functions import std_company
from kirby.pit import Provenance


VG_ENTITIES = ["Q210167", "Q1137109"]

def wiki_identify_en(name):
    wiki_link, wkp = identify(std_company(name), "en", entities=VG_ENTITIES)
    rv = {
        "wkp": [ wkp ],
        "wiki_link": [ wiki_link]
    }
    return rv

def wiki_identify_ja(name):
    wiki_link, wkp = identify(std_company(name), "ja", entities=VG_ENTITIES)
    rv = {
        "wkp": [ wkp ],
        "wiki_link": [ wiki_link]
    }
    return rv


def main():
    transform_source_property("name_en", remove_html)
    transform_source_property("name_ja", remove_html)
    transform_source_property("wkp", lambda x: x.split("/")[-1])
    enrich_dataset("name_en", wiki_identify_en, skip="wkp")
    enrich_dataset("name_ja", wiki_identify_ja, skip="wkp")
    prov = Provenance(DATASET_FILEPATH)
    prov.add(
        agent=PROV_AGENT,
        activity="dataset_transformation",
        description="removes html tags from 'name_en' and 'name_ja'; disambiguate 'name_en' and 'name_ja' with  wikidata"
    )
    prov.save()

if __name__ == "__main__":
    main()