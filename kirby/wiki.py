import requests
import json
import string
#from .helper import std
from SPARQLWrapper import SPARQLWrapper, JSON

WIKI_SEARCH = "https://{lang}.wikipedia.org/w/api.php?action=opensearch&search={q}&format=json"
WKP_QUERY = "https://{lang}.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles={entity}"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

#set up wikidata sparql
SPARQL = SPARQLWrapper(SPARQL_ENDPOINT)
QUERY = """
ASK
{{
  {{ wd:{wkp} wdt:P31 wd:Q210167 }} UNION {{ wd:{wkp} wdt:P31 wd:Q1137109 }}.
}}
"""

def wiki_search(q, lang):
    resp = requests.get(WIKI_SEARCH.format(lang=lang, q=q))
    results = json.loads(resp.text)
    full_results = {
        "lang": lang,
        "results": results[3]
    }
    return full_results


def is_video_game_company(wkp):
    SPARQL.setQuery(QUERY.format(wkp=wkp))
    SPARQL.setReturnFormat(JSON)
    results = SPARQL.query().convert()
    return results["boolean"]
    
def get_wkp(entity, lang="en"):
    rsp = requests.get(WKP_QUERY.format(lang=lang, entity=entity))
    results = json.loads(rsp.text)
    for page in results["query"]["pages"].values():
        if "pageprops" in page:
            return page["pageprops"]["wikibase_item"]
        else:
            return None


def identify(name, lang):
    result = wiki_search(name, lang)
    lang = result["lang"]
    for entry in result["results"]:
        entity = entry.split("/")[-1]
        #print(entity)
        wkp = get_wkp(entity, lang)

        if is_video_game_company(wkp):
            return entry, wkp
    return None, None