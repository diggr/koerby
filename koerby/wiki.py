"""
Wiki disambiguation

Not needed now
"""

import requests
import json
import string
from SPARQLWrapper import SPARQLWrapper, JSON

WIKI_SEARCH = "https://{lang}.wikipedia.org/w/api.php?action=opensearch&search={q}&format=json&limit=15"
WKP_QUERY = "https://{lang}.wikipedia.org/w/api.php?action=query&prop=pageprops&ppprop=wikibase_item&redirects=1&format=json&titles={entity}"
SPARQL_ENDPOINT = "https://query.wikidata.org/sparql"

SPARQL = SPARQLWrapper(SPARQL_ENDPOINT)

ASK_QUERY = """
ASK
{{
   {triples} .
}}
"""
ASK_TRIPLE = "{{ wd:{wkp} wdt:P31 wd:{entity} }}"

def wiki_search(q, lang):
    """
    Searches for :q: on Wikipedia API for language :lang: .
    Return the wikipedia links of the first 15 results.
    """
    resp = requests.get(WIKI_SEARCH.format(lang=lang, q=q))
    results = json.loads(resp.text)
    full_results = {
        "lang": lang,
        "results": results[3]
    }
    return full_results

def check_entity_type(wkp, entities):
    """
    Checks if entity :wkp: is instance of :entities:
    """
    triples = [ ASK_TRIPLE.format(wkp=wkp, entity=x) for x in entities ]
    triples = " UNION ".join(triples)
    ask_query = ASK_QUERY.format(triples=triples)

    SPARQL.setQuery(ask_query)
    SPARQL.setReturnFormat(JSON)
    results = SPARQL.query().convert()
    return results["boolean"]
    
def get_wkp(entity, lang="en"):
    """
    Returns the Wikidata ID for the Wikipedia entry :entity:
    """
    rsp = requests.get(WKP_QUERY.format(lang=lang, entity=entity))
    results = json.loads(rsp.text)
    for page in results["query"]["pages"].values():
        if "pageprops" in page:
            return page["pageprops"]["wikibase_item"]
        else:
            return None

def identify(name, lang, entities):
    """
    Searches Wikipedia for :name: .
    If the wikidata entry of a search result is an instance of 
    :entities:, the search result will be accepted as correct and 
    returned.
    """
    result = wiki_search(name, lang)
    lang = result["lang"]
    for entry in result["results"]:
        entity = entry.split("/")[-1]
        wkp = get_wkp(entity, lang)

        if check_entity_type(wkp, entities):
            return entry, wkp
    return None, None