import string
import socket
import json
import re
from geolite2 import geolite2
from geopy.geocoders import GoogleV3

PUNCT_TRANSTABLE = str.maketrans("","",".,:-〔〕'’*/!&?+")
FORMS= [" sarl", " sa", " bv"," co ltd", " tec", " coltd", " limited", " ltd", " llc", " gmbh", " ltda", " corp", " inc", " srl"]


def remove_html(t):
    if t:
        t = re.sub(r'<[^<]+?>', '', t)
        t = t.strip()
    return t

def std(t):
    if t:
        t = t.lower()
        t = t.translate(PUNCT_TRANSTABLE)
        for form in FORMS:
            t = t.replace(form, "")
            
        t = re.sub(r'\([^\()]+?\)', '', t)
        t = re.sub(r'\[[^\[]]+?\]', '', t)

        t = t.replace(" 株式会社", "")
        t = t.strip()

    return t

def top_level_domain(url):
    if url:
        tld = url.replace("http://", "").replace("https://", "").split("/")[0].split(".")[-1].split(":")[0].split(" ")[0]
        return tld
    else:
        return None

def get_geoip(url):
    if url:
        try:
            ip = socket.gethostbyname(url.replace("https://","").replace("http://", "").split("/")[0])
            reader = geolite2.reader()      
            output = reader.get(ip)
            result = output['country']
            return result
        except:
            return None
    else:
        return None

def get_domain_dict():
    with open("domain_countries.json") as f:
        domains = json.load(f)

    domain_dict = { x["fields"]["tld"]:x["fields"]["country"] for x in domains }
    return domain_dict
    

def get_geolocation(address, google_api_key):
    google_en = GoogleV3(google_api_key)
    google_jp = GoogleV3(google_api_key, domain="maps.google.co.jp")
    raise NotImplementedError


def is_country(country):
    if country in DOMAIN_DICT.values():
        return True
    else:
        return False

DOMAIN_DICT = get_domain_dict()