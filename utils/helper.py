import string
import socket
from geolite2 import geolite2

PUNCT_TRANSTABLE = str.maketrans("","",".,:-〔〕'’*/!&?+")
FORMS= [" sarl", " sa", " bv"," co ltd", " tec", " coltd", " limited", " ltd", " llc", " gmbh", " ltda", " corp", " inc", " srl"]

def std(t):
    if t:
        t = t.lower()
        t = t.translate(PUNCT_TRANSTABLE)
        for form in FORMS:
            t = t.replace(form, "")
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