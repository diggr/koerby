import string
import re

PUNCT_TRANSTABLE = str.maketrans("","",".,:-〔〕'’*/!&?+")
FORMS= [" sarl", " sa", " bv"," co ltd", " tec", " coltd", " limited", " ltd", " llc", " gmbh", " ltda", " corp", " inc", " srl"]


def std_company(t):
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


def std_url(url):
    url = url.replace("http://", "")
    url = url.replace("https://", "")
    if url[len(url)-1] == "/":
        url = url[:len(url)-2]
    return url