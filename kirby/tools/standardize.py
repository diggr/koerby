import string
import re

PUNCT_TRANSTABLE = str.maketrans("","",".,:-〔〕'’*/!&?+")
COMPANY_FORMS= [" sarl", " sa", " bv"," co ltd", " tec", " coltd", " limited", " ltd", " llc", " gmbh", " ltda", " corp", " inc", " srl"]

def std_company(t):
    if t:
        t = t.lower()
        t = t.translate(PUNCT_TRANSTABLE)
        for form in COMPANY_FORMS:
            t = t.replace(form, "")
            
        t = re.sub(r'\([^\()]+?\)', '', t)
        t = re.sub(r'\[[^\[]]+?\]', '', t)

        t = t.replace(" 株式会社", "")
        t = t.strip()
    return t


def std_str(t):
    if t:
        t = t.lower()
        t = t.translate(PUNCT_TRANSTABLE)
        t = t.strip()
    return t

def std_url(t):
    

    return t