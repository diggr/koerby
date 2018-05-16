#!/usr/bin/env python3
"""
link module for linking datasets
"""

from itertools import product
from tqdm import tqdm
import random
import Levenshtein as lev
from .rules.general import *
from .filters import *
from .helpers import remove_tm, std, get_platform_groups, load_series
from .config import *

__author__ = "Florian Rämisch and Peter Mühleder"
__copyright = "Copyright 2017, Universitätsbibliothek Leipzig"
__email__ = "team@diggr.link"


#MATCHING RULES & FILTERS
ALL_RULES = [first_letter_rule, numbering_rule]
ALL_FILTERS = [perfect_match_filter, exclude_titles_filter, closest_year_filter]

#PLATFORM GROUPS
PLATFORM_GROUPS = get_platform_groups()

#
REMOVE_SERIES = load_series()

def _pre_processing(a):
    """
    inital steps of preparing string :a: for matching
    """
    if a:
        a = remove_tm(a)
        a = a.replace("Ⅱ", "II")
        a = a.split("(")[0]
        a = a.replace("〔", "").replace("〕","")

        for series in REMOVE_SERIES:
            if series in a:
                a = a.replace(series+" Series","")
                break
    return a.strip()

def _get_platforms(platform_name):
    """check if platform group exists for :platform_name: and returns list of possible platforms"""
    if platform_name in PLATFORM_GROUPS:
        platforms = PLATFORM_GROUPS[platform_name]
    else:
        platforms = [platform_name]
    return platforms

def _filter_platform(ds, platforms):
    """ filters a game dataset according to platform and year (period) """
    
    for game in ds.values():
        for p1, p2 in product(game["platforms"], platforms):

            match = False

            platform_name1 = p1["name"]
            platform_name2 = p2["name"]

            platforms1 = _get_platforms(platform_name1)
            platforms2 = _get_platforms(platform_name2)

            for p1, p2 in product(platforms1, platforms2):
                if p1 == p2:
                    yield game
                    match = True
                    break
            if match:
                break

            # if platform_name1 == platform_name2:
            #     yield game
            #     break


def link(ds1, ds2,threshold=THRESHOLD, rules=ALL_RULES, filters=ALL_FILTERS):       
    """ 
    matches to game datasets 
    deterministic: platform, year (period +- :YEAR_RANGE:)
    probabilistic: titles (every possible combination will be testes, best match ratio will be used)
    """

    excluded_titles = load_excluded_titles()

    matches = []
    """
    data = random.sample(ds1.keys(), 1000)

    for key in tqdm(data):
        g1 = ds1[key]
    """
    for g1 in tqdm(ds1.values()):
        possible = []
   
        for g2 in _filter_platform(ds2, g1["platforms"]):
            best_ratio = 0

            if g2["titles"] == None:
                print(g2)

            for a, b in product(g1["titles"], g2["titles"]):
                
                a = _pre_processing(a)
                b = _pre_processing(b)                

                r = lev.ratio(std(a), std(b))

                if r > threshold and r < 1.0:

                    if a and b:
                        #apply matching rules
                        weight = 0
                        for rule in rules:
                            weight += rule(a,b)

                        r = lev.ratio(std(a),std(b)) - weight

                if r > best_ratio: best_ratio = r


            if best_ratio > threshold:
                possible.append([g2, best_ratio])
        
        if len(possible) > 0:
            possible = sorted(possible, key=lambda x: x[1], reverse=True)
            matches.append([g1, possible])

    #apply post match context filters
    for context_filter in filters:
        matches = context_filter(matches)

    return matches

def link_titles(titles_a,titles_b, rules=ALL_RULES):
    best_ratio = 0
    for a, b in product(titles_a, titles_b):
        a = _pre_processing(a)
        b = _pre_processing(b)         

        #rules = [first_letter_rule, numbering_rule]

        if a and b:
            weights = [ rule(a,b) for rule in rules ]

            r = lev.ratio(std(a),std(b)) - sum(weights)

            if r > best_ratio: best_ratio = r
    
    return best_ratio