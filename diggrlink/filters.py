#!/usr/bin/env python3
"""
link module for linking datasets
"""

import math
from itertools import product
from .config import *
from .helpers import load_excluded_titles

def exclude_titles_filter(matches):
    """
    Checks if linked game is part of the excluded titles list.
    If so, and the link ratio is under 0.95, the link will be removed
    """
    filtered_matches = []
    excluded_titles = load_excluded_titles()

    for match in matches:
        exclude = False
        for ex_title, game_title in product(excluded_titles, match[0]["titles"]):
            if game_title:
                if ex_title.lower() in game_title.lower() and match[1][0][1] < 0.70:
                    exclude = True
                    print(" exclude ... "+game_title)
        if not exclude:
            filtered_matches.append(match)
                
    return filtered_matches

def perfect_match_filter(matches):
    """
    When links with ratio 1.0 exists in possible matches, remove the rest.
    """

    for match in matches:
        perfect_links = list(filter(lambda x: x[1] == 1, match[1]))
        if len(perfect_links) > 0:
            match[1] = perfect_links

    return matches

def _matches_without_perfect_ratio(matches):
    for match in matches:

        perfect_matches = list(filter(lambda x: x[1] == 1, match[1]))
        if len(perfect_matches) == 0 and len(match[1]) > 0:
            yield match

def _same_platforms(platforms1, platforms2):
    #print(platforms1)
    for p1, p2 in product(platforms1, platforms2):
        if p1["name"] == p2["name"]:
            yield (p1, p2)

def closest_year_filter(matches):
    """
    If there are multiple possible links without 1.0 ratio, weight year difference.
    """

    for match in _matches_without_perfect_ratio(matches):

        game_platforms =  match[0]["platforms"]
        new_possible_matches = []
        for possible_match in match[1]:

            #find the clostest distance between release dates 
            closest_distance = 100

            for p1, p2 in _same_platforms(game_platforms, possible_match[0]["platforms"]):
                try:
                    p1_year = int(p1["date"][:4])
                except:
                    p1_year = None

                try:
                    p2_year = int(p2["date"][:4])
                except:
                    p2_year = None

                if p1_year and p2_year:
                    distance = abs(p1_year-p2_year)
                    if distance < closest_distance: closest_distance = distance
                else:
                    print(p1)
                    print(p2)

            #print("closest distance "+str(closest_distance))
            
            if closest_distance != 100:
                new_ratio = round(possible_match[1] - (math.log(closest_distance+1)/10), 10)
                possible_match[1] = new_ratio

    return matches

def best_match_filter(matches):
    """
    check if two games link to the same game with different ratios. if so, only keep link with best ratio.
    """

    return matches