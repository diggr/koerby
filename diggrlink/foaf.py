#!/usr/bin/env python3
"""
foaf linking module
"""

import re
from itertools import chain
import couchdb
from tqdm import tqdm
import copy

FOAF_THRESHOLD = 0.6


def get_foaf_datasets(ds_name, db):
    ds1_name, ds2_name = ds_name.split("_")
    
    for tmp_ds in list(db):
        if ds1_name in tmp_ds and ds_name != tmp_ds:
            split = tmp_ds.split("_")
            if ds1_name == split[0]:
                ds3_name = split[1]
            else:
                ds3_name = split[0]
            
            for tmp2_ds in list(db):
                if ds3_name in tmp2_ds and ds1_name not in tmp2_ds and ds2_name in tmp2_ds:
                    yield(ds_name, tmp_ds, tmp2_ds, (ds1_name, ds2_name, ds3_name))


def _get_id(linking_entry, linking_ds_name, source_ds_name):
    split = linking_ds_name.split("_")
    if source_ds_name == split[0]:
        return str(linking_entry[0]["id"])
    else:
        return str(linking_entry[1][0][0]["id"])


def _get_corresponding_entries(ds, _id, target_ds, ratio):
    results = []
    
    dataset1 = ds["dataset1"]
    if target_ds == dataset1:
        for link in ds["data"]:
            for possible_link in link[1]:
                if str(possible_link[0]["id"]) == str(_id):
                    if ratio < possible_link[1]: possible_link[1] = ratio
                    if possible_link[1] > FOAF_THRESHOLD:
                        results.append([link[0], possible_link[1], "foaf"])
                    break
    else:
        for link in ds["data"]:
            if str(link[0]["id"]) == str(_id):
                for possible_link in link[1]:
                    if ratio < possible_link[1]:
                        possible_link[1] = ratio
                        possible_link.append("foaf")
                        if possible_link[1] > FOAF_THRESHOLD:
                            results.append(possible_link)
                #results = link[1]
                break
    
    return results


def foaf_linking(a, link_ds1, link_ds2, link_ds3, names):

    
    foaf_links = []
    done = []

    print("foaf linking ...")
    
    for link in tqdm(link_ds2["data"]):
        source_id = _get_id(link, link_ds2["_id"], names[0])
        result_id = _get_id(link, link_ds2["_id"], names[2])
        ratio = link[1][0][1]
        
        if result_id not in done:
            done.append(result_id)
            
            new_entries = _get_corresponding_entries(link_ds3, result_id, names[1], ratio)

            if new_entries:
                foaf_links.append([source_id, new_entries])

    print("updating linking dataset ...")

    for entry in tqdm(foaf_links):
        _id = entry[0]
        new_links = entry[1]
        new_entry = True
        for link in link_ds1["data"]:
            if str(link[0]["id"]) == str(_id):
                new_entry = False
                for new_link in new_links:
                    found = False
                    for possible_link in link[1]:
                        if str(possible_link[0]["id"]) == str(new_link[0]["id"]):
                            if new_link[1] > possible_link[1]:
                                possible_link[1] = new_link[1]
                                possible_link.append("foaf")
                            found = True
                            break
                    if not found:
                        link[1].append(new_link)
                break
        if new_entry:
            link_ds1["data"].append([a[entry[0]], new_links])

    
    #link_ds1.pop("_rev")
    update = copy.deepcopy(link_ds1)
    
    return update