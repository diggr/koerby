# kirby

Pipline/Framework for csv dataset integration


## Process:

0_ preprocess.py:
* loads datasets from 'source' folder
* identify corresponding wikipedia and wikidata items if availabe
* writes datasets containing wiki data into 'pre' folder

1_integration.py:
* loads dataset form 'pre' folder
* integrates datasets into a single folder, using (1) IDs, (2) company website url, and (3) company name similarity
* creates a uuid for each company
* identifies location using website url (top level domain and geoip lookup)
* saves combined dataset into 'data' folder

2_export.py:
* creates export csv file from combined dataset in 'data' folder
* export csv ordered by (english) company name 

3_update.py:
* updates combined dataset with information from export csv