from kirby.matching import match_datasets
from diggrlink.link import link_titles

def main():

    match_config = [{
        "deterministic": [{
            "fields": ["platforms"],
            "std_func": None
        }],
        "probabilistic": {
            "fields": ["name", "label_ja", "label_de", "label_en"],
            "cmp_func": link_titles
        }
    }]

    match_datasets(match_config)

if __name__ == "__main__":
    main()