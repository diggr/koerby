from kirby.rdf import generate_rdf_dataset, load_rdf_dataset, match_literal, generate_matches
from std_functions import std_url, std_company


def main():

    match_config = [
        (["wkp"], lambda x: x.split("/")[-1]),
        (["url"], std_url),
        (["name_en", "name_ja"], std_company)
    ]
    generate_matches(match_config)

if __name__ == "__main__":
    main()