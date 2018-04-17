from kirby.rdf import generate_rdf_dataset, load_rdf_dataset, match_literal, generate_matches
from kirby.utils import std, std_url

COMPANY_FORMS= [" sarl", " sa", " bv"," co ltd", " tec", " coltd", " limited", " ltd", " llc", " gmbh", " ltda", " corp", " inc", " srl", " 株式会社"]

def costum_std(*args, **kwargs):
    s = std(*args, **kwargs, rm_spaces=True, rm_strings=COMPANY_FORMS)
    return s

def main():

    match_config = [
        (["wkp"], lambda x: x.split("/")[-1]),
        (["url"], std_url),
        (["name_en", "name_ja"], costum_std)
    ]
    generate_matches(match_config)

if __name__ == "__main__":
    main()