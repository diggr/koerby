# kørbyi

Pipline/framework for csv dataset integration

## Installation

Intall dependencies
```zsh
$ pip install -r requirements.txt
```

Install kirby 
```zsh
$ pip install -e .
```

## Usage

* Create kirby project folder with config.yml

```yaml
project:
  name: game_metadata
  entity-type: game

directories:
  data: ../../2_linking_data
  source: ../..//1_std_datasets

namespaces:
  core: "http://kirby.diggr.link/"
  entry: "http://kirby.diggr.link/entry/"
  dataset: "http://kirby.diggr.link/dataset/"
  property: "http://kirby.diggr.link/property/"
  match: "http://kirby.diggr.link/match/"

```

* Initialize project and build RDF dataset

Build an RDF graph form the json files located in the 'source' directory.

The RDF graph uses the namespaces defined in the config yaml file.

Call the `generate_rdf_dataset()` function to create the RDF dataset and save it 
to the 'data' directory. 'project.name' from the yaml file will be used as filename.

The filename of the json file will be used as the dataset name.

For every datafield in the json file, a property `p_<field_name>` will be created.


```python
from koerby.rdf import generate_rdf_dataset

if __name__ == "__main__":
    generate_rdf_dataset()
```

* Dataset linking/matching

Create a RDF graph containing probability of two dataset entries matching.

The matching process needs to be configured with a dict containing deterministic and
probabilistic matching rules. 

Deterministic rules check for exact matches between properties (fields). It is possible
to provide a list of values to ignore, and a standardize function (which takes a string 
literal as parameter and returns a string literal).

Probabilistic rule needs a compare function, which takes two lists of string literal as 
parameter and return a matching ratio ratio between 0.0 and 1.0.

```python
from koerby.matching import match_datasets

#some linking algorithm
from diggrlink.link import link_titles

if __name__ == "__main__":

    match_config = [{
        "deterministic": [{
            "fields": ["platforms"],
            "ignore_values": ["PC", "Apple Mac OS", "Linux", "iPhone", "iPad", "Android"],
            "std_func": None
        }],
        "probabilistic": {
            "fields": ["title", "alt_titles"],
            "cmp_func": link_titles
        }
    }]

    match_datasets(match_config)

```

## Authors

* Peter Mühleder <muehleder@saw-leipzig.de>
* Florian Rämisch <raemisch@ub.uni-leipzig.de>

## Copyright and License

GNU Affero General Public License v3

2019,2020 Universitätsbibliothek Leipzig <info@ub.uni-leipzig.de>
