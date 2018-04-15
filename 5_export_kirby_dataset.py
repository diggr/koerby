from kirby.rdf import export_kirby_dataset

def main():
    export_config = {
        "name": "test.csv",
        "properties": [
            "wkp",
            "name_en",
            "name_ja",
            "url",
            "address"
        ],
        "order_by": "name_en"
    }

    export_kirby_dataset(export_config)

if __name__ == "__main__":
    main()