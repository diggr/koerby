import click

from koerby.api import start_api

@click.group()
def cli():
    pass

@cli.command()
def api():
    print("starting api ...")
    start_api()

if __name__ == "__main__":
    cli()