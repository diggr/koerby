import click

from koerby.api import start_api

@click.group()
def cli():
    pass

@cli.command()
@click.option("--host", default="127.0.0.1", show_default=True)
@click.option("--port", default=6662, show_default=True)
@click.option("--debug/--no-debug", default=True)
def api(host, port, debug):
    print("starting api ...")
    start_api(host, port, debug)

if __name__ == "__main__":
    cli()
