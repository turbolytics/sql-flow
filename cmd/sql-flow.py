import click
from sqlflow import cli as sqlflow_cli
import sqlflow.cli.run


@click.group()
def cli():
    pass


@click.command()
@click.argument('config')
def run(config):
    sqlflow.cli.run.start(config)


@click.group()
def dev():
    pass


@click.command()
@click.argument('config')
@click.argument('fixture')
def invoke(config, fixture):
    sqlflow_cli.dev.invoke(config, fixture)


dev.add_command(invoke)
cli.add_command(run)
cli.add_command(dev)


if __name__ == '__main__':
    cli()