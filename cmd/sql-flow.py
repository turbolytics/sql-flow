import click
from sqlflow import cli as sqlflow_cli
import sqlflow.cli.run


@click.group()
def cli():
    pass


@click.command()
@click.argument('config')
@click.option(
    '--max-msgs-to-process',
    type=int,
    default=None,
    help='Terminate execution after successfully processing this number.',
)
def run(config, max_msgs_to_process):
    sqlflow.cli.run.start(config, max_msgs_to_process)


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