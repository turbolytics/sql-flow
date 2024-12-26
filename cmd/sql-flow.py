import click
from sqlflow import logging
from sqlflow.config import new_from_path
from sqlflow.lifecycle import start, invoke


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
    conf = new_from_path(config)

    start(
        conf,
        max_msgs_to_process,
    )


@click.group()
def dev():
    pass


@click.command()
@click.argument('config')
@click.argument('fixture')
def invoke(config, fixture):
    invoke(config, fixture)


dev.add_command(invoke)
cli.add_command(run)
cli.add_command(dev)


if __name__ == '__main__':
    logging.init()
    cli()
