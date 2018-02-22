"""
CLI scripts for SMFR
"""
import os
import sys
import shutil

import click
from flask_migrate import upgrade


current_dir = os.path.dirname(__file__)
client_src = os.path.join(current_dir, '../../client')
dst = os.path.join(current_dir, 'client/')
if os.path.exists(dst):
    shutil.rmtree(dst)
shutil.copytree(client_src, dst)

from client.api_client import ApiLocalClient
from start import app


@app.cli.command()
@click.option('--init-config', '-i', required=True)
@click.option('--trigger', '-t', required=True)
@click.option('--collection-type', '-c', required=True)
@click.option('--forecast', '-f', required=False)
@click.option('--keywords', '-k', required=False)
@click.option('--locations', '-l', required=False)
@click.option('--runtime', '-r', required=False)
@click.option('--nuts3', '-n', required=False)
@click.option('--nuts3source', '-s', required=False)
def new_collection(init_config, trigger, collection_type, forecast, keywords, locations, runtime, nuts3, nuts3source):
    """Start a collector process with given arguments"""
    click.echo('===> Creating a new collection and attach a collector process to it:')
    config_msg = 'Config file: {}\nTrigger type: {}\nCollection type: {}\nForecast ID: {}\nKeywords file: {}\n' \
                 'Locations file: {}\nRunning Time: {}\nNuts3: {}\nNuts3 Source: {}'
    config_msg = config_msg.format(init_config, trigger, collection_type, forecast or '-', keywords or '-',
                                   locations or '-', runtime or '-', nuts3 or '-', nuts3source or '-')
    click.echo(config_msg)

    payload = {'config': init_config, 'trigger': trigger, 'ctype': collection_type,
               'forecast_id': forecast, 'kwfile': keywords, 'locfile': locations,
               'runtime': runtime, 'nuts3': nuts3, 'nuts3source': nuts3source}
    client = ApiLocalClient()
    res = client.new_collection(payload)
    click.echo(res)


@app.cli.command()
def list_collections():
    """List known collections in SMFR"""
    client = ApiLocalClient()
    res = client.list_collections()
    click.echo(res)


@app.cli.command()
def list_running_collectors():
    """List current running collections/collectors"""
    client = ApiLocalClient()
    res = client.list_running_collectors()
    click.echo(res)


@app.cli.command()
@click.option('--collector', '-c', required=True)
def stop_collector(collector):
    """Stop a given collector by its ID"""
    client = ApiLocalClient()
    click.echo('Stopping collector %s' % collector)
    res = client.stop_collector(collector)
    click.echo(res)


@app.cli.command()
def stop_all_collectors():
    """Stop a given collector by its ID"""
    client = ApiLocalClient()
    click.echo('Stopping all running collectors')
    res = client.stop_all()
    click.echo(res)


@app.cli.command()
def start_all_collectors():
    """Stop a given collector by its ID"""
    client = ApiLocalClient()
    click.echo('Starting all running collectors')
    res = client.start_all()
    click.echo(res)


@app.cli.command()
@click.option('--collector', '-c', required=True)
def start_collector(collector):
    """Start a given collector by its ID"""
    client = ApiLocalClient()
    res = client.start_collector(collector)
    click.echo(res)


@app.cli.command()
def empty_dbs():
    """Reset databases! Warning! Issue the command only in DEV environments"""
    from server.config import server_configuration
    from server.models import Tweet
    from cassandra.cqlengine.connection import get_session

    configuration = server_configuration()

    if not configuration.debug:
        click.echo('Cannot empty dbs: not a development instance!')
        click.echo('Set production to False in config.yaml')
        sys.exit(1)

    # removing all MySQL tables
    click.echo('Removing MySQL objects!')
    configuration.db_mysql.drop_all()
    configuration.db_mysql.engine.execute('TRUNCATE TABLE smfr.alembic_version')

    click.echo('Removing Cassandra objects!')
    session = get_session()
    tweet_table_name = Tweet.__table_name__ or Tweet.__name__.lower()
    stmt = 'TRUNCATE TABLE {}.{}'.format(Tweet.__keyspace__, tweet_table_name)
    session.execute(stmt)
    click.echo('Init DBs...performing migrations')
    upgrade()
    click.echo('DB at initial state!')


@app.cli.command()
@click.option('--file', '-f', required=True)
def test_upload(file):
    client = ApiLocalClient()
    formdata = {'kwfile_file': file}
    res = client.test_upload(formdata)
    click.echo(res)
