"""
CLI scripts for SMFR
"""
import sys

import ujson as json
import click
import datetime

from flask_migrate import upgrade

from smfrcore.client.api_client import ApiLocalClient
from start import app


@app.cli.command()
@click.option('--init-config', '-i', required=True)
@click.option('--trigger', '-t', required=True)
@click.option('--collection-type', '-c', required=True)
@click.option('--forecast', '-f', required=False)
@click.option('--keywords', '-k', required=False)
@click.option('--locations', '-l', required=False)
@click.option('--runtime', '-r', required=False)
@click.option('--nuts2', '-n', required=False)
@click.option('--nuts2source', '-s', required=False)
def new_collection(init_config, trigger, collection_type, forecast, keywords, locations, runtime, nuts2, nuts2source):
    """Start a collector process with given arguments"""
    click.echo('===> Creating a new collection and attach a collector process to it:')
    config_msg = 'Config file: {}\nTrigger type: {}\nCollection type: {}\nForecast ID: {}\nKeywords file: {}\n' \
                 'Locations file: {}\nRunning Time: {}\nNuts3: {}\nNuts3 Source: {}'
    config_msg = config_msg.format(init_config, trigger, collection_type, forecast or '-', keywords or '-',
                                   locations or '-', runtime or '-', nuts2 or '-', nuts2source or '-')
    click.echo(config_msg)

    payload = {'config': init_config, 'trigger': trigger, 'ctype': collection_type,
               'forecast_id': forecast, 'kwfile': keywords, 'locfile': locations,
               'runtime': runtime, 'nuts2': nuts2, 'nuts2source': nuts2source}
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
    """List current _running collections/collectors"""
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
    click.echo('Stopping all _running collectors')
    res = client.stop_all()
    click.echo(res)


@app.cli.command()
def start_all_collectors():
    """Stop a given collector by its ID"""
    client = ApiLocalClient()
    click.echo('Starting all _running collectors')
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
    from server.config import RestServerConfiguration
    from smfrcore.models.cassandramodels import Tweet
    from cassandra.cqlengine.connection import get_session

    configuration = RestServerConfiguration()

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
@click.option('--collectionid', '-c', required=True)
@click.option('--ttype', '-t', required=True)
def set_language(collectionid, ttype):
    from daemons.utils import safe_langdetect, tweet_normalization_aggressive
    from smfrcore.models.cassandramodels import Tweet

    tweets = Tweet.get_iterator(int(collectionid), ttype)
    for t in tweets:
        if t.lang is not None:
            continue
        original_json = json.loads(t.tweet)
        text = original_json['text']
        lang = safe_langdetect(tweet_normalization_aggressive(text))
        t.lang = lang
        if not t.created_at:
            t.created_at = datetime.datetime.strptime(original_json['created_at'], '%a %b %d %H:%M:%S +0000 %Y')
        click.echo('Saving lang {} for {}'.format(lang, t.tweetid))
        t.save()


@app.cli.command()
@click.option('--name', '-n', required=True)
@click.option('--email', '-e', required=True)
@click.option('--password', '-p', required=True)
def add_admin(name, email, password):
    from smfrcore.models.sqlmodels import User
    user = User.create(name=name, email=email, password=password, role='admin')
    click.echo('User created {}'.format(user))
