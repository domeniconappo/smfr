import sys

import connexion

from server.config import RestServerConfiguration


def create_app():
    cli_args = sys.argv
    is_webapp_bootstrapping = 'gunicorn' in cli_args[0]
    connexion_app = connexion.App('SMFR Rest Server', specification_dir='swagger/')
    config = RestServerConfiguration(connexion_app, bootstrap_webapp=is_webapp_bootstrapping)
    if is_webapp_bootstrapping:
        from daemons import Consumer, Collector
        config.init_mysql()
        config.init_cassandra()
        Consumer.build_and_start()
        Collector.resume_active()

    connexion_app.add_api('smfr.yaml', base_path=config.base_path)
    return config.flask_app


app = create_app()
