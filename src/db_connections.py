import psycopg2.extras
import yaml
import os
import traceback
import logging.handlers

from sshtunnel import SSHTunnelForwarder

LOG_FILENAME = 'migration.log'
logger = logging.getLogger('MyLogger')
logger.setLevel(logging.INFO)
handler = logging.handlers.RotatingFileHandler(LOG_FILENAME, maxBytes=1024 * 1024, backupCount=5)
logger.addHandler(handler)

base_dir = os.path.dirname(__file__)
logger.debug(base_dir)

try:
    with open(base_dir + "/conf/config.yml", 'r') as yml_file:
        cfg = yaml.load(yml_file)

    tunnel = SSHTunnelForwarder((cfg['destination']['ssh']['host'], 22), ssh_username=cfg['destination']['ssh']['user'],
                                ssh_private_key=base_dir + cfg['destination']['ssh']['pkey'],
                                remote_bind_address=('localhost', 5432),
                                local_bind_address=('localhost', 6543))
    if not tunnel.is_alive():
        tunnel.start()

    destination = psycopg2.connect(database=cfg['destination']['db']['database'], user=cfg['destination']['db']['user'],
                                   password=cfg['destination']['db']['password'], host=tunnel.local_bind_host,
                                   port=tunnel.local_bind_port)

    # destination = psycopg2.connect(database=cfg['destination']['db']['database'], user=cfg['destination']['db']['user'],
    #                                password=cfg['destination']['db']['password'], host='localhost',
    #                                port=5433)

    source = psycopg2.connect(host=cfg['source']['host'], database=cfg['source']['database'],
                              user=cfg['source']['username'], password=cfg['source']['password'],
                              port=cfg['source']['port'])
except Exception as ex:
    traceback.print_exc(ex)
    exit(1)


def close_tunnel():
    if tunnel.is_alive:
        tunnel.close()
