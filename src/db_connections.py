import psycopg2.extras
import yaml
import os
import traceback

from sshtunnel import SSHTunnelForwarder

base_dir = os.path.dirname(__file__)
print(base_dir)

try:
    with open(base_dir + "/conf/config.yml", 'r') as yml_file:
        cfg = yaml.load(yml_file)

    # tunnel = SSHTunnelForwarder((cfg['destination']['ssh']['host'], 22), ssh_username=cfg['destination']['ssh']['user'],
    #                             ssh_private_key=cfg['destination']['ssh']['pkey'],
    #                             remote_bind_address=('localhost', 5432),
    #                             local_bind_address=('localhost', 6543))
    # tunnel.start()
    # destination = psycopg2.connect(database=cfg['destination']['db']['database'], user=cfg['destination']['db']['user'],
    #                                password=cfg['destination']['db']['password'], host=tunnel.local_bind_host,
    #                                port=tunnel.local_bind_port)

    destination = psycopg2.connect(database=cfg['destination']['db']['database'], user=cfg['destination']['db']['user'],
                                   password=cfg['destination']['db']['password'], host='localhost',
                                   port=5433)

    source = psycopg2.connect(host=cfg['source']['host'], database=cfg['source']['database'],
                              user=cfg['source']['username'], password=cfg['source']['password'],
                              port=cfg['source']['port'])
except Exception as ex:
    traceback.print_exc(ex)
    exit(1)


# def close_tunnel():
#     if tunnel.is_alive:
#         tunnel.close()
