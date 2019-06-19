import argparse
import json
import traceback

import psycopg2
import psycopg2.extras

from common_utils import disable_triggers, enable_triggers
from db_connections import source, destination, cfg, base_dir, logger, close_tunnel


def migrate_process(table_name):
    file = "%s/mappings/updates/%s.json" % (base_dir, table_name)
    with open(file, 'r') as mapping:
        updates = json.load(mapping)
        mapping.close()

    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(updates['count'])
    results = cur.fetchone()
    no_rows = results['count']
    logger.info('No of Rows : %s' % str(no_rows))
    cur.close()

    batch_sz = int(cfg['source']['batch'])
    itr = 1
    if no_rows > batch_sz:
        itr = int((no_rows / batch_sz) + 1)
    logger.info('No of iterations : %s' % str(itr))

    for i in range(itr):
        logger.info('Iteration number %d' % (i + 1))
        offset = int(i * batch_sz)
        upper_limit = offset + batch_sz
        select_sql = 'select * from (%s) X where ROW_NUMBER > %s and ROW_NUMBER <= %s' \
                     % (updates['sql'], str(offset), str(upper_limit))
        logger.info(select_sql)
        cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(select_sql)
        rows = cur.fetchall()
        cur.close()

        q_params = list()
        q_str = ''
        for row in rows:
            r = dict(row)
            del r['row_number']
            q_str += updates['placeholder']
            for key in updates['keys']:
                q_params.extend([r[key]])
        q_str = q_str[:-1]

        try:
            sql = updates['update'] % q_str
            dest = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
            dest.execute(sql, q_params)
            destination.commit()
            dest.close()
        except Exception as ex:
            logger.error(ex)


def main():
    parser = argparse.ArgumentParser(description="Script for migrating data")
    parser.add_argument('-t', help="Source table name", default='stock_inventory_line')
    arguments = parser.parse_args()
    disable_triggers('public', arguments.t)
    migrate_process(arguments.t)
    enable_triggers('public', arguments.t)


if __name__ == "__main__":
    try:
        main()
        exit(0)
    except Exception as ex:
        traceback.print_exc(ex)
        exit(1)
    finally:
        destination.close()
        source.close()
        close_tunnel()
    exit(0)
