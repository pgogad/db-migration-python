import traceback
import argparse
from datetime import datetime

import psycopg2
import psycopg2.extras

from common_utils import get_mapping, get_primary_key, get_type, create_value_map, evaluate_val, disable_triggers, \
    enable_triggers, create_insert_part, check_case
from db_connections import source, destination, cfg, base_dir, logger, close_tunnel


def create_batch_insert(destination_schema, destination_table, schema_name, table_name):
    file = "%s/mappings/%s.json" % (base_dir, table_name)
    mapping = get_mapping(file)
    col_type_dest = get_type(destination_schema, destination_table)
    pk = get_primary_key(table_name)
    lst = list()
    for key in mapping:
        new_key = key
        if check_case(key):
            new_key = '"%s"' % key
        lst.append(new_key)
    if pk:
        lst.append('ROW_NUMBER() OVER(ORDER BY %s)' % pk)
    else:
        lst.append('ROW_NUMBER()')

    select_str = ",".join(lst)
    sql_count = 'select count(*) from %s.%s' % (schema_name, table_name)
    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_count)
    results = cur.fetchone()
    no_rows = results['count']
    logger.info('No of Rows : %s' % str(no_rows))
    cur.close()

    batch_sz = int(cfg['source']['batch'])
    itr = 1
    if no_rows > batch_sz:
        itr = int((no_rows / batch_sz) + 1)
    logger.info('No of iterations : %s' % str(itr))

    sql, key_lst = create_insert_part(destination_schema, destination_table, col_type_dest, mapping)
    for i in range(itr):
        logger.info('Iteration number %d' % (i + 1))
        offset = int(i * batch_sz)
        upper_limit = offset + batch_sz
        inner = 'select %s from %s.%s where trans_id != 63' % (select_str, schema_name, table_name)

        select_sql = 'select * from (%s) X where ROW_NUMBER > %s and ROW_NUMBER <= %s' \
                     % (inner, str(offset), str(upper_limit))

        logger.info(select_sql)
        cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(select_sql)
        rows = cur.fetchall()
        cur.close()
        values_lst = list()
        for row in rows:
            values = ()
            value_map = create_value_map(mapping, row)
            for key in key_lst:
                if key in value_map.keys():
                    values = values + (evaluate_val(col_type_dest, key, value_map[key]),)
                else:
                    values = values + (evaluate_val(col_type_dest, key, None),)
            values_lst.append(values)

        try:
            dest = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
            psycopg2.extras.execute_values(dest, sql + ' %s', values_lst, template=None, page_size=1000)
            destination.commit()
            dest.close()
        except Exception as ex:
            logger.error(ex)


def migrate_process(ds, dt, s, t):
    script_start_time = datetime.now()
    logger.info("########################################################################################")
    logger.info('Migration Started For : %s' % dt)
    disable_triggers(ds, dt)
    create_batch_insert(ds, dt, s, t)
    enable_triggers(ds, dt)
    logger.info('Migration Completed For : %s And Duration of Execution was %s'
                % (dt, (datetime.now() - script_start_time)))


def main():
    parser = argparse.ArgumentParser(description="Script for migrating data")
    parser.add_argument('-s', help="Source schema name", default='public')
    parser.add_argument('-t', help="Source table name", default='stock_inventory_line')
    parser.add_argument('-ds', help="Destination schema name", default='public')
    parser.add_argument('-dt', help="Destination schema name", default='stock_inventory_line')
    arguments = parser.parse_args()

    migrate_process(arguments.ds, arguments.dt, arguments.s, arguments.t)


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
