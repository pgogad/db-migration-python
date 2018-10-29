import argparse
import datetime
import traceback

import psycopg2
import psycopg2.extras

from common_utils import get_mapping, get_primary_key, get_type, create_value_map, evaluate_val, disable_triggers, \
    enable_triggers, create_insert_part
from db_connections import source, destination, cfg, base_dir


def create_batch_insert(destination_schema, destination_table, schema, table_name, sd, ed):
    file = "%s/mappings/%s.json" % (base_dir, table_name)
    mapping = get_mapping(file)
    col_type_dest = get_type(destination_schema, destination_table)
    pk = get_primary_key(table_name)
    lst = list()
    for key in mapping.keys():
        if mapping[key]['with_comma']:
            lst.append('"%s"' % key)
        else:
            lst.append(key)
    lst.append('ROW_NUMBER() OVER(ORDER BY %s)' % pk)
    select_str = ",".join(lst)
    sql_count = 'select count(*) from %s.%s where create_date >= \'%s\' and create_date <= \'%s\'' \
                % (schema, table_name, sd, ed)

    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_count)
    results = cur.fetchone()
    no_rows = results['count']
    print('Total records found %s' % str(no_rows))
    cur.close()

    batch_sz = int(cfg['source']['batch'])
    itr = 1
    if no_rows > batch_sz:
        itr = int((no_rows / batch_sz) + 1)
    print('No of itrs : %s' % str(itr))

    sql, key_lst = create_insert_part(destination_schema, destination_table, col_type_dest, mapping)
    table = 'select %s from %s.%s where create_date >= \'%s\' and create_date <= \'%s\'' \
            % (select_str, schema, table_name, sd, ed)

    for i in range(itr):
        task(int(i * batch_sz), batch_sz, table, mapping, key_lst, col_type_dest, sql)


def task(offset, batch_sz, table, mapping, key_lst, col_type_dest, sql):
    upper_limit = offset + batch_sz
    sql_select = 'select * from (%s) x where ROW_NUMBER > %s and ROW_NUMBER <= %s' \
                 % (table, str(offset), str(upper_limit))
    print(sql_select)
    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_select)
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

    dest = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    psycopg2.extras.execute_values(dest, sql + ' %s', values_lst, template=None, page_size=1000)
    destination.commit()
    dest.close()


def count_down(destination_schema, destination_table, schema, table_name, sd, ed):
    start_date = datetime.datetime.strptime('%s 00:00:00' % sd, '%Y-%m-%d %H:%M:%S')
    end_date = datetime.datetime.strptime('%s 23:59:59' % ed, '%Y-%m-%d %H:%M:%S')
    current = start_date

    while current <= end_date:
        sd = current.strftime('%Y-%m-%d 00:00:00')
        ed = current.strftime('%Y-%m-%d 23:59:59')
        print('Processing from %s to %s' % (sd, ed))
        create_batch_insert(destination_schema, destination_table, schema, table_name, sd, ed)
        current += datetime.timedelta(days=1)


def main():
    parser = argparse.ArgumentParser(description="Script for migrating data")
    parser.add_argument('-s', help="Source schema name", default='public')
    parser.add_argument('-t', help="Source table name", default='stock_inventory_line')
    parser.add_argument('-ds', help="Destination schema name", default='public')
    parser.add_argument('-dt', help="Destination schema name", default='stock_inventory_line')
    parser.add_argument('-sd', help="Start date in format 'YYYY-MM-dd'", default='2016-06-17')
    parser.add_argument('-ed', help="End date in format 'YYYY-MM-dd'", default='2016-12-31')
    arguments = parser.parse_args()
    disable_triggers(arguments.ds, arguments.dt)
    count_down(arguments.ds, arguments.dt, arguments.s, arguments.t, arguments.sd, arguments.ed)
    enable_triggers(arguments.ds, arguments.dt)


if __name__ == "__main__":
    try:
        start_time = datetime.datetime.utcnow()
        main()
        end_time = datetime.datetime.utcnow()
        print('Start time : %s\nEnd time : %s'
              % (start_time.strftime('%Y-%m-%d 00:00:00'), end_time.strftime('%Y-%m-%d 00:00:00')))
        exit(0)
    except Exception as ex:
        traceback.print_exc(ex)
        exit(1)
    finally:
        destination.close()
        source.close()
        # if tunnel.is_alive:
        #     tunnel.stop()
