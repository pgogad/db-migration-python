import argparse
import traceback

import psycopg2
import psycopg2.extras
from common_utils import get_mapping, get_primary_key, get_type, create_value_map, evaluate_val, disable_triggers, \
    enable_triggers, create_insert_part
from db_connections import source, destination, cfg, base_dir


def create_batch_insert(destination_schema, destination_table, schema_name, table_name):
    file = "%s/mappings/%s.json" % (base_dir, table_name)
    mapping = get_mapping(file)
    col_type_dest = get_type(destination_schema, destination_table)
    pk = get_primary_key(table_name)
    lst = list()
    for key in mapping:
        if mapping[key] and mapping[key]['with_comma']:
            lst.append('"%s"' % key)
        else:
            lst.append(key)
    lst.append('ROW_NUMBER() OVER(ORDER BY %s)' % pk)
    select_str = ",".join(lst)
    sql_count = 'select count(*) from %s.%s' % (schema_name, table_name)
    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_count)
    results = cur.fetchone()
    no_rows = results['count']
    print(no_rows)
    cur.close()

    batch_sz = int(cfg['source']['batch'])
    itr = 1
    if no_rows > batch_sz:
        itr = int((no_rows / batch_sz) + 1)
    print('No of itrs : %s' % str(itr))

    sql, key_lst = create_insert_part(destination_schema, destination_table, col_type_dest, mapping)

    for i in range(itr):
        offset = int(i * batch_sz)
        upper_limit = offset + batch_sz
        table = 'select %s from %s.%s' % (select_str, schema_name, table_name)
        sql_select = 'select * from (%s) x where ROW_NUMBER between %s and %s' % (table, str(offset), str(upper_limit))

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


def main():
    parser = argparse.ArgumentParser(description="Script for migrating data")
    parser.add_argument('-s', help="Source schema name", required=True)
    parser.add_argument('-t', help="Source table name", required=True)
    parser.add_argument('-ds', help="Destination schema name", required=True)
    parser.add_argument('-dt', help="Destination schema name", required=True)
    arguments = parser.parse_args()
    disable_triggers(arguments.ds, arguments.dt)
    create_batch_insert(arguments.ds, arguments.dt, arguments.s, arguments.t)
    enable_triggers(arguments.ds, arguments.dt)


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
        # close_tunnel()
