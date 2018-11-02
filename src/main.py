import traceback
from datetime import datetime

import psycopg2
import psycopg2.extras

from argument_list import src_dest_table_names, src_schema_name, dest_schema_name
from common_utils import get_mapping, get_primary_key, get_type, create_value_map, evaluate_val, disable_triggers, \
    enable_triggers, create_insert_part
from db_connections import source, destination, cfg, base_dir, logger


def create_batch_insert(destination_schema, destination_table, schema_name, table_name):
    file = "%s/mappings/%s.json" % (base_dir, table_name)
    mapping = get_mapping(file)
    col_type_dest = get_type(destination_schema, destination_table)
    pk = get_primary_key(table_name)
    lst = list()
    for key in mapping:
        new_key = key.join('""')
        new_key = new_key.replace('\'', '')
        lst.append(new_key)
    if pk:
        lst.append('ROW_NUMBER() OVER(ORDER BY %s)' % pk)
    select_str = ",".join(lst)
    sql_count = 'select count(*) from %s.%s' % (schema_name, table_name)
    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_count)
    results = cur.fetchone()
    no_rows = results['count']
    logger.info('No of Rows : ' + '%s' % no_rows)
    cur.close()

    batch_sz = int(cfg['source']['batch'])
    itr = 1
    if no_rows > batch_sz:
        itr = int((no_rows / batch_sz) + 1)
    logger.info('No of iterations : ' + '%s' % str(itr))

    sql, key_lst = create_insert_part(destination_schema, destination_table, col_type_dest, mapping)
    new_key_lst = []
    for data in key_lst:
        a = data.strip('"')
        new_key_lst.append(a)

    for i in range(itr):
        offset = int(i * batch_sz)
        if pk:
            sql_select = 'select %s from %s.%s order by ROW_NUMBER LIMIT %s OFFSET %s' \
                         % (select_str, schema_name, table_name, cfg['source']['batch'], str(offset))
        else:
            sql_select = 'select %s from %s.%s LIMIT %s OFFSET %s' \
                         % (select_str, schema_name, table_name, cfg['source']['batch'], str(offset))
        cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
        cur.execute(sql_select)
        rows = cur.fetchall()
        cur.close()
        values_lst = list()
        for row in rows:
            values = ()
            value_map = create_value_map(mapping, row)
            for key in new_key_lst:
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


def main():
    script_start_time = datetime.now()
    for table_name in src_dest_table_names:
        logger.info("##########################################################################################")
        logger.info('Migration Started For : %s' % table_name)
        table_start_time = datetime.now()
        disable_triggers(dest_schema_name, table_name)
        create_batch_insert(dest_schema_name, table_name, src_schema_name, table_name)
        enable_triggers(dest_schema_name, table_name)
        logger.info('Migration Completed For : %s and Duration of Execution was %s'
                    % (str(table_name), str(datetime.now() - table_start_time)))

    logger.info("##########################################################################################")
    logger.info('*************************************')
    logger.info('Total Execution Time : %s' % (datetime.now() - script_start_time))
    logger.info('*************************************')


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
