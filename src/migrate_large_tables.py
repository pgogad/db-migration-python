import traceback
from datetime import datetime
from datetime import timedelta
import psycopg2
import psycopg2.extras
from argument_list import bulk_src_schema_name, bulk_dest_schema_name, bulk_src_dest_table_name, start_date, end_date
from common_utils import get_mapping, get_primary_key, get_type, create_value_map, evaluate_val, disable_triggers, \
    enable_triggers, create_insert_part, logger
from db_connections import source, destination, cfg, base_dir, close_tunnel


def create_batch_insert(dschema, dtable, schema, table_name, col_type_dest, select_str, mapping, insert_sql, key_lst,
                        sd, ed):
    end_time = datetime.strptime(ed, '%Y-%m-%d %H:%M:%S.%f') + timedelta(days=1)
    ed = datetime.strftime(end_time, '%Y-%m-%d 00:00:00.000000')

    sql_count = 'select count(*) from %s.%s where create_date >= \'%s\' and create_date < \'%s\'' \
                % (schema, table_name, sd, ed)

    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_count)
    results = cur.fetchone()
    no_rows = results['count']
    logger.info('Total records found %d' % no_rows)
    cur.close()

    batch_sz = int(cfg['source']['batch'])
    itr = 1
    if no_rows > batch_sz:
        itr = int((no_rows / batch_sz) + 1)
    logger.info('Total number of iterations : %d' % itr)

    select_sql = 'select %s from %s.%s where create_date >= \'%s\' and create_date < \'%s\'' \
                 % (select_str, schema, table_name, sd, ed)

    for i in range(itr):
        task(i, select_sql, mapping, key_lst, col_type_dest, insert_sql)

    sql_count = 'select count(*) from %s.%s where create_date >= \'%s\' and create_date < \'%s\'' \
                % (dschema, dtable, sd, ed)

    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql_count)
    results = cur.fetchone()
    dest_no_rows = results['count']
    cur.close()
    logger.info('Total records inserted %d' % dest_no_rows)

    if dest_no_rows != no_rows:
        logger.error('Missing %d records for %s' % (no_rows - dest_no_rows, sd))


def task(i, select_sql, mapping, key_lst, col_type_dest, insert_sql):
    logger.info('Iteration number %d' % (i + 1))
    batch_sz = int(cfg['source']['batch'])
    offset = int(i * batch_sz)
    upper_limit = offset + batch_sz
    sql_select = 'select * from (%s) x where ROW_NUMBER > %s and ROW_NUMBER <= %s' \
                 % (select_sql, str(offset), str(upper_limit))
    logger.info(sql_select)
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

    try:
        dest = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extras.execute_values(dest, insert_sql + ' %s', values_lst, template=None, page_size=1000)
        destination.commit()
        dest.close()
    except Exception as ex:
        logger.error(ex)


def count_down(dschema, dtable, schema, table_name, sd, ed):
    end_time = datetime.strptime('%s 23:59:59.999999' % ed, '%Y-%m-%d %H:%M:%S.%f')
    current = datetime.strptime('%s 00:00:00.000000' % sd, '%Y-%m-%d %H:%M:%S.%f')

    file = "%s/mappings/%s.json" % (base_dir, table_name)
    mapping = get_mapping(file)
    col_type_dest = get_type(dschema, dtable)
    pk = get_primary_key(table_name)
    lst = list()
    for key in mapping.keys():
        new_key = key.join('""')
        new_key = new_key.replace('\'', '')
        lst.append(new_key)
    if pk:
        lst.append('ROW_NUMBER() OVER(ORDER BY %s)' % pk)
    select_str = ",".join(lst)

    insert_sql, key_lst = create_insert_part(dschema, dtable, col_type_dest, mapping)

    while current <= end_time:
        sd = current.strftime('%Y-%m-%d 00:00:00.000000')
        ed = current.strftime('%Y-%m-%d 23:59:59.999999')
        logger.info('Processing from %s to %s' % (sd, ed))
        create_batch_insert(dschema, dtable, schema, table_name, col_type_dest, select_str, mapping, insert_sql,
                            key_lst, sd, ed)
        current += timedelta(days=1)


def main():
    script_start_time = datetime.now()
    logger.info("########################################################################################")
    logger.info('Migration Started For : %s' % bulk_src_dest_table_name)
    disable_triggers(bulk_dest_schema_name, bulk_src_dest_table_name)
    count_down(bulk_dest_schema_name, bulk_src_dest_table_name, bulk_src_schema_name,
               bulk_src_dest_table_name, start_date, end_date)
    enable_triggers(bulk_dest_schema_name, bulk_src_dest_table_name)
    logger.info('Migration Completed For : %s And Duration of Execution was $s'
                % bulk_src_dest_table_name, (datetime.now() - script_start_time))


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
