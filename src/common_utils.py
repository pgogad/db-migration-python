import json

import psycopg2
import psycopg2.extras

from db_connections import source, destination, logger


def disable_triggers(schema_name, table_name):
    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = "ALTER TABLE %s.%s DISABLE TRIGGER ALL;" % (schema_name, table_name)
    cur.execute(sql)
    cur.close()


def enable_triggers(schema_name, table_name):
    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    sql = "ALTER TABLE %s.%s ENABLE TRIGGER ALL;" % (schema_name, table_name)
    cur.execute(sql)
    cur.close()


def get_primary_key(table_name):
    sql = 'SELECT a.attname AS col_name, format_type(a.atttypid, a.atttypmod) ' \
          'AS data_type FROM pg_index i JOIN pg_attribute a ' \
          'ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) ' \
          'WHERE i.indrelid = \'%s\'::regclass AND i.indisprimary;' % table_name

    cur = source.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    lst = list()
    for row in rows:
        lst.append(row['col_name'])
    logger.debug('%s' % ",".join(lst))
    return ",".join(lst)


def get_mapping(filename):
    with open(filename, 'r') as mapping:
        data = json.load(mapping)
        mapping.close()
    logger.debug('%s' % json.dumps(data))
    return data


def check_case(incoming):
    key_words = ['date', 'type', 'name']
    if incoming in key_words:
        return True

    return any(letter.isupper() for letter in incoming)


def create_insert_part(destination_schema, destination_table, col_type, mapping):
    sql = 'INSERT INTO %s.%s(%s) VALUES'
    key_lst = list()
    for key in col_type:  # col_type is destination table column type
        if key in mapping.keys():  # if key exists in mapping i.e. we need to map old data to new schema
            if check_case(mapping[key]):
                key_lst.append('"%s"' % mapping[key])
            else:
                key_lst.append(mapping[key])
        else:  # If mapping does not exist in mapping it implies that this is a new column
            if check_case(key):
                key_lst.append('"%s"' % key)
            else:
                key_lst.append(key)

    sql = sql % (destination_schema, destination_table, ','.join(key_lst))
    logger.debug('%s' % sql)
    return sql, key_lst


def evaluate_val(col_type, key, value):
    key = key.replace('"', '')
    if col_type[key]['data_type'] == 'integer':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return 0
            else:
                return None
    elif col_type[key]['data_type'] == 'character varying':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    elif col_type[key]['data_type'] == 'boolean':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return False
            else:
                return None
    elif col_type[key]['data_type'] == 'text':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    elif col_type[key]['data_type'] == 'double precision':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return 0.0
            else:
                return None
    elif col_type[key]['data_type'] == 'date':
        if value is not None:
            return str(value)
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return "1971-01-01 00:00:00"
            else:
                return None
    elif col_type[key]['data_type'] == 'numeric':
        if value is not None:
            return value
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return 0
            else:
                return None
    elif col_type[key]['data_type'] == 'timestamp without time zone':
        if value is not None:
            return str(value)
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return "1971-01-01 00:00:00"
            else:
                return None
    elif col_type[key]['data_type'] == 'bytea':
        if value is not None:
            return str(value)
        else:
            if col_type[key]['is_nullable'] == 'NO':
                return ""
            else:
                return None
    else:
        print("Cannot find mapping for type : " + col_type[key]['data_type'])
        return None


def get_type(schema_name, table_name):
    sql = "SELECT column_name, data_type, is_nullable FROM INFORMATION_SCHEMA.COLUMNS WHERE " \
          "table_schema = '%s' AND table_name = '%s'" \
          % (schema_name, table_name)
    cur = destination.cursor(cursor_factory=psycopg2.extras.DictCursor)
    cur.execute(sql)
    rows = cur.fetchall()
    cur.close()
    type_map = dict()
    for row in rows:
        obj = dict()
        obj['data_type'] = row['data_type']
        obj['is_nullable'] = row['is_nullable']
        type_map[row['column_name']] = obj
    return type_map


def create_value_map(mapping, row):
    value_map = dict()
    for key in row.keys():
        if key in mapping.keys():
            if check_case(mapping[key]):
                value_map['"%s"' % mapping[key]] = row[key]
            else:
                value_map[mapping[key]] = row[key]
    return value_map
