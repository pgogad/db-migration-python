from migrate_large_tables import migrate_process

# Argument list for bulk data
bulk_src_schema_name = 'public'
bulk_dest_schema_name = 'public'
bulk_src_dest_table_name = 'res_shipping_code'
start_date = '2012-12-05'
end_date = '2013-10-31'

# Argument list for multiple tables
src_schema_name = 'public'
dest_schema_name = 'public'
src_dest_table_names = ['shipping_canadapost', 'res_shipping_code']

if __name__ == "__main__":
    for table in src_dest_table_names:
        migrate_process(bulk_dest_schema_name, table, bulk_src_schema_name, table, start_date, end_date)
