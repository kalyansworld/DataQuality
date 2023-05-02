import pandas as pd
from sqlalchemy import create_engine

import Audit_table_insertion as dq

postgres_host = "pljbd06.prodapt.com"
postgres_user = "postgres"
postgres_password = "newPassword"
postgres_database = "mydatabase"


engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))


df = pd.read_sql_query(f"select * from control_table", engine)
table_names = list(df['table_names'])

null_validation = list(df['null_validation'])
duplicate_validation = list(df['duplicate_validation'])
historcial_records_validation = list(df['historcial_records_validation'])
historcial_records_validation_column_names = list(df['historcial_records_validation_column_names'])
missing_records_validation = list(df['missing_records_validation'])
missing_records_validation_column_names = list(df['missing_records_validation_column_names'])
sudden_spikes_validation = list(df['sudden_spikes_validation'])
sudden_spikes_validation_column_names = list(df['sudden_spikes_validation_column_names'])
min_max_avg_validation = list(df['min_max_avg_validation'])
empty_strings_validation = list(df['empty_strings_validation'])
schema_records_validation = list(df['stg_schema_records_status'])
schema_records_validation_columns = list(df['stg_schema_records_column_names'])
check_default_columns_validation = list(df['check_default_columns'])
check_data_latency_hour = list(df['check_data_latency_hour'])
check_data_latency_hour_column_names = list(df['check_data_latency_hour_column_names'])
distinct_count_of_each_col = list(df['distinct_count_of_each_col'])
count_of_each_value = list(df['count_of_each_value'])
dtypes = list(df['get_dtypes'])

dq.check_schema_historical_records()
dq.records_count_of_all_tables()
dq.execute_null_validation_for_entire_table()
dq.get_duplicates_of_a_table()

for ind in range (len(table_names)):

    df1=pd.read_sql_query(f"select * from {table_names[ind]}", engine)
    column_in_table=list(df1)

    if null_validation[ind]=='Y':
        dq.execute_null_validation(table_names[ind],column_in_table)
    if duplicate_validation[ind]=="Y":
        dq.execute_unique_validation(table_names[ind],column_in_table)
    if min_max_avg_validation[ind]=="Y":
        dq.find_max_min_values(table_names[ind],column_in_table)
    if empty_strings_validation[ind]=="Y":
        dq.find_empty_strings_count(table_names[ind],column_in_table)
    if sudden_spikes_validation[ind]=="Y":
        dq.find_sudden_spikes_date(table_names[ind],sudden_spikes_validation_column_names[ind])
    if missing_records_validation[ind]=="Y":
        dq.find_missing_records_by_dates(table_names[ind],missing_records_validation_column_names[ind])
    if historcial_records_validation[ind]=="Y":
        dq.find_historical_records_validation(table_names[ind],historcial_records_validation_column_names[ind])
    if check_default_columns_validation[ind]=="Y":
        dq.check_default_columns(table_names[ind])
    if check_data_latency_hour[ind]=="Y":
        dq.check_data_latency_hour(table_names[ind],check_data_latency_hour_column_names[ind])
    if distinct_count_of_each_col[ind]=="Y":
        dq.get_distinct_counts_each_column(table_names[ind],column_in_table)
    if count_of_each_value[ind]=="Y":
       dq.get_counts_of_each_value_column(table_names[ind],column_in_table)
    if dtypes[ind] == "Y":
        dq.get_d_types(table_names[ind])

