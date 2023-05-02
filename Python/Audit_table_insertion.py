import logging
from datetime import date, datetime

import numpy as np
import pandas as pd
import psycopg2
from sqlalchemy import create_engine

logging.basicConfig(filename='D:\PYHTON\practice.log', level=logging.INFO,
                    format='%(asctime)s:%(levelname)s:%(message)s')

postgres_host = "pljbd06.prodapt.com"
postgres_user = "postgres"
postgres_password = "newPassword"
postgres_database = "mydatabase"
engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))


def persist_audit(audit_table,audit_subject,audit_inputs,audit_result,audit_comments,audit_threshold):
    try:
        conn = engine.connect(close_with_result=True)
        audit_time = datetime.now()
        query = f"INSERT INTO audittable (audit_time,audit_table,audit_subject,audit_inputs,audit_result,audit_comments,audit_threshold) VALUES ('{audit_time}','{audit_table}','{audit_subject}','{audit_inputs}','{audit_result}','{audit_comments}','{audit_threshold}');"
        conn.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def execute_null_validation(table_name,column_name_list):
    try:
        for column_name in column_name_list:
            df = pd.read_sql_query(f"select {column_name} from {table_name}", engine)
            subject = "NULL VALIDATION"
            percent_missing = int(df.isnull().sum() * 100 / len(df))
            if percent_missing>=75 and percent_missing<=100:
                threshold="RED"
            elif percent_missing<75 and percent_missing>=25:
                threshold="YELLOW"
            else:
                threshold="GREEN"
            if percent_missing==100:
                comments="100 PERCENTAGE NULL validation"
            elif percent_missing<100 and percent_missing>0:
                comments="Partial NULL validation"
            else:
                comments = "------NILL------"
            persist_audit(table_name,subject,column_name,percent_missing,comments,threshold)
            logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def execute_unique_validation(table_name,column_name_list): 
    try:
        for column_name in column_name_list:
            df = pd.read_sql_query(f"select {column_name} from {table_name}", engine)
            subject = "DUPLICATE VALIDATION"
            duplicate_values = (df.duplicated().sum()*100/len(df))
            if duplicate_values >= 1:
                threshold="RED"
                comments="DUPLICATES PRESENT"
            else:
                threshold="GREEN"
                comments = "UNIQUE COLUMN"
            persist_audit(table_name,subject,column_name,duplicate_values,comments,threshold)
            logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def find_historical_records_validation(table_name,column_name):
    try:
        df = pd.read_sql_query(f"select case when (min({column_name})) <= (current_timestamp + interval '-26 month') then 0 else 1 end as validation from {table_name}", engine)
        subject = "HISTORICAL RECORDS VALIDATION"
        historical_records = (df['validation'][0])
        #print(historical_records)
        if df['validation'][0] == 0:
            threshold = "GREEN"
            comments = "Records Present"
        else:
            threshold = "RED"
            comments = "Records missing"          
        persist_audit(table_name,subject,column_name,historical_records,comments,threshold)
        logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def find_missing_records_by_dates(table_name,column_name):
    try:
        data = pd.read_sql_query(f"select {column_name} as data_record from {table_name}",engine)
        subject = "MISSING RECORDS VALIDATION"
        data_list = list(data['data_record'])
        values_1 = data_list[0]
        if isinstance(values_1, date):
            data_1 = pd.read_sql_query(f"select cast({column_name} as date) as date_record from {table_name}  group by cast({column_name} as date) order by cast({column_name} as date)",engine)
            data_list_1 = list(data_1['date_record'])
            start_date = pd.read_sql_query(f"select cast(current_timestamp  + interval '-26 month' as date)",engine)
            end_date = pd.read_sql_query(f"select cast(current_timestamp as date)",engine)
            data_1,data_2 = list(start_date['date']),list(end_date['now'])
            date_1,date_2 = data_1[0],data_2[0]
            df = pd.DataFrame(data_list_1)
            df = df.set_index(0)
            df.index = pd.to_datetime(df.index)
            values = (pd.date_range(start= date_1, end= date_2).difference(df.index).strftime("%Y-%m-%d").tolist())
            if len(values)==0:
                result = "missing_dates = NILL"
                threshold = "GREEN"
                comments = "All Records Present"
            else:
                result =', '.join(values)
                threshold = "RED"
                comments = "RECORDS ARE MISSING"
        else :
            result = "No validation for String/Numerical values"
            threshold = "Green"
            comments = "---NON-DATE COLUMN---"
        persist_audit(table_name,subject,column_name,result,comments,threshold)
        logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)



def find_sudden_spikes_date(table_name,column_name):
    try:
        #df = pd.read_sql_query(f"select count(*), to_char({column_name}, 'yyyy-mm-dd') from {table_name}  group by to_char({column_name}, 'yyyy-mm-dd') order by to_char({column_name}, 'yyyy-mm-dd') asc", engine)
        subject ="SUDDEN SPIKES DETECTION"
        data = pd.read_sql_query(f"select {column_name} as data_record from {table_name}",engine)
        data_list = list(data['data_record'])
        values_1 = data_list[0]
        if isinstance(values_1, date):
            df = pd.read_sql_query(f"select count(*), cast({column_name} as date) as dates from {table_name}  group by cast({column_name} as date)  order by cast({column_name} as date) asc", engine)
            records_count = list(df['count'])
            records_date = list(df['dates'])
            outliers = detect_outliers(records_count)
            outliers_validation = 0 if len(outliers) != 0 else 1
            if outliers_validation == 0:
                ind = 0; indl = []
                for i in range(len(records_count)):
                    if ind < len(outliers):
                        if records_count[i] == outliers[ind]:
                            ind+=1
                            indl.append(i)
                threshold = "RED"
                comments = ""
                for ind in indl :
                    r = str(records_date[ind])+" = "+str(records_count[ind])
                    comments += (r+",")
            else:
                threshold = "GREEN"
                comments = "-----NILL-----"
        else:
            outliers_validation = "No validation for String/Numerical values"
            threshold = "Green"
            comments = "---NON-DATE COLUMN---"

        persist_audit(table_name,subject,column_name,outliers_validation,comments,threshold)
        logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def detect_outliers(records_count):
    outliers = []
    set_threshold = 3
    mean_1 = np.mean(records_count)
    std_1 = np.std(records_count)
    for y in records_count :
        z_score = (y - mean_1)/std_1
        if np.abs(z_score) > set_threshold:
            outliers.append(y)
    return outliers

def find_empty_strings_count(table_name,column_name_list):
    try:
        for column_name in column_name_list:
            df = pd.read_sql_query(f"SELECT {column_name} as list from {table_name}", engine)
            subject = "EMPTY STRING VALIDATION"
            values = list(df['list'])
            for i in range(len(values)):
                values[i]=str(values[i])
            empty_string_result = values.count(' ')
            if empty_string_result == 0:
                threshold = "GREEN"
                comments = "-----NILL-----"
            else:
                threshold = "RED"
                comments = "Empty strings present in a column " +str(empty_string_result)
            persist_audit(table_name,subject,column_name,empty_string_result,comments,threshold)
            logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
            print(error)

def find_max_min_values(table_name,column_name_list):
    try:
        for column_name in column_name_list:
            df = pd.read_sql_query(f"select {column_name} as list from {table_name}", engine)
            subject = "MAX, MIN, AVG VALIDATION"
            values = list(df['list'])
            values_1 = values[0]
            if isinstance(values_1,int) or isinstance(values_1,float):
                min_val,max_val,avg_val = min(values),max(values),sum(values)/len(values)
                result= str(min_val)+ "," +str(max_val)+ "," +str(avg_val)
                comments="Min,Max and Avg values are Obtained"
                threshold = "GREEN"
            elif isinstance(values_1, date):
                df_1 = pd.read_sql_query(f"select EXTRACT(DAY FROM MAX({column_name})-MIN({column_name})) AS duration from {table_name}",engine)
                min_date =  min(values); max_date = max(values)
                days_count = list(df_1['duration'])
                result= str(min_date)+ "," +str(max_date)+ "," +str(days_count[0])
                comments="Earliest_date,latest_date and no.of days are Obtained"
                threshold = "GREEN"
            elif isinstance(values_1, str):
                min_value = "No min for string"
                max_value = "No max for string"
                avg = "No avg for string"
                result= str(min_value)+ "," +str(max_value)+ "," +str(avg)
                comments=" No Min,Max,avg found"
                threshold = "GREEN"
            else:
                min_value = "-----NO VALUES-----"
                max_value = "-----NO VALUES-----"
                avg = "-----NO VALUES-----"
                result= str(min_value)+ "," +str(max_value)+ "," +str(avg)
                comments="Min,Max and Avg values are not calculated"
                threshold = "RED"
            persist_audit(table_name,subject,column_name,result,comments,threshold)
            logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def check_default_columns(table_name):
    try:
        df1=pd.read_sql_query(f"SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name   = '{table_name}' ", engine)
        column_in_table=(df1['column_name'])
        input_columns= (column_in_table.str.cat(sep=', '))
        column_check_list=["DF_SRC_SYS_CD","DF_DATA_SRC_CD","DF_FIRST_REC_ID","DF_LAST_REC_ID","DF_CREATE_DT","DT_UPDATE_DT"]
        subject="COLUMNS CHECK VALIDATION"
        flag=True;missing_list=""
        for column in column_check_list:
            if column not in input_columns:
                missing_list+=(column+":YES, ")
                flag=False 
            else:
                missing_list+=(column+":NO, ")
        if flag!=True:
            result= missing_list[0:len(missing_list)-1]
            comments=f"Column missing in {table_name}"
            threshold="RED"
        else:
            result=f"NO Column missing in {table_name}"
            comments=f"NO Column missing in {table_name}"
            threshold="GREEN"
        persist_audit(table_name,subject,input_columns,result,comments,threshold) 
        logging.info('{}-{}-{}'.format(table_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error) 

#column_check('winlink_customerentityaccount')

def check_schema_historical_records():
    try:
        df1=pd.read_sql_query(f"SELECT distinct(tablename) FROM pg_catalog.pg_tables WHERE schemaname != 'pg_catalog' AND schemaname != 'information_schema' and tablename != 'audittable' and tablename != 'control_table' and tablename != 'accountoriginal' order by tablename asc ", engine)
        table_name_list=list(df1['tablename'])
        table_name = df1['tablename']
        input_tables = (table_name.str.cat(sep=', '))
        column_list = ""
        subject = "CHECK HISTORY OF 1 WEEK RECORDS"
        greater_records_table = ""
        for table in table_name_list:
            column = check(table)
            column_list += (column+",")
            #print(column)
            df2 = pd.read_sql_query(f"select case when (min({column}) between (current_timestamp + interval '-7 days') AND now() ) then 0 else 1 end as validation from {table} ", engine)
            res = int(df2['validation'][0])
            if res == 1:
                greater_records_table += (table+",")
        if len(greater_records_table) == 0:
            threshold = "GREEN"
            comments = "Accurate Data"
            result = "----NILL----"
        else:
            threshold = "RED"
            result = greater_records_table[0:len(greater_records_table)-1]
            comments = "Inaccurate Data"
        column_list = column_list[0:len(column_list)-1]
        persist_audit(input_tables,subject,column_list,result,comments,threshold)    
        logging.info('{}-{}'.format(subject,'JOB SUCCEEDED'))       
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error) 

def check(table):
    try:
        df = pd.read_sql_query(f"select * from control_table", engine)
        table_names = list(df['table_names'])
        schema_records_validation = list(df['stg_schema_records_status'])
        schema_records_validation_columns = list(df['stg_schema_records_column_names'])
        if table in table_names:
            ind = table_names.index(table)
            if schema_records_validation[ind]=="Y":
                
                return schema_records_validation_columns[ind]          
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error) 

def check_data_latency_hour(table_name,column_name):
    try:
        df = pd.read_sql_query(f"Select count(*) from {table_name} where {column_name} > (current_timestamp + interval '-1 hour') ", engine)
        subject = "CHECK DATA LATENCY"
        historical_records = (df['count'][0])
        if historical_records == 0:
            threshold = "GREEN"
            comments = "NO records greater than of last one hour"
        else:
            threshold = "RED"
            comments = "YES records greater than of last one hour"          
        persist_audit(table_name,subject,column_name,historical_records,comments,threshold)
        logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)

def  get_distinct_counts_each_column(table_name,column_name_list):
    try:
        for column_name in column_name_list:

            df = pd.read_sql_query(f"select count({column_name}) as Total_count, count(distinct ({column_name})) as Distinct_count from {table_name}", engine)
            subject = "DISTINCT COUNTS OF EACH COLUMN"
            total_count = list(df['total_count'])
            distinct_count= list(df['distinct_count'])
            records_counts = str(total_count[0]) + " , " + str(distinct_count[0])   
            threshold = "GREEN"
            comments = "The distinct count of the column is :" +str(distinct_count[0])
            persist_audit(table_name,subject,column_name,records_counts,comments,threshold)          
            logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def  get_counts_of_each_value_column(table_name, column_name_list):
    try:
        for column_name in column_name_list:
            df_1 = pd.read_sql_query(f"SELECT {column_name} as value from {table_name}", engine)
            subject = "DISTINCT COUNTS OF EACH VALUES IN A COLUMN"
            value_names = list(df_1['value'])
            value = value_names[0]
            if isinstance(value,date):
                df = pd.read_sql_query(f"select count(*), cast({column_name} as date) as dates from {table_name}  group by cast({column_name} as date) order by cast({column_name} as date) asc", engine)
                dates_column = list(df['dates'])
                count_column = list(df['count'])
                result = ""
                for ind in range(len(dates_column)):    
                    result+=(str(dates_column[ind])+" = "+str(count_column[ind])+", ")   
            else:
                df = pd.read_sql_query(f"SELECT {column_name} as value,COUNT(*) AS Each_value_count FROM {table_name} group by {column_name}",engine)
                value_column = list(df['value'])
                count_column = list(df['each_value_count'])
                result = ""
                for ind in range(len(value_column)):
                    result+=(str(value_column[ind]).replace("'", "`") +" = "+str(count_column[ind])+", ") 
                         
            threshold = "GREEN"
            comments = "Count of each value in a column has been displayed"
            #print(table_name,subject,column_name,result,comments,threshold) 
            persist_audit(table_name,subject,column_name,result,comments,threshold)         
            logging.info('{}-{}-{}-{}'.format(table_name,column_name,subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)

def records_count_of_all_tables():
    try:
        df_1 = pd.read_sql_query(f"SELECT relname as table_name,n_live_tup as records_count FROM pg_stat_user_tables where schemaname = 'public' and relname != 'audittable' and relname != 'control_table' ORDER BY n_live_tup DESC;", engine)
        subject = "RECORDS COUNT OF ALL TABLES IN DB"
        total_count = list(df_1['records_count'])
        table_name_list = list(df_1['table_name'])
        result = ""
        for ind in range(len(table_name_list)):
            result+=(str(table_name_list[ind])+" = "+str(total_count[ind])+", ") 
        column_name = "Records count is taken (column names are not necessary here)"
        threshold = "GREEN"
        comments = "The records count of all the table is obtained"
        persist_audit(' , '.join(table_name_list),subject,column_name,result,comments,threshold)        
        logging.info('{} : {}'.format(subject,"RAN SUCCESSFULLY"))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def execute_null_validation_for_entire_table():
    try:
        df=pd.read_sql_query(f"SELECT table_name  FROM information_schema.tables WHERE table_schema='public' and table_type = 'BASE TABLE' and table_name != 'audittable' and table_name != 'control_table' " , engine)
        table_list=list(df['table_name'])
        result=[]
        subject = 'NULL COUNT OF ENTIRE TABLE'
        column_name = 'Column names not necessary'
        for table in table_list:
            df1=pd.read_sql_query(f"select * from {table}", engine)
            column_list=list(df1)
            sum_of_null_in_table=0
            for column in column_list:
                df2 = pd.read_sql_query(f"select {column} from {table}", engine)
                sum_of_null_in_column = int(df2.isnull().sum())
                sum_of_null_in_table+=sum_of_null_in_column 
            result.append(table+" = "+str(sum_of_null_in_table))
        comments = 'Null count of each table'
        threshold =  'GREEN'
        persist_audit(' , '.join(table_list),subject,column_name,' , '.join(result),comments,threshold)
        logging.info('{}-{}'.format(subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)

def get_duplicates_of_a_table(): 
    try:
        df_2=pd.read_sql_query(f"SELECT table_name  FROM information_schema.tables WHERE table_schema='public' and table_type = 'BASE TABLE' and table_name != 'audittable' and table_name != 'control_table' " , engine)
        table_list=list(df_2['table_name'])
        subject = "DUPLICATES PERCENTAGE OF AN ENTIRE TABLE"
        for table in table_list:
            df = pd.read_sql_query(f"select count(*) as overall_count from {table}", engine)
            overall_cnt = (df['overall_count'][0])
            #print("overall count is :", overall_cnt)
            df_1 = pd.read_sql_query(f"SELECT COUNT(*) as dis_count from ( SELECT DISTINCT * FROM {table} ) T1", engine)
            distinct_count = (df_1['dis_count'][0])
            res = (abs(distinct_count - overall_cnt) / overall_cnt) * 100.0
            column_names = "Column names not necessary"
            if res == 0:
                comments = "No duplicates when checked row wise"
                threshold = "GREEN"
            else:
                comments = "Duplicates present when checked row wise"
                threshold = "RED"
            persist_audit(table,subject,column_names,res,comments,threshold) 
            logging.info('{}-{}'.format(subject,'JOB SUCCEEDED'))
    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)


def get_d_types(table_name):
    try:
        df_1 = pd.read_sql_query(f"SELECT column_name,data_type FROM information_schema.columns WHERE table_name = '{table_name}'", engine)
        subject = "Data types of all column"
        value_names = list(df_1['column_name'])
        data_types = list(df_1['data_type'])
        result = ""
        for ind in range(len(value_names)):
            result+=(str(value_names[ind])+" = "+str(data_types[ind])+", ")
        threshold = "GREEN"
        comments = "Data type of each column is displayed"
        print(table_name,subject,' , '.join(value_names),result,comments,threshold)
        logging.info('{}-{}'.format(subject,'JOB SUCCEEDED'))

    except (Exception, psycopg2.DatabaseError) as error:
        logging.exception(error)