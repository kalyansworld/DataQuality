import psycopg2
import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from datetime import datetime


postgres_host = "pljbd06.prodapt.com"
postgres_user = "postgres"
postgres_password = "newPassword"
postgres_database = "mydatabase"

def persist_audit(audit_table,audit_subject,audit_inputs,audit_result,audit_comments,audit_threshold):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        conn = engine.connect(close_with_result=True)
        audit_time = datetime.now()
        query = f"INSERT INTO audittable (audit_time,audit_table,audit_subject,audit_inputs,audit_result,audit_comments,audit_threshold) VALUES ('{audit_time}','{audit_table}','{audit_subject}','{audit_inputs}','{audit_result}','{audit_comments}','{audit_threshold}');"
        conn.execute(query)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def execute_null_validation(table_name,column_name_list):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
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
                comments="100 % NULL validation"
            elif percent_missing<100 and percent_missing>0:
                comments="Partial NULL validation"
            else:
                comments = "NILL---validation"
            persist_audit(table_name,subject,column_name,percent_missing,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)


def execute_duplicates_validation(table_name,column_name_list): 
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        for column_name in column_name_list:
            df = pd.read_sql_query(f"select {column_name} from {table_name}", engine)
            subject = "DUPLICATE VALIDATION"
            duplicate_values = int(df.duplicated().sum())
            if duplicate_values >= 1:
                threshold="RED"
                comments="DUPLICATE values"
            else:
                threshold="GREEN"
                comments = "NILL---Duplicates"
            persist_audit(table_name,subject,column_name,duplicate_values,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def find_historical_records_validation(table_name,column_name):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        df = pd.read_sql_query(f"select case when (min({column_name})) <= (current_timestamp + interval '-26 month') then 0 else 1 end as validation from {table_name}", engine)
        subject = "HISTORICAL RECORDS VALIDATION"
        #print(type(df))
        historical_records = (df['validation'][0])
        if df['validation'][0] == 1:
            threshold = "GREEN"
            comments = "Records Present"
        else:
            threshold = "RED"
            comments = "Records missing"      
        persist_audit(table_name,subject,column_name,historical_records,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def find_missing_records_by_months(table_name,column_name,expiry_in_months,month_list):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        df = pd.read_sql_query(f"select case when (count(yyymm.to_char)) < {expiry_in_months} then 0 else 1 end as validation from(select (to_char({column_name}, 'YYYY-MM')) from {table_name} where {column_name} > (select current_timestamp + interval '{expiry_in_months} months') group by  to_char({column_name}, 'YYYY-MM') order by to_char({column_name}, 'YYYY-MM') asc) yyymm", engine)
        subject = "MISSING RECORDS VALIDATION"
        #print(type(df))
        historical_records = (df['validation'][0])
        expiry_in_months*=-1
        if historical_records == 0:
            df1 = pd.read_sql_query(f"select (to_char({column_name}, 'YYYY-MM')) from {table_name} where {column_name} > (select current_timestamp + interval '{expiry_in_months} months') group by  to_char({column_name}, 'YYYY-MM') order by to_char({column_name}, 'YYYY-MM') asc ", engine)
            result = list(df1['to_char'])
            missing_months_list = ""
            for month in range(len(month_list)):
                if month_list[month] not in result:
                    missing_months_list+=(month_list[month] + " , ")
            threshold = "RED"
            comments = missing_months_list
        else:
            threshold = "GREEN"
            comments = "No records missing"
        persist_audit(table_name,subject,column_name,historical_records,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def find_sudden_spikes_date(table_name,column_name):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        df = pd.read_sql_query(f"select count(*), to_char({column_name}, 'yyyy-mm-dd') from {table_name}  group by to_char({column_name}, 'yyyy-mm-dd') order by to_char({column_name}, 'yyyy-mm-dd') asc", engine)
        subject ="SUDDEN SPIKES DETECTION"
        records_count = list(df['count'])
        records_date = list(df['to_char'])
        outliers = detect_outliers(records_count)
        outliers_validation = 0 if len(outliers) != 0 else 1
        if outliers_validation == 0:
            '''outliers = []
            set_threshold = 2
            mean_1 = np.mean(records_count)
            std_1 = np.std(records_count)
            for y in records_count :
                z_score = (y - mean_1)/std_1
                if np.abs(z_score) > set_threshold:
                    outliers.append(y) '''
            ind = 0; indl = []
            for i in range(len(records_count)):
                if ind < len(outliers):
                    if records_count[i] == outliers[ind]:
                        ind+=1
                        indl.append(i)
            threshold = "RED"
            comments = ""
            for ind in indl :
                r = str(records_date[ind])+":"+str(records_count[ind])
                comments += (r+",")
        else:
            threshold = "GREEN"
            comments = "No outliers"

        persist_audit(table_name,subject,column_name,outliers_validation,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def detect_outliers(records_count):
    outliers = []
    set_threshold = 0.5
    mean_1 = np.mean(records_count)
    std_1 = np.std(records_count)
    for y in records_count :
        z_score = (y - mean_1)/std_1
        if np.abs(z_score) > set_threshold:
            outliers.append(y)
    return outliers

def find_empty_strings_count(table_name,column_name_list):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        for column_name in column_name_list:
            df = pd.read_sql_query(f"SELECT {column_name} as list from {table_name}", engine)
            subject = "EMPTY STRING VALIDATION"
            #df1 = pd.read_sql_query(f"select count({column_name}) from {table_name}  where ({column_name}) = ' '", engine)
            values = list(df['list'])
            values_1 = values[0]
            #empty_string_result = (df1['count'][0])
            empty_string_result = values.count(' ')
            if isinstance(values_1, str):
                if empty_string_result == 0:
                    threshold = "GREEN"
                    comments = "No Empty Strings"
                else:
                    threshold = "YELLOW"
                    comments = "The number of empty strings present in a column " +str(empty_string_result)
            else:
                comments = "No string data found"
                threshold = "RED"
            persist_audit(table_name,subject,column_name,empty_string_result,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def find_max_min_values(table_name,column_name_list):
      try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        for column_name in column_name_list:
            df = pd.read_sql_query(f"select {column_name} as list from {table_name}", engine)
            subject = "MAX, MIN, AVG VALIDATION"
            values = list(df['list'])
            values_1 = values[0]
            if isinstance(values_1,int) or isinstance(values_1,float):
                min_val,max_val,avg_val = min(values),max(values),sum(values)/len(values)
                result="min_val= "+str(min_val)+","+"max_val= "+str(max_val)+","+"avg_val= "+str(avg_val)
                comments="Min,Max and Avg values are calculated"
                threshold = "GREEN"
            else:
                result="No min,max,avg values for String values"
                comments="Min,Max and Avg values are not calculated"
                threshold = "RED"
            persist_audit(table_name,subject,column_name,result,comments,threshold)
      except (Exception, psycopg2.DatabaseError) as error:
          print(error)


# main function
currentMonth = datetime.now().month
currentYear = datetime.now().year
list_of_tables_columns = {}
table_name_list=[];column_name_list=[]
table_name = "" ; user_input = ""
while user_input != 'N':
    print("Enter the table name")
    table_name = input().strip()
    print('Enter the column name with comma seperated')
    column_name = input().strip()
    column_name_list = column_name.split(',')
    list_of_tables_columns[table_name] = column_name_list 
    print('Enter the Expiry of months:')
    expiry_in_months = int(input())
    month_list=[]
    interval = expiry_in_months
    while(interval>0):
        if currentMonth==0:
            currentYear-=1
            currentMonth=12
        month_list.append("{:d}-{:02d}".format(currentYear,currentMonth))
        interval-=1
        currentMonth-=1
    print('Enter date_column_name for historical, missing and spikes records')
    print("Enter the column_name for:", table_name)
    column_name = input().strip()
    find_historical_records_validation(table_name,column_name)
    find_missing_records_by_months(table_name,column_name,expiry_in_months,month_list)
    find_sudden_spikes_date(table_name,column_name)
    print('Press Y or N')
    user_input = input().strip()
print(list_of_tables_columns)
for table_name in list_of_tables_columns:
    execute_null_validation(table_name,list_of_tables_columns[table_name])
    execute_duplicates_validation(table_name,list_of_tables_columns[table_name])
    find_empty_strings_count(table_name,list_of_tables_columns[table_name])
    find_max_min_values(table_name,list_of_tables_columns[table_name])


    
#print("table_name:{} column_name_list:{}".format(table_name,list_of_tables_columns[table_name]))

