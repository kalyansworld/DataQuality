import psycopg2
import pandas as pd
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

def find_max_min(table_name,column_name):
    try:
        engine = create_engine("postgresql+psycopg2://{user}:{password}@{host}/{database}".format(user=postgres_user,password=postgres_password,host=postgres_host,database=postgres_database))
        df = pd.read_sql_query(f"select {column_name} from {table_name}", engine)
        subject = "Max Min Verification"
        max_min_values = int(df.min()), int(df.max())
        print(max_min_values)
        comments = ("The Maximum and minimum values are :", +max_min_values)
        threshold = 'Green'
        persist_audit(table_name,subject,column_name,max_min_values,comments,threshold)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

table_name=input().strip() #table_name input from user 
column_name=input().strip() #column_name input from user
find_max_min(table_name,column_name)
