import sys
import pandas as pd
import numpy as np
import json
import sqlalchemy
from sqlalchemy import URL, create_engine
import datetime
from sqlalchemy.exc import ProgrammingError
from glob import glob
import os
import smtplib
from email.message import EmailMessage
import re
import traceback
def create_db_if_not_exists(sql_select_query: str, sql_create_query: str, engine: sqlalchemy.Engine):
    #  tries to connect to database by simple select * from command and if there is an error then it creates database
    try:
        df = select_from_db(sql_select_query, engine=engine)
    except ProgrammingError as e:
        conn = engine.raw_connection()
        conn.cursor().execute(sql_create_query)
        conn.commit()
        conn.close()
        df = select_from_db(sql_select_query, engine=engine)

    return df

def dest_table(select_query: str):
    return re.findall(r'\[.*\]', select_query)[0]

def table_columns(df):
    cols = list(df.columns)
    if 'surkey' in cols:
        cols.remove('surkey')
    return cols

def connect_to_db(server: str, username: str, password: str, database: str):  # Connects to specified database
    con_string = (f'DRIVER={{ODBC Driver 18 for SQL Server}};TrustServerCertificate=yes'
                  f';SERVER={server};DATABASE={database};UID={username};PWD={password}')
    connection_url = URL.create(
        "mssql+pyodbc",
        query={"odbc_connect": con_string}
    )
    db_engine = create_engine(connection_url)
    return db_engine

def read_jsons():  # Reads the specified json file
    json_directory = sys.argv[1]

    search = sorted(glob(os.path.join(json_directory, "*.json")))
    if len(search) == 3:
        for i, file in enumerate(search):
            if os.path.basename(file) == 'COLUMNS.json':
                with open(file, 'r', encoding='utf-8') as j:
                    columns_json = json.loads(j.read())

            elif os.path.basename(file) == 'CONFIG.json':
                with open(file, 'r', encoding='utf-8') as j:
                    config_json = json.loads(j.read())

            elif os.path.basename(file) == 'QUERY.json':
                with open(file, 'r', encoding='utf-8') as j:
                    query_json = json.loads(j.read())

    return (config_json, query_json, columns_json)

def read_query(query_dict: dict):
    if len(query_dict) == 1:
        return query_dict['SELECT'], ""

    return query_dict['SELECT'], query_dict['CREATE']

def select_from_db(sql_query: str, engine: sqlalchemy.Engine):
    with engine.connect() as connection:
        df = pd.read_sql(sql_query, connection)
    return df

def to_python_int(vals):  # change numpy datatypes to python standard datatypes otherwise database error
    outer_list = []
    for rows in vals:
        inner_list = []
        for entry in rows:
            if isinstance(entry, (np.int8, np.int16, np.int32, np.int64)):
                inner_list.append(int(entry))
            elif isinstance(entry, (np.float16, np.float32, np.float64)):
                inner_list.append(float(entry))

            # elif isinstance(entry, datetime.date):
            #     inner_list.append(datetime.datetime.combine(entry,datetime.time.min))
            else:
                inner_list.append(entry)
        outer_list.append(inner_list)

    return outer_list
def insert_into_dwh(destination_table: str, df_insert: pd.DataFrame, engine: sqlalchemy.Engine, first_insert=True,
                    add_time_cols=True):
    #  insert_into_db follows sql: INSERT INTO [DB] (COLUMN1, COLUMN2,..) VALUES (?,?,..)

    if first_insert:
        valid_from = datetime.datetime(1900, 1, 1, 0, 0, second=0)
    else:
        valid_from = timestamp - datetime.timedelta(minutes=1)

    if 'surkey' in df_insert.columns:
        df_insert = df_insert.drop(['surkey'], axis=1, errors='ignore')

    values = df_insert.values.tolist()

    if add_time_cols:
        add = [timestamp, valid_from]
        values = [v + add for v in values]
        sql = f"""INSERT INTO {destination_table} ({','.join([f"[{x}]" for x in df_insert.columns])
                                                    + ',[timestamp],[valid_from]'}) 
                                             VALUES ({("?," * (len(df_insert.columns) + len(add)))[:-1]})"""
    else:
        sql = f"""INSERT INTO {destination_table} ({','.join([f'[{x}]' for x in df_insert.columns])}) 
        VALUES ({("?," * (len(df_insert.columns)))[:-1]})"""

    conn = engine.raw_connection()
    conn.cursor().executemany(sql, to_python_int(values))
    conn.commit()
    conn.close()

def clean_jsons(days=14):
    json_files = glob(f"C:\\Python_DWH\\Check_Tables\\json_files\\*.json")
    json_files = [x for x in json_files if datetime.datetime.strptime(os.path.basename(x).split("_", 1)[1][:-5], "%d_%m_%Y") <= timestamp-datetime.timedelta(days=days)]
    for x in json_files:
        os.remove(x)

if __name__ == '__main__':
    try:
        timestamp = datetime.datetime.now()
        duplicate_rows = {}
        clean_jsons()
        config_json, query_json, columns_json = read_jsons()
        dwh_engine = connect_to_db(server=config_json['DWH_SERVER'], username=config_json['DWH_USERNAME'],
                                   password=config_json['DWH_PASSWORD'],
                                   database=config_json['DWH_DATABASE'])
        dwh_select_query, dwh_create_query = read_query(query_dict=query_json['DWH_QUERY'])
        table_names = select_from_db("""SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';""",dwh_engine)
        table_names = table_names[(table_names['TABLE_SCHEMA']!='dbo') & (table_names['TABLE_SCHEMA']!='job')& (table_names['TABLE_SCHEMA']!='changes')& (table_names['TABLE_SCHEMA']!='stock')]
        table_names = table_names[(table_names['TABLE_NAME']!='plant_data')]
        for i,(db_schema,db_name) in table_names.iterrows():
            df = select_from_db(f"SELECT * FROM [DWH].[{db_schema}].[{db_name}]", dwh_engine)
            pks = table_names = select_from_db(f"""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 AND TABLE_NAME = '{db_name}';""",dwh_engine)
            pks = pks[pks['COLUMN_NAME'] != 'surkey']
            cols = sorted(list(pks['COLUMN_NAME'])+["valid_from","valid_to","timestamp"])
            df = df[cols]

            duplicate_rows_df = df[df.duplicated(keep='first')]

            if len(duplicate_rows_df) > 0:
                all_errors=[]
                for value in duplicate_rows_df.iterrows():
                    condition = ' and '.join([f"[{x}]='{y}'" for x, y in zip(list(value[1].index), value[1].tolist())])
                    all_errors.append(f"SELECT * FROM [DWH].[{db_schema}].[{db_name}] WHERE {condition}")
                duplicate_rows[db_name] = all_errors
        if len(duplicate_rows) > 0:

            json_object = json.dumps(duplicate_rows, indent=4, ensure_ascii=False)
            with open(f"C:\\Python_DWH\\Check_Tables\\json_files\\update_{timestamp.strftime("%d_%m_%Y")}.json", "w", encoding='utf-8') as f:
                f.write(json_object)

            msg = EmailMessage()
            host = "mail.cc-energy.com"
            port = 25

            msg['Subject'] = "Database Errors"
            msg['From'] = 'it@cce-holding.com'
            msg['To'] = 'it@cce-holding.com'
            with open(f"C:\\Python_DWH\\Check_Tables\\json_files\\update_{timestamp.strftime("%d_%m_%Y")}.json", "r") as f:
                msg.add_attachment(
                    f.read(),
                    filename=f"update_{timestamp.strftime("%d_%m_%Y")}.json",
                    subtype="json"
                )
            smtpObj = smtplib.SMTP(host=host, port=port)
            smtpObj.send_message(msg)


            job_select_query, job_create_query = read_query(
                query_dict=query_json['JOB_QUERY'])  # get job select and create query
            create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                    engine=dwh_engine)  # create job database if it does not exist
            df_job_q = select_from_db(sql_query=job_select_query, engine=dwh_engine)  # get entries from job database
            dest_table_job = dest_table(select_query=job_select_query)  # get table name for job table
            job_columns = table_columns(df_job_q)  # get table columns for job table
            df_job = pd.DataFrame([["Check", len(duplicate_rows), 0, 0, None, timestamp]],
                                  columns=job_columns[1:])  # create entry for job
            insert_into_dwh(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine, first_insert=False,
                            add_time_cols=False)
            print("Successful")

    except Exception as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        last_track_back = trace_back[-1]

        job_select_query, job_create_query = read_query(
            query_dict=query_json['JOB_QUERY'])  # get job select and create query
        create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                engine=dwh_engine)  # create job database if it does not exist
        df_job_q = select_from_db(sql_query=job_select_query, engine=dwh_engine)  # get entries from job database
        dest_table_job = dest_table(select_query=job_select_query)  # get table name for job table
        job_columns = table_columns(df_job_q)  # get table columns for job table
        exception_string = (f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
                            f"Exception type: {ex_type}\nException value: {ex_value}")
        df_job = pd.DataFrame([["Check", -1, -1, -1, exception_string, timestamp]],
                              columns=job_columns[1:])
        # create entry for failed job
        insert_into_dwh(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine, first_insert=False,
                        add_time_cols=False)
        print("Failure")