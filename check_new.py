import traceback

from dwh_lib import DWH
import os
import glob
import datetime
import time
import sys
import smtplib
from email.message import EmailMessage
import pandas as pd
import json
from sqlalchemy.exc import  ProgrammingError

def clean_jsons(days=14):
    json_files = glob.glob(f"C:\\Python_DWH\\Check_Tables\\json_files\\*.json")
    json_files = [x for x in json_files if datetime.datetime.strptime(os.path.basename(x).split("_", 1)[1][:-5],
                                                                      "%d_%m_%Y") <= timestamp - datetime.timedelta(
        days=days)]
    for x in json_files:
        os.remove(x)


if __name__ == '__main__':
    try:
        start_time = time.time()
        timestamp = datetime.datetime.now()
        dwh = DWH(timestamp, sys.argv[1], True)
        duplicate_rows = {}
        clean_jsons()
        dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'], username=dwh.config_json['DWH_USERNAME'],
                                       password=dwh.config_json['DWH_PASSWORD'],
                                       database=dwh.config_json['DWH_DATABASE']
                                    )
        dwh_select_query, dwh_create_query = dwh.read_query(query_type='DWH_QUERY')
        table_names = dwh.select_from_db(
            """SELECT TABLE_SCHEMA,TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE';""",
            dwh_engine)
        table_names = table_names[(table_names['TABLE_SCHEMA'] != 'dbo') & (table_names['TABLE_SCHEMA'] != 'job') & (
                    table_names['TABLE_SCHEMA'] != 'changes')]
        for i, (db_schema, db_name) in table_names.iterrows():
            df = dwh.select_from_db(f"SELECT * FROM [DWH].[{db_schema}].[{db_name}]", dwh_engine)


            pks = table_names = dwh.select_from_db(
                f"""SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE WHERE
                 OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1 
                 AND TABLE_NAME = '{db_name}';""",
                dwh_engine)
            pks = pks[pks['COLUMN_NAME'] != 'surkey']

            if "valid_from" in pks and "valid_to" in pks and "timestamp" in pks:
                cols = sorted(list(pks['COLUMN_NAME']) + ["valid_from", "valid_to", "timestamp"])
                df = df[cols]
                duplicate_rows_df = df[df.duplicated(keep='first')]

                df2 = dwh.select_from_db(f"SELECT * FROM [DWH].[{db_schema}].[{db_name}] where [valid_from] is NULL",
                                         dwh_engine)
                df2 = df2[cols]
                duplicate_rows_df = pd.concat([df2,duplicate_rows_df])


                if len(duplicate_rows_df) > 0:
                    all_errors = []
                    for value in duplicate_rows_df.iterrows():
                        condition = ' and '.join([f"[{x}]='{y}'" for x, y in zip(list(value[1].index), value[1].tolist())])
                        all_errors.append(f"SELECT * FROM [DWH].[{db_schema}].[{db_name}] WHERE {condition}")
                    duplicate_rows[db_name] = all_errors

        if len(duplicate_rows) > 0:
            json_object = json.dumps(duplicate_rows, indent=4, ensure_ascii=False)
            with open(f"C:\\Python_DWH\\Check_Tables\\json_files\\update_{timestamp.strftime("%d_%m_%Y")}.json", "w",
                      encoding='utf-8') as f:
                f.write(json_object)

            msg = EmailMessage()
            host = "mail.cc-energy.com"
            port = 25

            msg['Subject'] = "Database Errors"
            msg['From'] = 'it@cce-holding.com'
            msg['To'] = 'it@cce-holding.com'
            with open(f"C:\\Python_DWH\\Check_Tables\\json_files\\update_{timestamp.strftime("%d_%m_%Y")}.json",
                      "r") as f:
                msg.add_attachment(
                    f.read(),
                    filename=f"update_{timestamp.strftime("%d_%m_%Y")}.json",
                    subtype="json"
                )
            smtpObj = smtplib.SMTP(host=host, port=port)
            smtpObj.send_message(msg)

        job_select_query, job_create_query = dwh.read_query(
            query_type='JOB_QUERY')  # get job select and create query
        dwh.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                    engine=dwh_engine)  # create job database if it does not exist
        df_job_q = dwh.select_from_db(select_query=job_select_query,
                                      engine=dwh_engine)  # get entries from job database
        dest_table_job = dwh.dest_table(select_query=job_select_query)  # get table name for job table
        job_columns = dwh.table_columns(df_job_q)  # get table columns for job table
        df_job = pd.DataFrame([["Check", len(duplicate_rows), 0, 0, None, timestamp]],
                              columns=job_columns[1:])  # create entry for job
        dwh.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                           first_insert=False,
                           add_time_cols=False)
        print("Successful")

    except (Exception,) as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        last_track_back = trace_back[-1]

        job_select_query, job_create_query = dwh.read_query(query_type='JOB_QUERY')  # get job select and create query
        dwh.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                    engine=dwh_engine)  # create job database if it does not exist
        df_job_q = dwh.select_from_db(select_query=job_select_query, engine=dwh_engine)  # get entries from job database
        dest_table_job = dwh.dest_table(select_query=job_select_query)  # get table name for job table
        job_columns = dwh.table_columns(df_job_q)  # get table columns for job table
        exception_string = (f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
                            f"Exception type: {ex_type}\nException value: {ex_value}")
        df_job = pd.DataFrame([["Check", -1, -1, -1, exception_string, dwh.timestamp]],
                              columns=job_columns[1:])
        # create entry for failed job
        dwh.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine, first_insert=False,
                           add_time_cols=False)
        print("Failure")
