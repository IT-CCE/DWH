import traceback

from dwh_lib import DWH
import datetime
import sys
import glob
import os
import pandas as pd
import numpy as np
import time

if __name__ == '__main__':
    try:
        start_time = time.time()
        timestamp = datetime.datetime.now()
        dwh = DWH(timestamp, sys.argv[1], True)
        excel_path = sys.argv[2]

        excel_file_names = glob.glob(os.path.join(excel_path, "**\\*.xlsx"), recursive=True)
        excel_file_names.sort(key=lambda op: os.path.getctime(op), reverse=True)

        # excel
        df_excel = pd.read_excel(excel_file_names[0], engine='openpyxl', header=1)  # read excel
        df_excel_columns = dwh.config_json['EXCEL_COLUMN_NAMES'].split(",")
        df_excel.columns = df_excel_columns
        df_excel = df_excel.dropna(subset=[df_excel_columns[0]])  # Drop Entries with no Value in first columns
        df_excel = df_excel.fillna(np.nan).replace([np.nan], [None])  # fill np.nan with python None
        df_excel[df_excel_columns[0]] = df_excel[df_excel_columns[0]].astype(int)

        year, month = excel_file_names[0].split("\\")[-1].split(" ")[0].split("-")
        df_excel['year'] = int(year)
        df_excel['month'] = int(month)
        excel_year, excel_month = int(year), int(month)

        # db connectors
        source_engine = dwh.connect_to_db(server=dwh.config_json['SOURCE_SERVER'], username=dwh.config_json['SOURCE_USERNAME'],
                                       password=dwh.config_json['SOURCE_PASSWORD'],
                                       database=dwh.config_json['SOURCE_DATABASE'])

        dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'], username=dwh.config_json['DWH_USERNAME'],
                                       password=dwh.config_json['DWH_PASSWORD'],
                                       database=dwh.config_json['DWH_DATABASE'])

        # read queries
        source_query, _ = dwh.read_query(query_type='SOURCE_QUERY')

        dwh_select_query, dwh_create_query = dwh.read_query(query_type='DWH_QUERY')

        job_select_query, job_create_query = dwh.read_query(query_type='JOB_QUERY')  # get job select and create query

        datatypes = dwh.config_json['EXCEL_DATATYPES'].split(";")

        # # source
        df_source = dwh.select_from_db(select_query=source_query, engine=source_engine)

        # merge
        df_merged = pd.merge(df_excel, df_source, on=['Anlage', 'year', 'month'], how='left')

        df_merged = df_merged.fillna(np.nan).replace([np.nan], [None])

        datatypes_query = f"DECLARE @query nvarchar(max) = '{source_query.replace('\'', '\'\'')}';\nEXEC sp_describe_first_result_set @query, null, 0;"
        df_datatypes = dwh.select_from_db(datatypes_query, source_engine)[['name', 'system_type_name']]

        datatype_source = []

        for i, x in df_datatypes.iterrows():
            if x['name'] not in ['Anlage', 'year', 'month']:
                datatype_source.append(x['system_type_name'])

        datatypes.extend(datatype_source)

        # create_config(df_merged, datatypes)

        df_merged['timestamp'] = timestamp

        df_dwh = dwh.create_db_if_not_exists(sql_select_query=dwh_select_query, sql_create_query=dwh_create_query,
                                             engine=dwh_engine)

        if len(df_dwh[(df_dwh['year'] == excel_year) & (df_dwh['month'] == excel_month)]) == 0:

            dest_table_dwh = dwh.dest_table(select_query=dwh_create_query)

            dwh.insert_into_db(destination_table=dest_table_dwh, df_insert=df_merged,
                               engine=dwh_engine)  # insert all entries
            new_rows = df_merged  # get values for job table
            updates_rows = []  # get values for job table

            # job
            dwh.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                        engine=dwh_engine)  # create job database if it does not exist
            df_job_q = dwh.select_from_db(select_query=job_select_query,
                                          engine=dwh_engine)  # get entries from job database
            dest_table_job = dwh.dest_table(select_query=job_select_query)  # get table name for job table
            job_columns = dwh.table_columns(df_job_q)  # get table columns for job table
            df_job = pd.DataFrame([[dwh.config_json['JOB_NAME'], len(new_rows), len(updates_rows), 0, None, timestamp]],
                                  columns=job_columns[1:])  # create entry for job
            dwh.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine)  # insert the row
            # os.remove(excel_file_names[0])
            print("Successful")
            print(f"Execution took: {round(time.time() - start_time, 2)}")

        else:
            print("Nothing to insert")

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
        df_job = pd.DataFrame([[dwh.config_json['JOB_NAME'], -1, -1, -1, exception_string, dwh.timestamp]],
                              columns=job_columns[1:])
        # create entry for failed job
        dwh.insert_into_db(destination_table=dest_table_job, df_insert=df_job,
                           engine=dwh_engine)  # insert the failure row
        print("Failed")
        print(f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
              f"Exception type: {ex_type}\nException value: {ex_value}")
