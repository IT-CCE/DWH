import datetime
import json
import sys
import time
import pandas as pd
import numpy as np
import sqlalchemy
from dwh_lib import DWH
import os
from glob import glob

def read_json(path):
    search = sorted(glob(os.path.join(path, "*.json")))
    for i, file in enumerate(search):
        if os.path.basename(file) == 'CONFIG.json':
            with open(file, 'r', encoding='utf-8') as j:
                config = json.loads(j.read())

    return config

def execute_alter(engine: sqlalchemy.Engine,
                  query: str):
    try:
        conn = engine.raw_connection()
        conn.cursor().execute(query)
        conn.commit()
        conn.close()
        return 0
    except Exception as e:
        return -1


if __name__ == '__main__':
    timestamp = datetime.datetime.now()
    config = read_json(sys.argv[1])
    start_time = time.time()
    for key, value in config.items():
        dwh = DWH(timestamp, value['JSON_PATH'], False)
        dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'],
                                       username=dwh.config_json['DWH_USERNAME'],
                                       password=dwh.config_json['DWH_PASSWORD'],
                                       database=dwh.config_json['DWH_DATABASE'])  # connect to database | DWH

        dwh_select_query, dwh_create_query = dwh.read_query(
            query_type='DWH_QUERY')  # select and create query | DWH
        dest_table_dwh = dwh.dest_table(select_query=dwh_create_query)  # destination table DWH
        df_dwh = dwh.create_db_if_not_exists(sql_select_query=dwh_select_query,
                                             sql_create_query=dwh_create_query,
                                             engine=dwh_engine)  # create the database if it does not exist | DWH
        df_dwh = df_dwh.fillna(np.nan).replace([np.nan], [None])
        del_after_date = df_dwh['timestamp'].max()-datetime.timedelta(days=int(value['DELETE ENTRIES OLDER THAN (in Days)']))
        before_entries = df_dwh[df_dwh['timestamp'] <= del_after_date]

        if len(before_entries) > 0:
            unique_entries = list(before_entries['timestamp'].unique())
            delete_timestamps = [x for x in unique_entries if x.weekday() != 6]


            for t in delete_timestamps:
                exit_code = execute_alter(dwh_engine,
                                          f"""DELETE FROM {dest_table_dwh} WHERE [timestamp] = '{t.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'""")
                if exit_code == -1:
                    dwh.exception_handling()
                    quit()
            if len(delete_timestamps) > 0:
                job_select_query, job_create_query = dwh.read_query(query_type='JOB_QUERY')

                dwh.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                             engine=dwh_engine)
                df_job_q = dwh.select_from_db(select_query=job_select_query, engine=dwh_engine)
                dest_table_job = dwh.dest_table(select_query=job_select_query)
                job_columns = dwh.table_columns(df_job_q)
                df_job = pd.DataFrame([["DELETE ENTRIES "+dwh.config_json['JOB_NAME'], len(delete_timestamps), 0,
                                        0, None, dwh.timestamp]], columns=job_columns[1:])

                dwh.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                                    first_insert=False,
                                    add_time_cols=False, mode=2)
    print("Successful")
    print(f"Execution took: {round(time.time() - start_time, 2)}")

