import sys
import pandas as pd
import numpy as np
import json
import sqlalchemy
from pandas import DataFrame, Series
from sqlalchemy import URL, create_engine, Engine
import datetime
from sqlalchemy.exc import ProgrammingError
import traceback
import re
from glob import glob
import os
from functools import reduce
import time
from typing import List, Union, Tuple, Any


class DWH:

    def __init__(self, timestamp: datetime.datetime, path: str, add_valid_from: bool) -> None:
        self.timestamp = timestamp
        self.config_json = None
        self.columns_json = None
        self.query_json = None
        self.path = path
        self.read_jsons(self.path)
        self.add_valid_from = add_valid_from

    def connect_to_db(self, server, username, password, database) -> Engine:
        """
        Establishes a Connection to the Database with the given Parameter

        :param db_type: str: Either DWH or SOURCE

        :return: sqlalchemy.Engine: database engine for operations
        """

        con_string = (f'DRIVER={{ODBC Driver 18 for SQL Server}};TrustServerCertificate=yes'
                      f';SERVER={server};DATABASE={database};UID={username};PWD={password}')
        connection_url = URL.create(
            "mssql+pyodbc",
            query={"odbc_connect": con_string}
        )
        db_engine = create_engine(connection_url)
        return db_engine

    def read_jsons(self, path) -> None:
        """
        Reads the json files with the path given in the argument
        :return: None
        """

        search = sorted(glob(os.path.join(path, "*.json")))
        for i, file in enumerate(search):
            if os.path.basename(file) == 'COLUMNS.json':
                with open(file, 'r', encoding='utf-8') as j:
                    self.columns_json = json.loads(j.read())

            elif os.path.basename(file) == 'CONFIG.json':
                with open(file, 'r', encoding='utf-8') as j:
                    self.config_json = json.loads(j.read())

            elif os.path.basename(file) == 'QUERY.json':
                with open(file, 'r', encoding='utf-8') as j:
                    self.query_json = json.loads(j.read())

    def to_python_vals(self, vals: List, cols, mode=1) -> List:
        """
        Changes the numpy database to python standard datatypes
        :param vals: List: of Lists that contain the values that should be converted
        :return:  List: converted List
        """

        if mode == 1:
            outer_list = []
            for rows in vals:
                inner_list = []
                for i, entry in enumerate(rows):
                    try:
                        datatype = self.columns_json[cols[i]]
                    except (KeyError, IndexError) as e:
                        datatype = None

                    if isinstance(entry, (np.int8, np.int16, np.int32, np.int64)):
                        entry = int(entry)
                    elif isinstance(entry, (np.float16, np.float32, np.float64)):
                        entry = float(entry)
                    elif isinstance(entry, np.bool_):
                        entry = bool(entry)

                    inner_list.append(entry)
                outer_list.append(inner_list)
        else:  # job
            outer_list = []
            for rows in vals:
                inner_list = []
                for entry in rows:
                    if isinstance(entry, (np.int8, np.int16, np.int32, np.int64)):
                        inner_list.append(int(entry))
                    elif isinstance(entry, (np.float16, np.float32, np.float64)):
                        inner_list.append(float(entry))
                    elif isinstance(entry, np.bool_):
                        inner_list.append(bool(entry))

                    else:
                        inner_list.append(entry)
                outer_list.append(inner_list)

        return outer_list

    def read_query(self, query_type: str) -> Union[str, tuple[str, str]]:
        """
        Returns the query for the specified dictionary
        :param query_type: str: of either SOURCE_QUERY,DWH_QUERY, CHANGE_QUERY
        :return: str: select query or tuple: select and create query
        """
        query_dict = self.query_json[query_type]

        if len(query_dict) == 1:
            return query_dict['SELECT']

        return query_dict['SELECT'], query_dict['CREATE']

    def identify_rows(self, scd_type2_columns: List, scd_type1_columns: List, dw_data_warehouse: pd.DataFrame,
                      df_source_table: pd.DataFrame, change_db_cols: List,
                      unique_pk: List) -> Tuple[DataFrame, DataFrame, List[Series | Any]]:
        """
        Function that compares current dwh table with source table and returns entries for change table, entries that
        should be inserted, entries that must be set to invalid
        :param change_db_cols: list: columns for change table
        :param scd_type2_columns: list: column names that should be scd2 updated
        :param scd_type1_columns: list: column names that should be simply overwritten
        :param dw_data_warehouse: pd.Dataframe: current version of the dwh table only entries with valid_to is None
        :param df_source_table: pd.Dataframe: current version of the source table
        :param unique_pk: list: primary keys of the table to be inserted
        :return: tuple: values for change table, values for new entries, values for updating
        """
        scd = (pd.concat([dw_data_warehouse, df_source_table])
               .drop_duplicates(keep=False, subset=scd_type2_columns + scd_type1_columns + unique_pk))
        scd = scd[
            scd.duplicated(keep=False, subset=unique_pk)]  # in scd there are all rows that are changing (old and new)
        scd = scd.fillna(np.nan).replace([np.nan], [None])

        new_records = pd.concat([dw_data_warehouse, df_source_table]).drop_duplicates(subset=unique_pk,
                                                                                      keep=False).reset_index()
        new_records = new_records.fillna(np.nan).replace([np.nan], [None])
        values_change_table = []
        scd2_new_rows = []
        update_rows = []

        for i, record in new_records.iterrows():
            if len(pd.merge(scd, pd.DataFrame(record).transpose(),
                            on=unique_pk)) != 0:  # if the new records appears in the scd2
                scd.drop(index=record['index'], inplace=True, errors='ignore')  # drop it

            if len(pd.merge(dw_data_warehouse, pd.DataFrame(record).transpose(),
                            on=unique_pk)) == 1:  # records that need to be "deleted"
                record = record.copy()
                record['timestamp'] = self.timestamp - datetime.timedelta(minutes=1)
                record['valid_to'] = self.timestamp
                record.drop(labels=['index'], errors='ignore', inplace=True)
                update_rows.append(record)
                values_change_table.append(record[unique_pk].to_list() + [None, None, self.timestamp, "Deleted Row"])
                new_records.drop(index=i, inplace=True)
        new_records.drop(labels=['index'], errors='ignore', inplace=True, axis=1)

        if len(scd) == len(df_source_table) * 2:  # if new column(s) were added
            for i, condition in scd[unique_pk].drop_duplicates().iterrows():
                all_filters = [scd[x] == y if y is not None else scd[x].isna() for x, y in
                               zip(condition.index, condition)]
                filter_rows = reduce(lambda x, y: x & y, all_filters)  # go over all old_row,new_row pairs for scd2
                df_diff = scd.loc[filter_rows]
                old_entry = df_diff.iloc[-2]
                new_entry = df_diff.iloc[-1]
                old_entry_comp = old_entry.drop(labels=['surkey', 'timestamp', 'valid_to', 'valid_from'],
                                                errors='ignore')
                new_entry_comp = new_entry.drop(labels=['surkey', 'timestamp', 'valid_to', 'valid_from'],
                                                errors='ignore')
                comparison = old_entry_comp.compare(new_entry_comp)  # get changed columns
                old_vals_df, new_vals_df = comparison['self'], comparison['other']

                old_entry1 = old_entry.copy()
                for _, (old_val, new_val) in enumerate(zip(old_vals_df.items(), new_vals_df.items())):
                    old_entry1[new_val[0]] = new_val[1]  # modify the added column(s)
                old_entry1['timestamp'] = self.timestamp - datetime.timedelta(minutes=1)
                old_entry1['valid_to'] = None
                update_rows.append(old_entry1)  # update the rows

        else:
            if len(scd) > 0:

                for i, condition in scd[unique_pk].drop_duplicates().iterrows():
                    # all_filters = [scd[x] == y for x, y in zip(condition.index, condition)]
                    all_filters = [scd[x] == y if y is not None else scd[x].isna() for x, y in
                                   zip(condition.index, condition)]
                    filter_rows = reduce(lambda x, y: x & y, all_filters)  #
                    df_diff = scd.loc[filter_rows]
                    old_entry = df_diff.iloc[-2]
                    new_entry = df_diff.iloc[-1]
                    old_entry1 = old_entry.copy()
                    old_entry_comp = old_entry.drop(labels=['surkey', 'timestamp', 'valid_to', 'valid_from'],
                                                    errors='ignore')
                    new_entry_comp = new_entry.drop(labels=['surkey', 'timestamp', 'valid_to', 'valid_from'],
                                                    errors='ignore')

                    comparison = old_entry_comp.compare(new_entry_comp)
                    comparison.drop(labels=scd_type1_columns, inplace=True, errors='ignore')
                    old_vals_df, new_vals_df = comparison['self'], comparison[
                        'other']  # old_vals_df and new_vals_df only contain scd2

                    for _, (old_val, new_val) in enumerate(zip(old_vals_df.items(), new_vals_df.items())):
                        key_cols = scd.loc[filter_rows][unique_pk].iloc[0].to_list()
                        key_cols.extend(
                            [old_val[1], new_val[1], self.timestamp,
                             # fill list with old_value, new_value, columns for change table
                             old_val[0]])
                        values_change_table.append(key_cols)

                    old_entry1['timestamp'] = self.timestamp - datetime.timedelta(
                        minutes=1)  # set timestamp and valid_to
                    old_entry1['valid_to'] = self.timestamp
                    if len(comparison) > 0:
                        update_rows.append(old_entry1)  # update valid,timestamp
                        scd2_new_rows.append(new_entry)  # new entry contains all new entries
                    else:
                        new_entry1 = new_entry.copy()
                        new_entry1['timestamp'] = self.timestamp - datetime.timedelta(minutes=1)
                        new_entry1['valid_to'] = None
                        new_entry1['valid_from'] = old_entry['valid_from']
                        update_rows.append(new_entry1)

        if len(new_records) > 0:
            for _, record in new_records.iterrows():
                values_change_table.append(record[unique_pk].to_list() + [None, None, self.timestamp, "New Row"])

        if len(new_records) > 0 and len(
                scd2_new_rows) > 0:  # if there are new_record and new_records from scd2 concat them
            insert_rows = pd.concat([pd.DataFrame(scd2_new_rows), new_records.dropna(axis=1)])
        elif len(scd2_new_rows) > 0:  # if there are only records for scd2
            insert_rows = pd.DataFrame(scd2_new_rows)
        else:
            insert_rows = new_records

        insert_rows.drop(['timestamp', 'valid_to', 'valid_from'], axis=1, inplace=True)
        insert_rows = insert_rows.fillna(np.nan).replace([np.nan], [None])

        values_change_table = pd.DataFrame(values_change_table, columns=change_db_cols)
        values_change_table = values_change_table.fillna(np.nan).replace([np.nan], [None])

        return values_change_table, insert_rows, update_rows

    def update_records(self, destination_table: str, update_entries: list, columns: list, primary_key: list,
                       engine: sqlalchemy.Engine, mode) -> None:
        # update_records follows sql: UPDATE [DB] SET [C1] = ?, [C2] = ?, ... WHERE [UNIQUE_ROWS] = UNIQUE_VALUE
        conn = engine.raw_connection()
        [columns.remove(x) for x in primary_key]
        if 'timestamp' not in columns and 'valid_from' not in columns and 'valid_to' not in columns:
            columns.extend(['timestamp', 'valid_from', 'valid_to'])

        for row in update_entries:
            where = row[primary_key + ['valid_from']]

            row.drop(['surkey'] + primary_key, inplace=True, errors='ignore')

            sql_query = f"""UPDATE {destination_table} SET {", ".join([f'[{x}] = ?' for x in columns])} 
            WHERE {" AND ".join([f"[{x}]=\'{y}\'" if x != 'valid_from'
                                                                                                                                        else f"[{x}]=\'{y.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}\'"
                                 for x, y in zip(where.index.tolist(), where.tolist())])}"""
            conn.cursor().executemany(sql_query, self.to_python_vals([row.values.tolist()], columns, mode))

        conn.commit()
        conn.close()

    def insert_into_db(self, destination_table: str, df_insert: pd.DataFrame, engine: sqlalchemy.Engine,
                       first_insert=False, add_time_cols=True, mode=1) -> None:
        """
        Function to insert entries in the format: INSERT INTO [DB] (COLUMN1, COLUMN2,..) VALUES (?,?,..)
        :param mode:
        :param destination_table: str: the destination table in the form: [..].[..].[..]
        :param df_insert: pd.Dataframe: of the entries for insertion
        :param engine: sqlalchemy.Engine: database engine where to insert entries
        :param first_insert: boolean: if true sets valid_from to 01.01.1900 00:00:00,
                                      otherwise subtracts 1 minute from timestamp
        :param add_time_cols: : boolean: if true adds timestamp and valid_from otherwise ignores them
        :return: None
        """

        if first_insert:
            valid_from = datetime.datetime(1900, 1, 1, 0, 0, second=0)
        else:
            valid_from = self.timestamp - datetime.timedelta(minutes=1)

        if 'surkey' in df_insert.columns:
            df_insert = df_insert.drop(['surkey'], axis=1, errors='ignore')

        values = df_insert.values.tolist()

        if add_time_cols:
            if self.add_valid_from:
                add = [self.timestamp, valid_from]
                add_names = ',[timestamp],[valid_from]'
            else:
                add = [self.timestamp]
                add_names = ',[timestamp]'
            values = [v + add for v in values]
            sql = f"""INSERT INTO {destination_table} ({','.join([f"[{x}]" for x in df_insert.columns])
                                                        + add_names}) 
                                                 VALUES ({("?," * (len(df_insert.columns) + len(add)))[:-1]})"""
        else:
            sql = f"""INSERT INTO {destination_table} ({','.join([f'[{x}]' for x in df_insert.columns])}) 
            VALUES ({("?," * (len(df_insert.columns)))[:-1]})"""

        conn = engine.raw_connection()
        conn.cursor().executemany(sql, self.to_python_vals(values, df_insert.columns, mode))
        conn.commit()
        conn.close()

    def select_from_db(self, select_query: str, engine: sqlalchemy.Engine) -> pd.DataFrame:
        """
        Execute select statement on a given database engine
        :param select_query: str: the sql select query
        :param engine: sqlalchemy.Engine: database engine where to execute queries
        :return: content of the database table as pandas Dataframe
        """
        with engine.connect() as connection:
            df = pd.read_sql(select_query, connection)
        return df

    def create_db_if_not_exists(self, sql_select_query: str, sql_create_query: str,
                                engine: sqlalchemy.Engine) -> pd.DataFrame:
        """
        Tries to connect to a database.
        If ProgrammingError -> Create database and returns the emtpy dataframe
        If Success --> Returns dataframe of the database
        :param sql_select_query: str: of the select query
        :param sql_create_query: str: of the create query
        :param engine: sqlalchemy.Engine: database engine where to execute queries
        :return: pd.Dataframe: content of the database table
        """
        try:
            df = self.select_from_db(sql_select_query, engine=engine)
        except ProgrammingError as e:
            conn = engine.raw_connection()
            conn.cursor().execute(sql_create_query)
            conn.commit()
            conn.close()
            df = self.select_from_db(sql_select_query, engine=engine)

        return df

    def dest_table(self, select_query: str) -> str:
        """
        Searches for the Destination Table in a select query
        :param select_query: str that defines the select query
        :return: str: the destination table in the form: [..].[..].[..]
        """
        return re.findall(r'\[.*\]', select_query)[0]

    def table_columns(self, df: pd.DataFrame) -> List:
        """
        Returns the columns of a Dataframe without surkey column
        :param df: pd.Dataframe: dataframe where to extract columns from
        :return: List: column names
        """
        cols = list(df.columns)
        if 'surkey' in cols:
            cols.remove('surkey')
        return cols

    def exception_handling(self):
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        last_track_back = trace_back[-1]

        dwh_engine = self.connect_to_db(server=self.config_json['DWH_SERVER'],
                                        username=self.config_json['DWH_USERNAME'],
                                        password=self.config_json['DWH_PASSWORD'],
                                        database=self.config_json['DWH_DATABASE'])

        job_select_query, job_create_query = self.read_query(query_type='JOB_QUERY')
        self.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                     engine=dwh_engine)
        df_job_q = self.select_from_db(select_query=job_select_query,
                                       engine=dwh_engine)  # get entries from job database
        dest_table_job = self.dest_table(select_query=job_select_query)  # get table name for job table
        job_columns = self.table_columns(df_job_q)  # get table columns for job table
        exception_string = (f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
                            f"Exception type: {ex_type}\nException value: {ex_value}")
        df_job = pd.DataFrame([[self.config_json['JOB_NAME'], -1, -1, -1, exception_string, self.timestamp]],
                              columns=job_columns[1:])
        # create entry for failed job
        self.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                            first_insert=False,
                            add_time_cols=False)  # insert the failure row
        print("Failed")
        print(f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
              f"Exception type: {ex_type}\nException value: {ex_value}")

        folder = f"C:\\Python_DWH\\Error_Log\\{datetime.datetime.strftime(self.timestamp, "%d_%m_%Y")}"
        if not os.path.exists(folder):
            os.mkdir(folder)

        with open(folder + "\\error.txt", "w") as f:
            f.write(f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
                    f"Exception type: {ex_type}\nException value: {ex_value}")

        if hasattr(self, 'new_rows'):
            self.new_rows.to_csv(folder + "\\new_rows.csv", header=True, na_rep="None", encoding='utf-8')
        if hasattr(self, 'scd2_vals'):
            self.scd2_vals.to_csv(folder + "\\scd2_vals.csv", header=True, na_rep="None", encoding='utf-8')
        if hasattr(self, 'updates_rows'):
            df_update_rows = pd.DataFrame(self.updates_rows)
            df_update_rows.to_csv(folder + "\\updates.csv", header=True, na_rep="None", encoding='utf-8')
        if hasattr(self, 'df_dwh'):
            self.df_dwh.to_csv(folder + "\\df_dwh.csv", header=True, na_rep="None", encoding='utf-8')
        if hasattr(self, 'df_source'):
            self.df_source.to_csv(folder + "\\df_source.csv", header=True, na_rep="None", encoding='utf-8')

    def execute_source_dwh(self):
        """
        Performs database update with source and dwh parameter defined in the config
        :return: None
        """
        start_time = time.time()

        try:
            ########################################## DWH Operations ################################################
            dwh_engine = self.connect_to_db(server=self.config_json['DWH_SERVER'],
                                            username=self.config_json['DWH_USERNAME'],
                                            password=self.config_json['DWH_PASSWORD'],
                                            database=self.config_json['DWH_DATABASE'])  # connect to database | DWH

            dwh_select_query, dwh_create_query = self.read_query(
                query_type='DWH_QUERY')  # select and create query | DWH
            dest_table_dwh = self.dest_table(select_query=dwh_create_query)  # destination table DWH
            self.df_dwh = self.create_db_if_not_exists(sql_select_query=dwh_select_query,
                                                       sql_create_query=dwh_create_query,
                                                       engine=dwh_engine)  # create the database if it does not exist | DWH
            self.df_dwh = self.df_dwh.fillna(np.nan).replace([np.nan], [None])

            # datatypes_query = f"DECLARE @query nvarchar(max) = '{dwh_select_query.replace('\'', '\'\'')}';\nEXEC sp_describe_first_result_set @query, null, 0;"
            #
            # df_datatypes = self.select_from_db(datatypes_query,dwh_engine)

            ######################################## Source Operations ################################################
            source_engine = self.connect_to_db(server=self.config_json['SOURCE_SERVER'],
                                               username=self.config_json['SOURCE_USERNAME'],
                                               password=self.config_json['SOURCE_PASSWORD'],
                                               database=self.config_json[
                                                   'SOURCE_DATABASE'])  # connect to source database

            source_select_query = self.read_query(query_type='SOURCE_QUERY')

            self.df_source = self.select_from_db(select_query=source_select_query,
                                                 engine=source_engine)  # execute query on source
            self.df_source = self.df_source.fillna(np.nan).replace([np.nan], [None])  # replace np.nan with python None

            ##################################### Change Operations ##################################################
            if self.config_json['MODE'] == 'SCD':
                change_select_query, change_create_query = self.read_query(query_type='CHANGE_QUERY')

                self.create_db_if_not_exists(sql_select_query=change_select_query, sql_create_query=change_create_query,
                                             engine=dwh_engine)
                df_change = self.select_from_db(select_query=change_select_query, engine=dwh_engine)

                change_table = self.dest_table(select_query=change_select_query)  # get the change table name
                change_db_cols = self.table_columns(df_change)  # get the change table columns

            ########################################## Insertions ###################################################

            if len(self.df_dwh) == 0 or self.config_json['MODE'] == 'INSERT':  # if the data warehouse table is empty
                self.insert_into_db(destination_table=dest_table_dwh, df_insert=self.df_source, engine=dwh_engine)
                self.new_rows = self.df_source  # get values for job table
                self.updates_rows = []  # get values for job table

            else:
                scd_t2_columns = [x for x in self.config_json['SCD2'].split(",") if len(x) > 0] if len(
                    self.config_json['SCD2']) > 0 else []
                scd_t1_columns = [x for x in self.config_json['SCD1'].split(",") if len(x) > 0] if len(
                    self.config_json['SCD1']) > 0 else []  # get all the scd type 1 columns
                unique_columns = self.config_json['DWH_UNIQUE_ENTRIES'].split(",")  # get the primary key(s)

                self.scd2_vals, self.new_rows, self.updates_rows = self.identify_rows(scd_type2_columns=scd_t2_columns,
                                                                                      scd_type1_columns=scd_t1_columns,
                                                                                      dw_data_warehouse=self.df_dwh,
                                                                                      df_source_table=self.df_source,
                                                                                      change_db_cols=change_db_cols,
                                                                                      unique_pk=unique_columns)

                ##################################### Update ##########################################################
                if len(self.updates_rows) > 0:
                    self.update_records(destination_table=dest_table_dwh, update_entries=self.updates_rows,
                                        columns=list(self.updates_rows[0].keys()),
                                        primary_key=unique_columns,
                                        engine=dwh_engine, mode=1)
                ##################################### Change ##########################################################
                if len(self.scd2_vals) > 0:
                    self.insert_into_db(destination_table=change_table, df_insert=self.scd2_vals, engine=dwh_engine,
                                        first_insert=False, add_time_cols=False, mode=1)

                ##################################### Insert ###########################################################
                if len(self.new_rows) > 0:
                    self.insert_into_db(destination_table=dest_table_dwh, df_insert=self.new_rows, engine=dwh_engine,
                                        first_insert=False, mode=1)

                ###################################### Job ############################################################

            job_select_query, job_create_query = self.read_query(query_type='JOB_QUERY')

            self.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                         engine=dwh_engine)
            df_job_q = self.select_from_db(select_query=job_select_query, engine=dwh_engine)
            dest_table_job = self.dest_table(select_query=job_select_query)
            job_columns = self.table_columns(df_job_q)
            df_job = pd.DataFrame([[self.config_json['JOB_NAME'], len(self.new_rows), len(self.updates_rows),
                                    0, None, self.timestamp]], columns=job_columns[1:])

            self.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                                first_insert=False,
                                add_time_cols=False, mode=2)

            print("Successful")
            print(f"Execution took: {round(time.time() - start_time, 2)}")

        except (Exception,):
            self.exception_handling()

    def execute_source(self, df_source: pd.DataFrame):
        """
        Performs database update with a given external source (Not Database)
        :return: None
        """
        start_time = time.time()

        try:
            ########################################## DWH Operations ################################################
            dwh_engine = self.connect_to_db(server=self.config_json['DWH_SERVER'],
                                            username=self.config_json['DWH_USERNAME'],
                                            password=self.config_json['DWH_PASSWORD'],
                                            database=self.config_json['DWH_DATABASE'])  # connect to database | DWH

            dwh_select_query, dwh_create_query = self.read_query(
                query_type='DWH_QUERY')  # select and create query | DWH
            dest_table_dwh = self.dest_table(select_query=dwh_create_query)  # destination table DWH
            df_dwh = self.create_db_if_not_exists(sql_select_query=dwh_select_query, sql_create_query=dwh_create_query,
                                                  engine=dwh_engine)  # create the database if it does not exist | DWH
            df_dwh = df_dwh.fillna(np.nan).replace([np.nan], [None])

            ######################################## Source Operations ################################################

            ##################################### Change Operations ##################################################
            if self.config_json['MODE'] == 'SCD':
                change_select_query, change_create_query = self.read_query(query_type='CHANGE_QUERY')

                self.create_db_if_not_exists(sql_select_query=change_select_query, sql_create_query=change_create_query,
                                             engine=dwh_engine)
                df_change = self.select_from_db(select_query=change_select_query, engine=dwh_engine)

                change_table = self.dest_table(select_query=change_select_query)  # get the change table name
                change_db_cols = self.table_columns(df_change)  # get the change table columns

            ########################################## Insertions ###################################################

            if len(df_dwh) == 0 or self.config_json['MODE'] == 'INSERT':  # if the data warehouse table is empty
                self.insert_into_db(destination_table=dest_table_dwh, df_insert=df_source, engine=dwh_engine, mode=1)
                self.new_rows = df_source  # get values for job table
                self.updates_rows = []  # get values for job table

            else:
                scd_t2_columns = [x for x in self.config_json['SCD2'].split(",") if len(x) > 0] if len(
                    self.config_json['SCD2']) > 0 else []
                scd_t1_columns = [x for x in self.config_json['SCD1'].split(",") if len(x) > 0] if len(
                    self.config_json['SCD1']) > 0 else []  # get all the scd type 1 columns
                unique_columns = self.config_json['DWH_UNIQUE_ENTRIES'].split(",")  # get the primary key(s)

                self.scd2_vals, self.new_rows, self.updates_rows = self.identify_rows(scd_type2_columns=scd_t2_columns,
                                                                                      scd_type1_columns=scd_t1_columns,
                                                                                      dw_data_warehouse=df_dwh,
                                                                                      df_source_table=df_source,
                                                                                      change_db_cols=change_db_cols,
                                                                                      unique_pk=unique_columns)

                ##################################### Update ##########################################################
                if len(self.updates_rows) > 0:
                    self.update_records(destination_table=dest_table_dwh, update_entries=self.updates_rows,
                                        columns=list(self.updates_rows[0].keys()),
                                        primary_key=unique_columns,
                                        engine=dwh_engine, mode=1)
                ##################################### Change ##########################################################
                if len(self.scd2_vals) > 0:
                    self.insert_into_db(destination_table=change_table, df_insert=self.scd2_vals, engine=dwh_engine,
                                        first_insert=False, add_time_cols=False, mode=1)

                ##################################### Insert ###########################################################
                if len(self.new_rows) > 0:
                    self.insert_into_db(destination_table=dest_table_dwh, df_insert=self.new_rows, engine=dwh_engine,
                                        first_insert=False, mode=1)

                ###################################### Job ############################################################

            job_select_query, job_create_query = self.read_query(query_type='JOB_QUERY')

            self.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                         engine=dwh_engine)
            df_job_q = self.select_from_db(select_query=job_select_query, engine=dwh_engine)
            dest_table_job = self.dest_table(select_query=job_select_query)
            job_columns = self.table_columns(df_job_q)
            df_job = pd.DataFrame([[self.config_json['JOB_NAME'], len(self.new_rows), len(self.updates_rows),
                                    0, None, self.timestamp]], columns=job_columns[1:])

            self.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                                first_insert=False,
                                add_time_cols=False, mode=2)

            print("Successful")
            print(f"Execution took: {round(time.time() - start_time, 2)}")

        except (Exception,):
            self.exception_handling()

    def execute_plain_insert_source(self, df_source):
        """
        Performs database update with a given external source (Not Database) by using a plain insert (No SCD)
        :return: None
        """
        start_time = time.time()

        try:
            ########################################## DWH Operations ################################################
            dwh_engine = self.connect_to_db(server=self.config_json['DWH_SERVER'],
                                            username=self.config_json['DWH_USERNAME'],
                                            password=self.config_json['DWH_PASSWORD'],
                                            database=self.config_json['DWH_DATABASE'])  # connect to database | DWH

            dwh_select_query, dwh_create_query = self.read_query(
                query_type='DWH_QUERY')  # select and create query | DWH
            dest_table_dwh = self.dest_table(select_query=dwh_create_query)  # destination table DWH
            self.create_db_if_not_exists(sql_select_query=dwh_select_query, sql_create_query=dwh_create_query,
                                         engine=dwh_engine)  # create the database if it does not exist | DWH
            ########################################## Insertions ###################################################

            self.insert_into_db(destination_table=dest_table_dwh, df_insert=df_source, engine=dwh_engine, mode=1)
            self.new_rows = df_source  # get values for job table
            self.updates_rows = []  # get values for job table

            ###################################### Job ############################################################

            job_select_query, job_create_query = self.read_query(query_type='JOB_QUERY')

            self.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                         engine=dwh_engine)
            df_job_q = self.select_from_db(select_query=job_select_query, engine=dwh_engine)
            dest_table_job = self.dest_table(select_query=job_select_query)
            job_columns = self.table_columns(df_job_q)
            df_job = pd.DataFrame([[self.config_json['JOB_NAME'], len(self.new_rows), len(self.updates_rows),
                                    0, None, self.timestamp]], columns=job_columns[1:])

            self.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                                first_insert=False,
                                add_time_cols=False, mode=2)

            print("Successful")
            print(f"Execution took: {round(time.time() - start_time, 2)}")

        except (Exception,):
            self.exception_handling()
