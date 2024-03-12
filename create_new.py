import datetime
import json
import math
import os
import re
import tkinter as tk
from glob import glob
from tkinter import filedialog
from tkinter import messagebox

import pandas as pd
import sqlalchemy
from sqlalchemy import URL, create_engine
from sqlalchemy.exc import ProgrammingError
from db_new import main
import sys


class TK_GUI():
    def dest_table(self, select_query: str):
        return re.findall(r'\[.*\]', select_query)[0]

    # return Parser(select_query).tables[0]

    def execute_alter(self, server: str, username: str, password: str, database: str,
                      query: str, vals=None):
        try:
            con_string = (f'DRIVER={{ODBC Driver 18 for SQL Server}};TrustServerCertificate=yes'
                          f';SERVER={server};DATABASE={database};UID={username};PWD={password}')
            connection_url = URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": con_string}
            )
            engine = create_engine(connection_url)
            conn = engine.raw_connection()
            if vals:
                conn.cursor().execute(query,vals)
            else:
                conn.cursor().execute(query)
            conn.commit()
            conn.close()
        except Exception as e:
            return e

    def connect_to_db(self, server: str, username: str, password: str, database: str,
                      query: str, return_val=False, job=False):  # Connects to specified database
        try:
            con_string = (f'DRIVER={{ODBC Driver 18 for SQL Server}};TrustServerCertificate=yes'
                          f';SERVER={server};DATABASE={database};UID={username};PWD={password}')
            connection_url = URL.create(
                "mssql+pyodbc",
                query={"odbc_connect": con_string}
            )
            db_engine = create_engine(connection_url)
            if return_val:
                return self.select_from_db(db_engine, query, return_val, job)
            else:
                self.select_from_db(db_engine, query, return_val, job)
        except sqlalchemy.exc.InterfaceError as e:
            messagebox.showerror("Error", f"Wrong Credentials for {server}_{database}: username: {username}, password: {password}")

    def select_from_db(self, engine: sqlalchemy.Engine, query, return_val=False, job=False, ):
        success = True
        try:
            with engine.connect() as connection:
                df = pd.read_sql(query, connection)

            if return_val:
                if job:
                    return df
                messagebox.showerror("Error", f"The table from the query: \n\n{query}\n\n already Exists")
                self.window.mainloop()
            else:
                self.select_types(df.columns)
        except ProgrammingError as e:
            return e

    def select_types(self, columns):

        self.window.withdraw()
        self.new_r = tk.Toplevel(self.window)
        self.new_r.geometry("1500x1000")
        if self.scd_or_insert_var.get() == 'SCD':
            cols = 10 if len(columns) > 10 else len(columns)
            rows = math.ceil(len(columns) / cols)
            options = ["No SCD Type", "SCD Type 1", "SCD Type 2", "Primary Key"]
            i = 0
            for r in range(0, rows * 2, 2):
                for c in range(cols):
                    clicked = tk.StringVar()
                    clicked.set(options[2])
                    l = tk.Label(self.new_r, text=f'{columns[i]}')
                    drop = tk.OptionMenu(self.new_r, clicked, *options)
                    l.grid(row=r, column=c, padx=15, pady=15)
                    drop.grid(row=r + 1, column=c, padx=15, pady=15)
                    self.vars.append((clicked, columns[i]))
                    i += 1
                    if i == len(columns):
                        break


        name_l = tk.Label(self.new_r, text=f'JOB Name:')
        self.name_e = tk.Entry(self.new_r, width=30)
        self.name_e.insert(0, 'RTF')
        dwh_schema_l = tk.Label(self.new_r, text=f'DWH Schema:')
        self.dwh_schema_e = tk.Entry(self.new_r, width=30)
        self.dwh_schema_e.insert(0, 'sales')
        dwh_l = tk.Label(self.new_r, text=f'DWH Table Name:')
        self.dwh_e = tk.Entry(self.new_r, width=30)
        self.dwh_e.insert(0, 'rtf')
        if self.scd_or_insert_var.get() == 'SCD':
            change_dwh_l = tk.Label(self.new_r, text=f'Change Table Name:')
            self.change_dwh_e = tk.Entry(self.new_r, width=30)
            self.change_dwh_e.insert(0, '..._change')
        b2 = tk.Button(self.new_r, text="Create", command=self.check_if_table_exists)

        name_l.place(relx=0.5, rely=0.55, anchor=tk.CENTER)
        self.name_e.place(relx=0.5, rely=0.575, anchor=tk.CENTER)
        dwh_schema_l.place(relx=0.5, rely=0.65, anchor=tk.CENTER)
        self.dwh_schema_e.place(relx=0.5, rely=0.675, anchor=tk.CENTER)
        dwh_l.place(relx=0.5, rely=0.75, anchor=tk.CENTER)
        self.dwh_e.place(relx=0.5, rely=0.775, anchor=tk.CENTER)
        if self.scd_or_insert_var.get() == 'SCD':
            change_dwh_l.place(relx=0.5, rely=0.85, anchor=tk.CENTER)
            self.change_dwh_e.place(relx=0.5, rely=0.875, anchor=tk.CENTER)
        b2.place(relx=0.5, rely=0.95, anchor=tk.CENTER)

        #

    def set_connect_button_visable(self, event):
        self.connect_button.place(relx=0.5, rely=0.95, anchor=tk.CENTER)

    def create_files(self):

        source_query = self.source_query.get("1.0", tk.END)
        datatypes_query = f"DECLARE @query nvarchar(max) = '{source_query.replace('\'', '\'\'')}';\nEXEC sp_describe_first_result_set @query, null, 0;"
        df_datatypes = self.connect_to_db(self.source_server_entry.get(), self.source_username_entry.get(),
                                          self.source_password_entry.get(),
                                          self.source_database_entry.get(), query=datatypes_query, return_val=True,
                                          job=True)[['name', 'system_type_name']]

        self.whole_path = os.path.join(r'C:\Python_DWH', os.path.join(self.fact_dim_var.get(), self.name_e.get()))
        is_created = False
        try:
            os.makedirs(self.whole_path)
        except FileExistsError as e:
            ans = messagebox.askokcancel("Warning",
                                         f"Attention the folder {self.whole_path} already exits\n Continue to overwrite existing?",
                                         icon=tk.messagebox.WARNING)
            is_created = False if ans else True

        self.create_dict = {}
        self.create_dict['SOURCE_QUERY'] = {}
        self.create_dict['DWH_QUERY'] = {}
        self.create_dict['JOB_QUERY'] = {}
        self.create_dict['CHANGE_QUERY'] = {}

        if not is_created:
            self.create_dict['SOURCE_QUERY']['SELECT'] = self.source_query.get("1.0",
                                                                               tk.END)  # f"{self.whole_path}/SOURCE_QUERY.txt"
            self.pks = []
            self.scd2 = []
            self.scd1 = []
            self.no_type = []
            columns_json = {}

            dwh_create = f"CREATE TABLE [{self.dwh_database_entry.get()}].[{self.dwh_schema_e.get()}].[{self.dwh_e.get()}] (\n\t[surkey] int identity NOT NULL,\n"  # os.path.join(self.whole_path,r'DWH_QUERY.txt'
            dwh_select = f"SELECT\n"

            if self.scd_or_insert_var.get() == 'SCD':

                for i, ((click, col), (_, datatype)) in enumerate(zip(self.vars, df_datatypes.iterrows())):
                    columns_json[datatype.iloc[0]] = datatype.iloc[1]
                    if click.get() == 'Primary Key' and datatype.iloc[0] == col:
                        dwh_create += f"\t[{col}] {datatype.iloc[1]} NOT NULL,\n"
                        dwh_select += f"\t[{col}],\n"
                        self.pks.append((col, datatype.iloc[1]))
                    elif click.get() == 'SCD Type 2' and datatype.iloc[0] == col:
                        dwh_create += f"\t[{col}] {datatype.iloc[1]},\n"
                        dwh_select += f"\t[{col}],\n"
                        self.scd2.append(col)
                    elif click.get() == 'SCD Type 1' and datatype.iloc[0] == col:
                        dwh_create += f"\t[{col}] {datatype.iloc[1]},\n"
                        dwh_select += f"\t[{col}],\n"
                        self.scd1.append(col)
                    elif click.get() == 'No SCD Type' and datatype.iloc[0] == col:
                        dwh_create += f"\t[{col}] {datatype.iloc[1]},\n"
                        dwh_select += f"\t[{col}],\n"
                        self.no_type.append(col)
            else:
                for i,datatype in df_datatypes.iterrows():

                    columns_json[datatype.iloc[0]] = datatype.iloc[1]
                    dwh_create += f"\t[{datatype.iloc[0]}] {datatype.iloc[1]},\n"
                    dwh_select += f"\t[{datatype.iloc[0]}],\n"

            if self.scd_or_insert_var.get() == 'SCD':
                dwh_create += "\t[timestamp] datetime,\n\t[valid_from] datetime,\n\t[valid_to] datetime\n"
                dwh_create += f"Primary Key (" + ','.join(['[' + x[0] + ']' for x in self.pks]) + ',[surkey])\n)\n'
            else:
                dwh_create += "\t[timestamp] datetime\n"
                dwh_create += f"Primary Key ([surkey])\n)\n"

            self.create_dict['DWH_QUERY']['CREATE'] = dwh_create
            if self.scd_or_insert_var.get() == 'SCD':
                dwh_select+=f"\t[timestamp],\n\t[valid_from],\n\t[valid_to]\nFROM [{self.dwh_database_entry.get()}].[{self.dwh_schema_e.get()}].[{self.dwh_e.get()}]\nWHERE [valid_to] IS NULL"
            else:
                dwh_select += f"\t[timestamp]\nFROM [{self.dwh_database_entry.get()}].[{self.dwh_schema_e.get()}].[{self.dwh_e.get()}]"

            self.create_dict['DWH_QUERY']['SELECT'] = dwh_select

            self.create_dict['JOB_QUERY']['CREATE'] = f"CREATE TABLE [{self.dwh_database_entry.get()}].[job].[job_table](\n\
            \t[id] int identity NOT NULL,\n\
            \t[name] nvarchar(max) NOT NULL,\n\
            \t[length_new_rows] int,\n\
            \t[length_updated_rows] int,\n\
            \t[exit_code] int NOT NULL,\n\
            \t[exception] nvarchar(max),\n\
            \t[timestamp] datetime NOT NULL,\
            \nPrimary Key ([id])\n)"

            self.create_dict['JOB_QUERY'][
                'SELECT'] = f"SELECT *\nFROM\n[{self.dwh_database_entry.get()}].[job].[job_table]"
            if self.scd_or_insert_var.get() == 'SCD':
                change_query_create = ""
                change_query_create += f"CREATE TABLE [{self.dwh_database_entry.get()}].[changes].[{self.change_dwh_e.get()}] (\n"
                for (col, datatype) in self.pks:
                    change_query_create += f"\t[{col}] {datatype} NOT NULL,\n"

                change_query_create += f"\t[OLD_VALUE] nvarchar(max),\n\t[NEW_VALUE] nvarchar(max),\n\t[timestamp] datetime,\n\t[CHANGED_COLUMN] nvarchar(50)\n"
                change_query_create += f"Primary Key ({','.join(['[' + x[0] + ']' for x in self.pks])}, [timestamp], [CHANGED_COLUMN])\n)\n"

                self.create_dict['CHANGE_QUERY']['CREATE'] = change_query_create
                self.create_dict['CHANGE_QUERY'][
                    'SELECT'] = f"SELECT\n*\nFROM [{self.dwh_database_entry.get()}].[changes].[{self.change_dwh_e.get()}]"

            json_dict = {
                "DWH_SERVER": self.dwh_servername_entry.get(),
                "DWH_DATABASE": self.dwh_database_entry.get(),
                "DWH_USERNAME": self.dwh_username_entry.get(),
                "DWH_PASSWORD": self.dwh_password_entry.get(),
                "DWH_UNIQUE_ENTRIES": ",".join([x[0] for x in self.pks]),

                "SOURCE_SERVER": self.source_server_entry.get(),
                "SOURCE_USERNAME": self.source_username_entry.get(),
                "SOURCE_PASSWORD": self.source_password_entry.get(),
                "SOURCE_DATABASE": self.source_database_entry.get(),

                "JOB_NAME": self.name_e.get(),

                "SCD1": ",".join(self.scd1) if len(self.scd1) > 0 else "",
                "SCD2": ",".join(self.scd2) if len(self.scd2) > 0 else "",
                "MODE": self.scd_or_insert_var.get()
            }

            json_object = json.dumps(self.create_dict, indent=4, ensure_ascii=False)
            with open(os.path.join(self.whole_path, "QUERY.json"), "w", encoding='utf-8') as f:
                f.write(json_object)

            json_object = json.dumps(json_dict, indent=4, ensure_ascii=False)
            with open(os.path.join(self.whole_path, f"CONFIG.json"), "w", encoding='utf-8') as f:
                f.write(json_object)

            json_cols = json.dumps(columns_json, indent=4, ensure_ascii=False)
            with open(os.path.join(self.whole_path, f"COLUMNS.json"), "w", encoding='utf-8') as f:
                f.write(json_cols)

            messagebox.showinfo("Info", "Files successful created")
            self.window1.quit()

    def check_if_table_exists(self):
        if any([click.get() == "Primary Key" for click, _ in self.vars]) or self.scd_or_insert_var.get() == 'INSERT':
            query = f'''SELECT * FROM [{self.dwh_database_entry.get()}].[{self.dwh_schema_e.get()}].[{self.dwh_e.get()}]'''
            self.connect_to_db(self.dwh_servername_entry.get(), self.dwh_username_entry.get(),
                               self.dwh_password_entry.get(),
                               self.dwh_database_entry.get(), query, True, job=True)
            if self.scd_or_insert_var.get() == 'SCD':
                query = f'''SELECT * FROM [{self.dwh_database_entry.get()}].[changes].[{self.change_dwh_e.get()}]'''
                self.connect_to_db(self.dwh_servername_entry.get(), self.dwh_username_entry.get(),
                                   self.dwh_password_entry.get(),
                                   self.dwh_database_entry.get(), query, True)

            query = f'''SELECT * FROM [{self.dwh_database_entry.get()}].[job].[job_table]'''
            df = self.connect_to_db(self.dwh_servername_entry.get(), self.dwh_username_entry.get(),
                                    self.dwh_password_entry.get(),
                                    self.dwh_database_entry.get(), query, True, True)

            if not self.name_e.get() in df['name'].unique():
                self.create_files()
            else:
                messagebox.showerror("Error", f"The name: {self.name_e.get()} is already in the job database")
        else:
            messagebox.showerror("Error", "At Least one Primary Key must be selected")

    def create_job(self):
        self.vars = []
        self.window = tk.Toplevel(self.window1)
        self.window1.withdraw()
        self.source_server_label = tk.Label(self.window, text="Source Servername: ")
        self.source_server_entry = tk.Entry(self.window, width=30)
        self.source_server_entry.insert(index=0, string=r"AT1SQLINST02\APPLUS", )
        self.source_username_label = tk.Label(self.window, text="Source Username: ")
        self.source_username_entry = tk.Entry(self.window, width=30)
        self.source_username_entry.insert(index=0, string=r"DWH_read_all")
        self.source_password_label = tk.Label(self.window, text="Source Passwort: ")
        self.source_password_entry = tk.Entry(self.window, width=30 ,show="*")
        self.source_password_entry.insert(index=0, string=r"PQYH1oXkCmQEd52xak3M")
        self.source_database_label = tk.Label(self.window, text="Source Database: ")
        self.source_database_entry = tk.Entry(self.window, width=30)
        self.source_database_entry.insert(index=0, string=r"APplusProd7")

        self.dwh_servername_label = tk.Label(self.window, text="DWH Servername: ")
        self.dwh_servername_entry = tk.Entry(self.window, width=30)
        self.dwh_servername_entry.insert(index=0, string=r"AT1SQLINST03\DWH", )
        self.dwh_username_label = tk.Label(self.window, text="DWH Username: ")
        self.dwh_username_entry = tk.Entry(self.window, width=30)
        self.dwh_username_entry.insert(index=0, string=r"DWH_Write")
        self.dwh_password_label = tk.Label(self.window, text="DWH Passwort: ")
        self.dwh_password_entry = tk.Entry(self.window, width=30,show="*")
        self.dwh_password_entry.insert(index=0, string=r"wZfw1Ly6wuBvbNbABZxB")
        self.dwh_database_label = tk.Label(self.window, text="DWH Database: ")
        self.dwh_database_entry = tk.Entry(self.window, width=30)
        self.dwh_database_entry.insert(index=0, string=r"DWH")

        options = ['Json_Files']
        self.fact_dim_var = tk.StringVar()
        self.scd_or_insert_var = tk.StringVar()
        self.fact_dim_label = tk.Label(self.window, text=f'Location')


        self.scd_or_insert = tk.Label(self.window, text=f'SCD or Plain INSERT')

        self.fact_dim_drop = tk.OptionMenu(self.window, self.fact_dim_var, *options,
                                           command=self.set_connect_button_visable)

        self.scd_or_insert_drop = tk.OptionMenu(self.window, self.scd_or_insert_var, *['SCD', 'INSERT'])


        self.sourcer_query_label = tk.Label(self.window, text="Query: ")
        self.source_query = tk.Text(self.window, width=50, height=10)
        self.source_query.insert(tk.END, r"SELECT * FROM 123")
        self.connect_button = tk.Button(self.window, text='Connect',
                                        command=lambda: self.connect_to_db(self.source_server_entry.get(),
                                                                           self.source_username_entry.get(),
                                                                           self.source_password_entry.get(),
                                                                           self.source_database_entry.get(),
                                                                           self.source_query.get("1.0", tk.END)))

        self.source_server_label.place(relx=0.25, rely=0.1, anchor=tk.CENTER)
        self.source_server_entry.place(relx=0.25, rely=0.15, anchor=tk.CENTER)
        self.source_username_label.place(relx=0.25, rely=0.2, anchor=tk.CENTER)
        self.source_username_entry.place(relx=0.25, rely=0.25, anchor=tk.CENTER)
        self.source_password_label.place(relx=0.25, rely=0.3, anchor=tk.CENTER)
        self.source_password_entry.place(relx=0.25, rely=0.35, anchor=tk.CENTER)
        self.source_database_label.place(relx=0.25, rely=0.4, anchor=tk.CENTER)
        self.source_database_entry.place(relx=0.25, rely=0.45, anchor=tk.CENTER)

        self.dwh_servername_label.place(relx=0.75, rely=0.1, anchor=tk.CENTER)
        self.dwh_servername_entry.place(relx=0.75, rely=0.15, anchor=tk.CENTER)
        self.dwh_username_label.place(relx=0.75, rely=0.2, anchor=tk.CENTER)
        self.dwh_username_entry.place(relx=0.75, rely=0.25, anchor=tk.CENTER)
        self.dwh_password_label.place(relx=0.75, rely=0.3, anchor=tk.CENTER)
        self.dwh_password_entry.place(relx=0.75, rely=0.35, anchor=tk.CENTER)
        self.dwh_database_label.place(relx=0.75, rely=0.4, anchor=tk.CENTER)
        self.dwh_database_entry.place(relx=0.75, rely=0.45, anchor=tk.CENTER)

        self.scd_or_insert.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        self.scd_or_insert_drop.place(relx=0.5, rely=0.55, anchor=tk.CENTER)

        self.fact_dim_label.place(relx=0.5, rely=0.6, anchor=tk.CENTER)
        self.fact_dim_drop.place(relx=0.5, rely=0.65, anchor=tk.CENTER)

        self.sourcer_query_label.place(relx=0.5, rely=0.7, anchor=tk.CENTER)
        self.source_query.place(relx=0.5, rely=0.8, anchor=tk.CENTER)
        self.connect_button.place(relx=0.5, rely=0.925, anchor=tk.CENTER)
        self.window.geometry("700x1000")

    def select_job(self, next_function):
        self.folder_selected = filedialog.askdirectory()
        search = sorted(glob(os.path.join(self.folder_selected, "*.json")))
        if len(search) == 3:
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
            next_function()
        else:
            messagebox.showerror("Error", f"Path {self.folder_selected} does not contain the 3 needed files")

    def compare_window(self):
        self.window2 = tk.Toplevel(self.window)
        self.window.withdraw()
        self.window2.geometry("1800x800")

        self.source_query_old = self.query_json['SOURCE_QUERY']['SELECT']

        self.old_query_label = tk.Label(self.window2, text='Old Query')
        self.old_query_entry = tk.Text(self.window2, width=100, height=30)
        self.old_query_entry.insert(index=tk.END, chars=self.source_query_old)

        self.old_query_label.place(rely=0.1, relx=0.25, width=100, height=30, anchor=tk.CENTER)
        self.old_query_entry.place(rely=0.45, relx=0.25, anchor=tk.CENTER)

        self.new_query_label = tk.Label(self.window2, text='New Query')
        self.new_query_entry = tk.Text(self.window2, width=100, height=30)

        self.new_query_label.place(rely=0.1, relx=0.75, anchor=tk.CENTER)
        self.new_query_entry.place(rely=0.45, relx=0.75, anchor=tk.CENTER)
        self.new_query_entry.insert(index=tk.END, chars="""SELECT * FROM ANGEBOT""")
        self.compare_button = tk.Button(self.window2, text='Compare', command=self.compare_dfs)

        self.compare_button.place(rely=0.8, relx=0.5, anchor=tk.CENTER)

    def add_column_to_files(self):

        self.scd2_alter = []
        self.scd1_alter = []
        self.no_type_alter = []

        datatypes_query = f"""DECLARE @query nvarchar(max) = '{self.query_json['SOURCE_QUERY']['SELECT'].replace('\'', '\'\'')}';\nEXEC sp_describe_first_result_set @query, null, 0;"""
        df_datatypes = self.connect_to_db(self.config_json['SOURCE_SERVER'],
                                          self.config_json['SOURCE_USERNAME'],
                                          self.config_json['SOURCE_PASSWORD'],
                                          self.config_json['SOURCE_DATABASE'],
                                          query=datatypes_query, return_val=True, job=True)[
            ['name', 'system_type_name']]

        self.alter_rows = []
        dwh_create = self.query_json['DWH_QUERY']['CREATE']
        dwh_select = self.query_json['DWH_QUERY']['SELECT']

        new_dwh_create = ""
        new_dwh_select = "SELECT \n"


        i1 = dwh_create.index("\n")
        new_dwh_create += dwh_create[:i1]
        new_dwh_create += "\n\t[surkey] int identity NOT NULL,\n"


        i = 0
        for _, datatype in df_datatypes.iterrows():
            if datatype.iloc[0] in self.columns_json:
                new_dwh_create += f"\t[{datatype.iloc[0]}] {self.columns_json[datatype.iloc[0]]},\n"
                new_dwh_select += f"\t[{datatype.iloc[0]}],\n"
            else:
                self.alter_rows.append((datatype.iloc[0], datatype.iloc[1]))
                new_dwh_create += f"\t[{datatype.iloc[0]}] {datatype.iloc[1]},\n"
                new_dwh_select += f"\t[{datatype.iloc[0]}],\n"
                self.columns_json[datatype.iloc[0]] = datatype.iloc[1]
                click, _ = self.vars1[i]
                if click.get() == 'SCD Type 2':
                    self.scd2_alter.append(datatype.iloc[0])
                elif click.get() == 'SCD Type 1':
                    self.scd1_alter.append(datatype.iloc[0])
                i += 1

        i12 = dwh_select.index("\t[timestamp]")
        new_dwh_select += dwh_select[i12:]

        i13 = dwh_create.index("\t[timestamp]")
        new_dwh_create += dwh_create[i13:]

        self.query_json['DWH_QUERY']['CREATE'] = new_dwh_create
        self.query_json['DWH_QUERY']['SELECT'] = new_dwh_select

        self.config_json["SCD1"] = self.config_json["SCD1"] + ("," if len(self.scd1_alter) > 0 else "") + (
            ",".join(self.scd1_alter) if len(self.scd1_alter) > 0 else "")
        self.config_json["SCD2"] = self.config_json["SCD2"] + ("," if len(self.scd2_alter) > 0 else "") + (
            ",".join(self.scd2_alter) if len(self.scd2_alter) > 0 else "")

        json_object = json.dumps(self.config_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, 'CONFIG.json'), "w", encoding='utf-8') as f:
            f.write(json_object)

        json_object = json.dumps(self.columns_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, "COLUMNS.json"), "w", encoding='utf-8') as f:
            f.write(json_object)

        json_object = json.dumps(self.query_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, "QUERY.json"), "w", encoding='utf-8') as f:
            f.write(json_object)

    def remove_column_from_files(self):

        old_dwh_query = self.query_json['DWH_QUERY']['CREATE']
        old_dwh_create = self.query_json['DWH_QUERY']['SELECT']
        new_dwh_query = ""
        new_dwh_create = ""

        for col in self.delete_columns:
            i1 = old_dwh_query.index(f"[{col}] {self.columns_json[col]},\n\t")
            i2 = i1 + len(f"[{col}] {self.columns_json[col]},\n\t")

            i11 = old_dwh_create.index(f"[{col}],\n\t")
            i22 = i11 + len(f"[{col}],\n\t")

            new_dwh_create = old_dwh_create[:i11] + old_dwh_create[i22:]

            new_dwh_query = old_dwh_query[:i1] + old_dwh_query[i2:]
            if col+"," in self.config_json['SCD1']:
                self.config_json['SCD1'] = self.config_json['SCD1'].replace(f'{col},', '')

            elif ","+col in self.config_json['SCD1']:
                self.config_json['SCD1'] = self.config_json['SCD1'].replace(f',{col}', '')

            elif col in self.config_json['SCD1']:
                self.config_json['SCD1'] = self.config_json['SCD1'].replace(f'{col}', '')

            if col+"," in self.config_json['SCD2']:
                self.config_json['SCD2'] = self.config_json['SCD2'].replace(f'{col},', '')
            elif ","+col in self.config_json['SCD2']:
                self.config_json['SCD2'] = self.config_json['SCD2'].replace(f',{col}', '')
            elif col in self.config_json['SCD2']:
                self.config_json['SCD2'] = self.config_json['SCD2'].replace(f'{col}', '')

            if col in self.columns_json:
                self.columns_json.pop(col)
            old_dwh_query = new_dwh_query
            old_dwh_create = new_dwh_create

        self.query_json['DWH_QUERY']['CREATE'] = new_dwh_query
        self.query_json['DWH_QUERY']['SELECT'] = new_dwh_create

        json_object = json.dumps(self.config_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, 'CONFIG.json'), "w", encoding='utf-8') as f:
            f.write(json_object)

        json_object = json.dumps(self.columns_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, "COLUMNS.json"), "w", encoding='utf-8') as f:
            f.write(json_object)

        json_object = json.dumps(self.query_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, "QUERY.json"), "w", encoding='utf-8') as f:
            f.write(json_object)

    def remove_col_from_database(self):
        table_name = self.dest_table(self.query_json['DWH_QUERY']['CREATE'])
        query = f'ALTER TABLE {table_name}'
        for i, col in enumerate(self.delete_columns):
            if i ==0:
               query+=" DROP COLUMN "
            query += f'[{col}]'
            if i != len(self.delete_columns) - 1:
                query += ", "
        e = self.execute_alter(self.config_json['DWH_SERVER'],
                               self.config_json['DWH_USERNAME'],
                               self.config_json['DWH_PASSWORD'],
                               self.config_json['DWH_DATABASE'],
                               query=query)
        if not isinstance(e,Exception):
            table_name2 = self.dest_table(self.query_json['JOB_QUERY']['CREATE'])
            values = [f'{self.config_json['JOB_NAME']} - Removed COLUMN(S): [{','.join(self.delete_columns)}] from {table_name}', 0,0,0,None,datetime.datetime.now()]
            query = f'INSERT INTO {table_name2} VALUES (?,?,?,?,?,?)'
            e = self.execute_alter(self.config_json['DWH_SERVER'],
                               self.config_json['DWH_USERNAME'],
                               self.config_json['DWH_PASSWORD'],
                               self.config_json['DWH_DATABASE'],
                               query=query, vals=values)

        return e, query

    def add_col_to_database(self):

        table_name = self.dest_table(self.query_json['DWH_QUERY']['CREATE'])
        query = f'ALTER TABLE {table_name} ADD '
        for i, (col,val) in enumerate(zip(self.new_cols,self.fill_values)):
            if 'nvarchar' in self.columns_json[col] or 'date' in self.columns_json[col]:
                query += f'[{col}] {self.columns_json[col]} DEFAULT \'{val.get()}\' WITH VALUES'
            elif ('int' in self.columns_json[col] or 'decimal' in self.columns_json[col] or\
                 'money' in self.columns_json[col] or 'bit' in self.columns_json[col] or 'numeric' in self.columns_json[col]):
                query += f'[{col}] {self.columns_json[col]} DEFAULT {val.get()} WITH VALUES'
            elif val.get().lower=='null':
                query += f'[{col}] {self.columns_json[col]} NULL'
            if i != len(self.new_cols) - 1:
                query += ",\n"
        e = self.execute_alter(self.config_json['DWH_SERVER'],
                               self.config_json['DWH_USERNAME'],
                               self.config_json['DWH_PASSWORD'],
                               self.config_json['DWH_DATABASE'],
                               query=query)

        if not isinstance(e,Exception):
            table_name2 = self.dest_table(self.query_json['JOB_QUERY']['CREATE'])
            values = [f'{self.config_json['JOB_NAME']} - Added COLUMN(S): [{','.join(self.new_cols)}] to {table_name}', 0,0,0,None,datetime.datetime.now()]
            query = f'INSERT INTO {table_name2} VALUES (?,?,?,?,?,?)'
            e = self.execute_alter(self.config_json['DWH_SERVER'],
                               self.config_json['DWH_USERNAME'],
                               self.config_json['DWH_PASSWORD'],
                               self.config_json['DWH_DATABASE'],
                               query=query, vals=values)

        return e, query

    def old_new_rows(self):
        ans = messagebox.askokcancel("Warning",
                                     f"Attention the QUERY.json, CONFIG.json and COLUMNS.JSON files are going to be overwritten!\n Continue to overwrite?",
                                     icon=tk.messagebox.WARNING)
        if ans:
            # self.source_query_new_real = self.source_query_new
            # for repl in [self.source_query_new[self.source_query_new.find(x)-1:self.source_query_new.find(",",self.source_query_new.find(x))+1] for x in self.new_cols]:
            #     self.source_query_new = self.source_query_new.replace(repl,"")
            self.query_json['SOURCE_QUERY']['SELECT'] = self.source_query_new

            done = True

            if len(self.delete_columns) > 0:
                self.remove_column_from_files()

                constraints = self.connect_to_db(server=self.config_json['DWH_SERVER'],
                                            username=self.config_json['DWH_USERNAME'],
                                            password=self.config_json['DWH_PASSWORD'],
                                            database=self.config_json['DWH_DATABASE'],
                                            return_val=True,
                                            job=True,
                                            query="""select con.[name] as constraint_name,
                                                    schema_name(t.schema_id) + '.' + t.[name]  as [table],
                                                    col.[name] as column_name,
                                                    con.[definition]
                                                from sys.default_constraints con
                                                left outer join sys.objects t
                                                on con.parent_object_id = t.object_id
                                                left outer join sys.all_columns col
                                                on con.parent_column_id = col.column_id
                                                and con.parent_object_id = col.object_id
                                                order by con.name""")
                table_name = ".".join([x.replace("[","").replace("]","") for x in self.dest_table(self.query_json['DWH_QUERY']['CREATE']).split(".")[1:]])
                constraints_table = constraints[constraints['table'] == table_name]
                if len(constraints_table) > 0:
                    for i,(constraint_name, column) in constraints[constraints['table'] == table_name][['constraint_name', 'column_name']].iterrows():
                        if column in self.delete_columns:
                            self.execute_alter(self.config_json['DWH_SERVER'],
                                               self.config_json['DWH_USERNAME'],
                                               self.config_json['DWH_PASSWORD'],
                                               self.config_json['DWH_DATABASE'],
                                               query=f"ALTER TABLE {self.dest_table(self.query_json['DWH_QUERY']['CREATE'])} DROP CONSTRAINT {constraint_name}")
                e, del_query = self.remove_col_from_database()
                if isinstance(e, Exception):
                    messagebox.showerror("Error", f"Database error with query {del_query}")
                    done = False

            if done and len(self.new_cols) > 0:
                if self.config_json['MODE'] == 'SCD':
                    if sys.argv[1] == "1":
                        main(["U:\\Python_Files\\db_new.py", self.folder_selected])
                    else:
                        main(["C:\\Python_DWH\\Python_Files\\db_new.py",self.folder_selected])
                #self.query_json['SOURCE_QUERY']['SELECT'] = self.source_query_new_real
                self.add_column_to_files()

                e, add_query = self.add_col_to_database()

                if isinstance(e, Exception):
                    messagebox.showerror("Error", f"Database error with query {add_query}")
                    done = False


            if done:
                ret = 0
                if self.config_json['MODE'] == 'SCD':
                    if sys.argv[1] == "1":
                        ret = main(["U:\\Python_Files\\db_new.py", self.folder_selected])
                    else:
                        ret = main(["C:\\Python_DWH\\Python_Files\\db_new.py", self.folder_selected])
                else:
                    df_job = self.connect_to_db(server=self.config_json['DWH_SERVER'],
                                                username=self.config_json['DWH_USERNAME'],
                                                password=self.config_json['DWH_PASSWORD'],
                                                database=self.config_json['DWH_DATABASE'],
                                                return_val=True,
                                                job=True,
                                                query=f"SELECT * FROM [{self.config_json['DWH_DATABASE']}].[job].[job_table]")
                    if df_job[df_job['name']==self.config_json['JOB_NAME']]['timestamp'].max().date()!=datetime.datetime.now().date():
                        if sys.argv[1] == "1":
                            ret = main(["U:\\Python_Files\\db_new.py", self.folder_selected, False])
                        else:
                            ret = main(["C:\\Python_DWH\\Python_Files\\db_new.py", self.folder_selected, False])
                if ret == 0:
                    messagebox.showinfo("Info", "Files and Database successfully altered")
                    self.window1.quit()
                else:
                    messagebox.showerror("Error", "An Error occurred")



    def compare_dfs(self):

        self.source_query_new = self.new_query_entry.get("1.0", tk.END)
        self.df_new = self.connect_to_db(self.config_json['SOURCE_SERVER'],
                                             self.config_json['SOURCE_USERNAME'],
                                             self.config_json['SOURCE_PASSWORD'],
                                             self.config_json['SOURCE_DATABASE'],
                                             self.source_query_new, True, True)

        self.df_old = self.connect_to_db(self.config_json['SOURCE_SERVER'],
                                         self.config_json['SOURCE_USERNAME'],
                                         self.config_json['SOURCE_PASSWORD'],
                                         self.config_json['SOURCE_DATABASE'],
                                         self.source_query_old, True, True)
        if isinstance(self.df_new, Exception):
            messagebox.showerror("Error", f"Error with new Query: {str(self.df_new.orig)}")
        else:
            df_new_col_set = set(self.df_new.columns)
            df_old_col_set = set(self.df_old.columns)
            self.new_cols = list(df_new_col_set.difference(df_old_col_set))
            self.delete_columns = list(df_old_col_set.difference(df_new_col_set))

            if df_old_col_set == df_new_col_set:
                messagebox.showerror("Error", "Columns of specified queries are the same")
            else:
                self.window3 = tk.Toplevel(self.window2)
                self.window3.geometry("800x800")
                self.window2.withdraw()


                self.alter_button1 = tk.Button(self.window3, text='Alter', command=self.old_new_rows)
                if len(self.new_cols)>0:
                    l = tk.Label(self.window3, text=f'Columns to be Added: [{','.join(self.new_cols)}]')
                    l.place(relx=0.5, rely=0.05, anchor=tk.CENTER)
                    self.vars1 = []
                    self.fill_values = []
                    cols = 10 if len(self.new_cols) > 10 else len(self.new_cols)
                    rows = math.ceil(len(self.new_cols) / cols)
                    options = ["No SCD Type", "SCD Type 1", "SCD Type 2"]
                    i = 0
                    tk.Label(self.window3,text="").grid(row=0, column=0, pady=15)

                    datatypes_query = f"DECLARE @query nvarchar(max) = '{self.source_query_new.replace('\'', '\'\'')}';\nEXEC sp_describe_first_result_set @query, null, 0;"

                    df_datatypes = self.connect_to_db(server=self.config_json['SOURCE_SERVER'],
                                                      username=self.config_json['SOURCE_USERNAME'],
                                                      password=self.config_json['SOURCE_PASSWORD'],
                                                      database=self.config_json['SOURCE_DATABASE'],
                                                      query=datatypes_query, return_val=True,
                                                      job=True)[['name', 'system_type_name']]

                    for r in range(1, (rows+1) * 4, 4):
                        for c in range(cols):
                            clicked = tk.StringVar()
                            fill_val = tk.StringVar()
                            clicked.set(options[2])
                            l = tk.Label(self.window3, text=f'{self.new_cols[i]}')
                            drop = tk.OptionMenu(self.window3, clicked, *options)
                            fill_label = tk.Label(self.window3, text=f'Fill Value (Datatype = {df_datatypes[df_datatypes['name']==self.new_cols[i]]['system_type_name'].iloc[0]})\nFor Datetime use YYYY-MM-DD HH:MM:SS)\nFor Decimal use .')
                            fill = tk.Entry(self.window3,textvariable=fill_val)
                            l.grid(row=r, column=c, padx=15, pady=15)
                            drop.grid(row=r + 1, column=c, padx=15, pady=15)
                            fill_label.grid(row=r + 2, column=c, padx=15, pady=15)
                            fill.grid(row=r + 3, column=c, padx=15, pady=15)
                            self.vars1.append((clicked, self.new_cols[i]))
                            self.fill_values.append(fill_val)
                            i += 1
                            if i == len(self.new_cols):
                                break
                        if i == len(self.new_cols):
                            break
                if len(self.delete_columns) > 0:
                    l = tk.Label(self.window3, text=f'Columns to be Deleted: [{','.join(self.delete_columns)}]')
                    l.place(relx=0.5, rely=0.8, anchor=tk.CENTER)

                self.alter_button1.place(relx=0.5, rely=0.9, anchor=tk.CENTER)



    def switch_to(self, prod):

        r1 = ["APplusTest7", "APplusProd7"] if prod else ["APplusProd7", "APplusTest7"]
        r2 = ["DWH_Test", "DWH"] if prod else ["DWH", "DWH_Test"]
        for query_dict in self.query_json.keys():
            for file in self.query_json[query_dict]:
                content = self.query_json[query_dict][file]
                if ('DWH_Test' in content and prod) or ('DWH_Test' not in content and not prod):
                    self.query_json[query_dict][file] = re.sub(r2[0], r2[1], content)

        self.config_json['DWH_DATABASE'] = r2[1]
        self.config_json['SOURCE_DATABASE'] = r1[1]

        json_object = json.dumps(self.config_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, 'CONFIG.json'), "w", encoding='utf-8') as f:
            f.write(json_object)

        json_object = json.dumps(self.query_json, indent=4, ensure_ascii=False)
        with open(os.path.join(self.folder_selected, "QUERY.json"), "w", encoding='utf-8') as f:
            f.write(json_object)

        messagebox.showinfo("Info",
                            f"Successfully switched from {'Test to Production' if prod else 'Production to Test'}")
        self.window1.quit()

    def alter_job(self, next_function):
        self.window = tk.Toplevel(self.window1)
        self.window1.withdraw()
        self.job_button = tk.Button(self.window, text="Select Job to modify",
                                    command=lambda: self.select_job(next_function))
        self.job_button.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        self.window.geometry("400x200")

    def __init__(self):

        self.window1 = tk.Tk()
        self.window1.title('GUI for Data Warehouse')
        self.create_button = tk.Button(text="Create New Job", command=self.create_job)
        self.alter_button = tk.Button(text='Alter existing Job', command=lambda: self.alter_job(self.compare_window))
        self.prod_button = tk.Button(text='Switch to Production',
                                     command=lambda: self.alter_job(lambda: self.switch_to(True)))
        self.test_button = tk.Button(text='Switch to Test',
                                     command=lambda: self.alter_job(lambda: self.switch_to(False)))

        self.create_button.place(relx=0.33, rely=0.25, anchor=tk.CENTER)
        self.alter_button.place(relx=0.66, rely=0.25, anchor=tk.CENTER)
        self.prod_button.place(relx=0.33, rely=0.75, anchor=tk.CENTER)
        self.test_button.place(relx=0.66, rely=0.75, anchor=tk.CENTER)

        self.window1.geometry("400x400")
        self.window1.mainloop()


if __name__ == "__main__":
    gui = TK_GUI()
