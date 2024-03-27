import datetime
import json
import glob
import os
import tkinter as tk
from tkinter import filedialog
from tkinter import messagebox
import pandas as pd
from dwh_lib import DWH
import sys
import numpy as np
from tkcalendar import Calendar, DateEntry
class GUI:
    # unique_entry = 'Artikel'
    # change_col_excel = ['Preis zum 31.12.23','Gesamtwert am 31.12.23']
    # change_col_dwh = ['InvPreis','Wert']
    # timestamp = datetime.datetime.now()
    # excel = pd.read_excel("U:\\Lagerbewertung_Upload aus Power BI_31-12-23_revHAWE_15.02.2024.xlsx")
    # dwh = DWH(timestamp, sys.argv[1], False)
    # new = excel[[unique_entry]+change_col_excel]
    # new.loc[:,'Artikel'] = new['Artikel'].apply(lambda x: str(x))
    # dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'],
    #                                 username=dwh.config_json['DWH_USERNAME'],
    #                                 password=dwh.config_json['DWH_PASSWORD'],
    #                                 database=dwh.config_json['DWH_DATABASE'])  # connect to database | DWH
    #
    # dwh_select_query, dwh_create_query = dwh.read_query(
    #     query_type='DWH_QUERY')  # select and create query | DWH
    #
    # dwh_select_query = dwh_select_query[:dwh_select_query.index("\n\t")] + "\n\t[surkey]," + dwh_select_query[dwh_select_query.index("\n\t"):]
    #
    # df_dwh = dwh.select_from_db(dwh_select_query,dwh_engine)
    #
    # df_dwh = df_dwh[df_dwh['timestamp'] >= datetime.datetime(year=2023,month=12,day=31, hour=23, minute=59,second=59)]
    # queries = []
    # for i,artikel_nr in new.iterrows():
    #     entry = str(artikel_nr[unique_entry])
    #     change_items_dwh = df_dwh[df_dwh[unique_entry]==entry]
    #     change_items_new = new[new[unique_entry]==entry]
    #     old_val = change_items_dwh[[unique_entry]+change_col_dwh+['surkey']]
    #     new_vals = change_items_new[[unique_entry]+change_col_excel]
    #     merged = pd.merge(new_vals,old_val,on=unique_entry,how='left')
    #     select = f"""SELECT * FROM [DWH].[stock].[stock_value] WHERE {unique_entry}='{artikel_nr[unique_entry]}' and ({" or ".join([f'[surkey]={x}' for x in merged['surkey']])}) and [Mandant]='CCE OESTERREICH'"""
    #     update = f"""UPDATE [DWH].[stock].[stock_value] SET {", ".join([f'[{y}]={x}' for x,y in zip(merged[change_col_excel].iloc[0],change_col_dwh)])} WHERE {unique_entry}='{artikel_nr[unique_entry]}' and ({" or ".join([f'[surkey]={x}' for x in merged['surkey']])}) and [Mandant]='CCE OESTERREICH'"""
    #     queries.append((select,update))

    def search_file(self):
        search = sorted(glob.glob(os.path.join(self.dwh_folder, "*.json")))
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

        if not hasattr(self, "query_json"):
            messagebox.showerror("Error", "No query_json file found")
        else:
            self.populate_window()

    def populate_window(self):
        self.window1 = tk.Toplevel(self.window)
        self.window1.protocol("WM_DELETE_WINDOW", self.on_closing)
        self.window.withdraw()
        try:
            timestamp = datetime.datetime.now()
            self.df_excel = pd.read_excel(self.excel_path.name,header=0, engine='openpyxl')
            #self.df_excel = self.df_excel.apply(pd.to_datetime, errors='ignore')
            self.dwh = DWH(timestamp, self.dwh_folder, False if self.config_json['MODE'] == 'INSERT' else True)

            self.dwh_engine = self.dwh.connect_to_db(server=self.dwh.config_json['DWH_SERVER'],
                                            username=self.dwh.config_json['DWH_USERNAME'],
                                            password=self.dwh.config_json['DWH_PASSWORD'],
                                            database=self.dwh.config_json['DWH_DATABASE'])  # connect to database | DWH

            dwh_select_query, dwh_create_query = self.dwh.read_query(
                query_type='DWH_QUERY')  # select and create query | DWH

            dwh_select_query = dwh_select_query[:dwh_select_query.index("\n\t")] + "\n\t[surkey]," + dwh_select_query[dwh_select_query.index("\n\t"):]
            if self.config_json['MODE'] == 'INSERT':
                self.df_dwh = self.dwh.select_from_db(dwh_select_query,self.dwh_engine)
            else:
                self.df_dwh = self.dwh.select_from_db(dwh_select_query.replace("WHERE [valid_to] IS NULL",""), self.dwh_engine)

            options = list(range(1,20))
            self.str_var = tk.StringVar()
            dropdown = tk.OptionMenu(self.window1, self.str_var, *options, command=lambda x: self.insert_cols())
            dropdown.widgetName = "w1"
            dropdown.place(rely=0.1,relx=0.5, anchor=tk.CENTER)
            label = tk.Label(self.window1, text="Select Number of Columns to update")
            label.widgetName = "w2"
            label.place(rely=0.05,relx=0.5, anchor=tk.CENTER)


        except Exception as e:
            messagebox.showerror("Error",str(e))

        self.window1.geometry("1500x800")

    def insert_cols(self):

        [x.destroy() for x in self.window1.winfo_children() if x.widgetName not in ["w1","w2"]]

        self.vars = []
        self.window1.rowconfigure(0, weight=1)
        for i in range(int(self.str_var.get())):
            self.window1.rowconfigure(i, weight=1)
            self.window1.columnconfigure(i, weight=1)
            str_var_n = tk.StringVar()
            str_var_i = tk.StringVar()
            options = self.df_dwh.columns
            dropdown = tk.OptionMenu(self.window1, str_var_n, *options)
            dropdown.grid(row=1, column=i, padx=5, pady=15)
            dropdown.widgetName="z1"
            label = tk.Label(self.window1, text="Replace with")
            label.widgetName = "z1"
            label.grid(row=2, column=i, padx=5, pady=15)
            options = self.df_excel.columns
            dropdown = tk.OptionMenu(self.window1, str_var_i, *options)
            dropdown.widgetName = "z1"
            dropdown.grid(row=3, column=i, padx=5, pady=15)
            self.vars.append((str_var_n,str_var_i))

        button = tk.Button(self.window1,text="Continue",command=self.exec)
        button.widgetName = "z1"
        self.window1.rowconfigure(4, weight=1)
        column_nr = (int(self.str_var.get())-1)// 2
        button.grid(row=9, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) %2!=0 else 2)
        self.cal = Calendar(self.window1,font="Arial 14", selectmode='day',cursor="hand1")
        self.cal.grid(row=5, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) % 2 != 0 else 2)
        label = tk.Label(self.window1, text="Only choose entries after this date: ")
        label.grid(row=4, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) % 2 != 0 else 2)

        self.str_var_radio = tk.StringVar(value="0")

        self.r1 = tk.Radiobutton(self.window1, text="Update All DWH Entries with the same values",value="0",variable=self.str_var_radio)
        self.r1.select()
        self.r1.grid(row=6, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) % 2 != 0 else 2)
        self.r2 = tk.Radiobutton(self.window1, text="Update each Entry with the specific timestamp",value="1",variable=self.str_var_radio)
        self.r2.deselect()
        self.r2.grid(row=7, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) % 2 != 0 else 2)

        self.window1.geometry("1800x800")
    def exec(self):
        converted_date = datetime.datetime.strptime(self.cal.get_date(),"%m/%d/%y")
        if not converted_date.date()==datetime.datetime.today().date() and self.config_json['MODE'] == 'INSERT':
            self.df_dwh = self.df_dwh[self.df_dwh['timestamp'] >= converted_date]

        unique = self.config_json['DWH_UNIQUE_ENTRIES'] if self.str_var_radio.get() == "0" else [self.config_json['DWH_UNIQUE_ENTRIES']] + ['timestamp']
        dwh_select_query, dwh_create_query = self.dwh.read_query(query_type='DWH_QUERY')  # select and create query | DWH
        insert_table = self.dwh.dest_table(select_query=dwh_create_query)
        if self.config_json['MODE'] == 'INSERT':
            if self.str_var_radio.get() == "0":
                for entry in self.df_excel[unique]:
                    entry = str(entry)
                    change_items_dwh = self.df_dwh[self.df_dwh[unique]==entry]
                    change_items_new = self.df_excel[self.df_excel[unique]==entry]
                    dwh_items = [x.get() for x,y in self.vars]
                    new_items = [y.get() for x, y in self.vars]

                    old_val = change_items_dwh[dwh_items+[unique]+['surkey']+['timestamp']]
                    new_val = change_items_new[[unique]+new_items]
                    merged = pd.merge(old_val,new_val,on=unique,how='left')
                    for i,row in merged.iterrows():
                        select_cond = " and ".join([f"[{x.get()}]='{merged[merged['surkey'] == row['surkey']][x.get()].iloc[0]}'"
                                                    if 'nvarchar' in self.columns_json[dwh_items[j]]
                                                    or 'date' in self.columns_json[dwh_items[j]]
                                                    else f"[{x.get()}]={merged[merged['surkey'] == row['surkey']][x.get()].iloc[0]}"
                                                    for j,(x,y) in enumerate(self.vars)])

                        select = f"""SELECT * FROM {insert_table} WHERE [surkey] = {row['surkey']} and {select_cond}"""
                        update_cond = ",".join([f"[{x.get()}]='{merged[merged['surkey'] == row['surkey']][y.get()].iloc[0]}'"
                                                if 'nvarchar' in self.columns_json[dwh_items[j]]
                                                   or 'date' in self.columns_json[dwh_items[j]]
                                                else f"[{x.get()}]={merged[merged['surkey'] == row['surkey']][y.get()].iloc[0]}"
                                                for j,(x,y) in enumerate(self.vars)])

                        update =f"""UPDATE {insert_table} SET {update_cond} WHERE [surkey] = {row['surkey']} and {select_cond}"""
                        self.queries.append((select,update))

            elif self.str_var_radio.get() == "1":
                for _,(entry,timestamp) in self.df_excel[unique].iterrows():
                    entry = str(entry)
                    timestamp = timestamp.date()

                    self.df_dwh['timestamp'] = self.df_dwh['timestamp'].apply(lambda x: x.date() if type(x) != datetime.date else x)
                    self.df_excel['timestamp'] = self.df_excel['timestamp'].apply(lambda x: x.date() if type(x) != datetime.date else x)

                    change_items_dwh = self.df_dwh[(self.df_dwh[unique[0]] == entry) & (self.df_dwh['timestamp'] == timestamp)]
                    change_items_new = self.df_excel[(self.df_excel[unique[0]] == entry) & (self.df_excel['timestamp'] == timestamp)]
                    dwh_items = [x.get() for x, y in self.vars]
                    new_items = [y.get() for x, y in self.vars]

                    old_val = change_items_dwh[dwh_items + [unique[0]] + ['surkey'] + ['timestamp']]
                    new_val = change_items_new[[unique[0]] + new_items + ['timestamp']]

                    merged = pd.merge(old_val, new_val, on=unique, how='left')

                    select_cond = " and ".join([f"[{x.get()}]='{merged.iloc[0][x.get()]}'"
                                                if 'nvarchar' in self.columns_json[dwh_items[j]]
                                                else f"[{x.get()}]='{merged.iloc[0][x.get()]}'"
                                                if 'date' in self.columns_json[dwh_items[j]]
                                                else f"[{x.get()}]={merged.iloc[0][x.get()]}"
                                                for j,(x,y) in enumerate(self.vars)])

                    select = f"""SELECT * FROM {insert_table} WHERE [surkey] = {merged.iloc[0]['surkey']} and {select_cond}"""
                    update_cond = ",".join([f"[{x.get()}]='{merged.iloc[0][y.get()]}'"
                                            if 'nvarchar' in self.columns_json[dwh_items[j]]
                                            else f"[{x.get()}]='{merged.iloc[0][y.get()].date()}'"
                                            if 'date' in self.columns_json[dwh_items[j]]
                                            else f"[{x.get()}]={merged.iloc[0][y.get()]}"
                                            for j,(x,y) in enumerate(self.vars)])

                    update = f"""UPDATE {insert_table} SET {update_cond} WHERE [surkey] = {merged.iloc[0]['surkey']} and {select_cond}"""
                    self.queries.append((select, update))

        else:
            self.df_dwh['valid_to'] = self.df_dwh['valid_to'].replace({pd.NaT: datetime.datetime(year=2099,month=1,day=1)})
            change_select_query, change_create_query = self.dwh.read_query(query_type='CHANGE_QUERY')
            self.df_dwh_change = self.dwh.select_from_db(change_select_query,engine=self.dwh_engine)
            for entry in self.df_excel[unique]:
                entry = str(entry)
                change_items_dwh = self.df_dwh[(self.df_dwh[unique]==entry) & (self.df_dwh['valid_to']>=converted_date)]
                change_items_new = self.df_excel[self.df_excel[unique] == entry]
                dwh_items = [x.get() for x, y in self.vars]
                new_items = [y.get() for x, y in self.vars]

                old_val = change_items_dwh[dwh_items + [unique] + ['surkey'] + ['timestamp']]
                new_val = change_items_new[[unique] + new_items]

                same_names = [x for x in new_items if x in dwh_items]
                if len(same_names) > 0:
                    for same in same_names:
                        new_val.rename(columns={f'{same}': f'{same}_new'}, inplace=True)
                        new_items.remove(same)
                        new_items.append(f'{same}_new')
                        var = [y for x, y in self.vars if y.get()==same][0]
                        var.set(f'{same}_new')
                merged = pd.merge(old_val, new_val, on=unique, how='left')



                for i, row in merged.iterrows():
                    select_cond = " and ".join(
                        [f"[{x.get()}]='{merged[merged['surkey'] == row['surkey']][x.get()].iloc[0]}'"
                         if 'nvarchar' in self.columns_json[dwh_items[j]]
                         else f"[{x.get()}]='{merged[merged['surkey'] == row['surkey']][x.get()].iloc[0]}'"
                         if 'date' in self.columns_json[dwh_items[j]]
                         else f"[{x.get()}]={merged[merged['surkey'] == row['surkey']][x.get()].iloc[0]}"
                         for j, (x, y) in enumerate(self.vars)])

                    select = f"""SELECT * FROM {insert_table} WHERE [surkey] = {row['surkey']} and {select_cond}"""
                    update_cond = ",".join(
                        [f"[{x.get()}]='{merged[merged['surkey'] == row['surkey']][y.get()].iloc[0]}'"
                         if 'nvarchar' in self.columns_json[dwh_items[j]]
                         else f"[{x.get()}]='{merged[merged['surkey'] == row['surkey']][y.get()].iloc[0].date()}'"
                         if 'date' in self.columns_json[dwh_items[j]]
                         else f"[{x.get()}]={merged[merged['surkey'] == row['surkey']][y.get()].iloc[0]}"
                         for j, (x, y) in enumerate(self.vars)])

                    update = f"""UPDATE {insert_table} SET {update_cond} WHERE [surkey] = {row['surkey']} and {select_cond}"""
                    self.queries.append((select, update))

                ############################### CHANGES ################################################################

                change_items = self.df_dwh_change[(self.df_dwh_change[unique]==entry)& (self.df_dwh_change['timestamp']>=converted_date)]
                insert_table_2 = self.dwh.dest_table(select_query=change_select_query)
                for j,(x,y) in enumerate(self.vars):
                    old_val2 = change_items[change_items['CHANGED_COLUMN']==f'{x.get()}']
                    new_value = f"'{change_items_new[x.get()].iloc[0]}'" if 'nvarchar' in self.columns_json[dwh_items[j]] else f"'{change_items_new[x.get()].iloc[0].date()}'" if 'date' in self.columns_json[dwh_items[j]] else f"{change_items_new[x.get()].iloc[0]}"
                    update = f"""UPDATE {insert_table_2} SET [NEW_VALUE]={new_value} WHERE [{unique}]='{old_val2[unique].iloc[0]}' and [timestamp]='{old_val2['timestamp'].iloc[0].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}' and [CHANGED_COLUMN] = '{x.get()}'"""
                    self.queries.append((f"""SELECT * FROM {insert_table_2} WHERE [{unique}]='{old_val2[unique].iloc[0]}' and [timestamp]='{old_val2['timestamp'].iloc[0].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}' and [CHANGED_COLUMN] = '{x.get()}'""", update))



        [x.destroy() for x in self.window1.winfo_children()]
        t1 = tk.Text(self.window1, height=20, width=80)
        t1.insert(tk.END, "\n".join([x[0] for x in self.queries]))
        t1.place(relx=0.25,rely=0.5,anchor=tk.CENTER)
        t2 = tk.Text(self.window1, height=20, width=80)
        t2.insert(tk.END, "\n".join([x[1] for x in self.queries]))
        t2.place(relx=0.75,rely=0.5,anchor=tk.CENTER)

    def on_closing(self):
        self.window1.destroy()
        self.window.destroy()

    def ask_directory_or_file(self,mode):
        if mode==1:
            self.dwh_folder = filedialog.askdirectory(initialdir="U:\\Json_Files\\RTF_old")
            if self.dwh_folder:
                self.select_dwh_label.config(text=f"Selected: {self.dwh_folder}")
        elif mode==2:
            self.excel_path = filedialog.askopenfile(initialdir="U:\\",initialfile="F1.xlsx")
            if self.excel_path:
                self.select_excel_label.config(text=f"Selected: {self.excel_path.name}")

        if self.excel_path is not None and self.dwh_folder is not None:
            select_dwh_button = tk.Button(self.window, text="Continue",
                                          command=lambda: self.search_file())
            select_dwh_button.place(relx=0.5, rely=0.85, anchor=tk.CENTER)

    def __init__(self):
        self.window1,self.query_json,self.config_json,self.columns_json,self.excel_path = None,None,None,None,None
        self.dwh_folder = None
        self.queries = []
        self.window = tk.Tk()
        self.window.title('Query Selection')
        select_dwh_button = tk.Button(self.window, text="Select Folder of DWH Table",
                                      command=lambda: self.ask_directory_or_file(1))
        select_dwh_button.place(relx=0.25, rely=0.5, anchor=tk.CENTER)

        self.select_dwh_label = tk.Label(self.window, text="Selected: ")
        self.select_dwh_label.place(relx=0.25, rely=0.75, anchor=tk.CENTER)

        self.select_excel_label = tk.Label(self.window, text="Selected: ")
        self.select_excel_label.place(relx=0.75, rely=0.75, anchor=tk.CENTER)

        select_excel_button = tk.Button(self.window, text="Select Excel", command=lambda:self.ask_directory_or_file(2))
        select_excel_button.place(relx=0.75, rely=0.5, anchor=tk.CENTER)

        self.window.geometry("1000x200")
        self.window.mainloop()
    #

if __name__ == "__main__":
    gui = GUI()
    # unique_entry = 'Artikel'
    # change_col_excel = ['Preis zum 31.12.23','Gesamtwert am 31.12.23']
    # change_col_dwh = ['Wert']
    # timestamp = datetime.datetime.now()
    # excel = pd.read_excel("U:\\Lagerbewertung.xlsx")
    # dwh = DWH(timestamp, sys.argv[1], False)
    # new = excel[[unique_entry]+change_col_excel]
    # new.loc[:,'Artikel'] = new['Artikel'].apply(lambda x: str(x))
    # dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'],
    #                                 username=dwh.config_json['DWH_USERNAME'],
    #                                 password=dwh.config_json['DWH_PASSWORD'],
    #                                 database=dwh.config_json['DWH_DATABASE'])  # connect to database | DWH
    #
    # dwh_select_query, dwh_create_query = dwh.read_query(
    #     query_type='DWH_QUERY')  # select and create query | DWH
    #
    # dwh_select_query = dwh_select_query[:dwh_select_query.index("\n\t")] + "\n\t[surkey]," + dwh_select_query[dwh_select_query.index("\n\t"):]
    #
    # df_dwh = dwh.select_from_db(dwh_select_query,dwh_engine)
    #
    # df_dwh = df_dwh[df_dwh['timestamp'] >= datetime.datetime(year=2023,month=12,day=31, hour=23, minute=59,second=59)]
    # # df_dwh['Test'] = df_dwh['Menge'].astype(float) * df_dwh['InvPreis'].astype(float)
    # # df_dwh['Test2'] =  df_dwh['Test']-df_dwh['Wert']
    #
    # artikel = ['101241','101245','101246','101249','101256','101257','101315']
    # inv_preis = [76.32,5.49,5.49,5.49,5.49,0.02,0.08,1.96]
    #
    # queries = []
    #
    #
    #
    # for artikel_nr,value in zip(artikel,inv_preis):
    #     entry = str(artikel_nr)
    #     change_items_dwh = df_dwh[df_dwh[unique_entry]==entry]
    #     #change_items_new = new[new[unique_entry]==entry]
    #     old_val = change_items_dwh[[unique_entry]+change_col_dwh+['surkey']+['timestamp']+['Menge']]
    #     #new_vals = change_items_new[[unique_entry]+change_col_excel]
    #     #merged = pd.merge(new_vals,old_val,on=unique_entry,how='left')
    #     for i,x in old_val.iterrows():
    #         select = f"""SELECT * FROM [DWH].[stock].[stock_value] WHERE [surkey] = {x['surkey']} and [timestamp] = '{x['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""
    #         update =f"""UPDATE [DWH].[stock].[stock_value] SET [Wert] = {round(value*x['Menge'],2)},[InvPreis]={value} WHERE [surkey] = {x['surkey']} and [timestamp] = '{x['timestamp'].strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}'"""
    #         queries.append((select,update))
    #
    # print(queries)