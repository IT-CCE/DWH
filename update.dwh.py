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
            dwh = DWH(timestamp, sys.argv[1], False)

            dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'],
                                            username=dwh.config_json['DWH_USERNAME'],
                                            password=dwh.config_json['DWH_PASSWORD'],
                                            database=dwh.config_json['DWH_DATABASE'])  # connect to database | DWH

            dwh_select_query, dwh_create_query = dwh.read_query(
                query_type='DWH_QUERY')  # select and create query | DWH

            dwh_select_query = dwh_select_query[:dwh_select_query.index("\n\t")] + "\n\t[surkey]," + dwh_select_query[dwh_select_query.index("\n\t"):]

            self.df_dwh = dwh.select_from_db(dwh_select_query,dwh_engine)

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
        button.grid(row=6, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) %2!=0 else 2)
        self.cal = Calendar(self.window1,font="Arial 14", selectmode='day',cursor="hand1")
        self.cal.grid(row=5, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) % 2 != 0 else 2)
        label = tk.Label(self.window1, text="Only choose entries after this date: ")
        label.grid(row=4, column=column_nr, padx=5, pady=5, columnspan=1 if int(self.str_var.get()) % 2 != 0 else 2)
        self.window1.geometry("1500x800")
    def exec(self):
        converted_date = datetime.datetime.strptime(self.cal.get_date(),"%m/%d/%y")
        if not converted_date.date()==datetime.datetime.today().date():
            self.df_dwh = self.df_dwh[self.df_dwh['timestamp'] >= converted_date]


        #pd.merge(new_vals, old_val, on=unique_entry, how='left')

        for x,y in self.vars:
            print(x,y)
    def on_closing(self):
        self.window1.destroy()
        self.window.destroy()

    def ask_directory_or_file(self,mode):
        if mode==1:
            self.dwh_folder = filedialog.askdirectory(initialdir="U:\\DB_old\\Fact\\Stock")
            if self.dwh_folder:
                self.select_dwh_label.config(text=f"Selected: {self.dwh_folder}")
        elif mode==2:
            self.excel_path = filedialog.askopenfile(initialdir="U:\\",initialfile="Lagerbewertung.xlsx")
            if self.excel_path:
                self.select_excel_label.config(text=f"Selected: {self.excel_path.name}")

        if self.excel_path is not None and self.dwh_folder is not None:
            select_dwh_button = tk.Button(self.window, text="Continue",
                                          command=lambda: self.search_file())
            select_dwh_button.place(relx=0.5, rely=0.85, anchor=tk.CENTER)

    def __init__(self):
        self.window1,self.query_json,self.config_json,self.columns_json,self.excel_path = None,None,None,None,None
        self.dwh_folder = None
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