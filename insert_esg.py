import time

import pandas as pd
import numpy as np
import sys

import sqlalchemy

from dwh_lib import DWH
from sqlalchemy import URL, create_engine
import tkinter as tk
from glob import glob
from tkinter import filedialog
from tkinter import messagebox
import datetime
from tkinter import font
import os
from PIL import Image, ImageTk
class TK_GUI():

    def update_records(self, destination_table: str, update_entries: list, columns: list, primary_key: list,
                       engine: sqlalchemy.Engine) -> None:
        """
           Update specified records in a database table.
           follows sql: UPDATE [DB] SET [C1] = ?, [C2] = ?, ... WHERE [UNIQUE_ROWS] = UNIQUE_VALUE

        Parameters:
                - destination_table (str): The name of the table to update.
                - update_entries (list): A list of dictionaries representing the rows to update.
                - columns (list): List of columns that need updating.
                - primary_key (list): Columns that uniquely identify rows to update.
                - engine (sqlalchemy.Engine): Database connection engine.

        Raises:
                - Exception: When updating failed
        """
        conn = engine.raw_connection()

        columns = [col for col in columns if col not in primary_key+['ANP_ESG_POS']]
        cursor = conn.cursor()
        try:
            for row in update_entries:
                where = row[primary_key]

                row.drop(['surkey'] + primary_key+['ANP_ESG_POS'], inplace=True, errors='ignore')

                header = f"UPDATE {destination_table} SET"
                columns_update_sql = f"{", ".join([f'[{x}] = ?' for x in columns])} WHERE "
                condition = [f"[{x}]=\'{y}\'" if x != 'valid_from'
                else f"[{x}]=\'{y.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]}\'"
                                     for x, y in zip(where.index.tolist(), where.tolist())]
                where_clause = " AND ".join(condition)

                sql_query = header + columns_update_sql + where_clause
                cursor.executemany(sql_query, self.dwh.to_python_vals([row.values.tolist()]))
            conn.commit()

        except Exception as e:
            conn.rollback()
            raise Exception(f"Could not update Entries: {str(e)}")

        finally:
            cursor.close()
            conn.close()


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
            pass

    def select_file(self):
        self.excel_path = filedialog.askopenfilename()

        self.df = pd.read_excel(self.excel_path, header=13)
        self.df.drop(columns='Unnamed: 0', inplace=True)
        self.df.columns = ['Date'] + list(self.df.columns[1:])
        total_index = self.df[self.df['Date'] == 'Total'].index[0]
        self.df.drop(index=self.df.iloc[total_index - 1:].index, inplace=True)
        self.df.reset_index(inplace=True, drop=True)


        self.df_dropdown = self.dwh.select_from_db(select_query=f"""SELECT 
[Firma] = ADRESSE.FIRMA,
[FirmaName] = ADRESSE.FIRMA1,
[Adresse] = ADRESSE.ADRESSE,
[Mandant] = FIRMA_A.ANP_MANDANTID,
[ESG_Type] = ADRESSE.ANP_ESG_TYPE
FROM [APplus{self.system}].[dbo].ADRESSE 
LEFT JOIN FIRMA_A on ADRESSE.FIRMA=FIRMA_A.FIRMA
WHERE ADRESSE.ANP_ESG=1""",
                                       engine=self.source_engine)


        self.anp_pos_nr = self.dwh.select_from_db(select_query=f"""SELECT 
  THIS,
  LAST
  FROM [APplus{self.system}].[dbo].[NUMMER] WHERE TABELLE='ANP_ESG_POS' and GUELTIGAB >= '2024-01-01'""",
                                       engine=self.source_engine)

        [x.destroy() for x in self.window1.winfo_children() if x not in self.widget_list]

        self.widget_list = []

        def continue_button(string_var):
            b2 = tk.Button(self.window1, text="Continue", command=verifiy_user)
            b2.place(relx=0.5, rely=0.9, anchor=tk.CENTER)
            self.user_var = tk.StringVar()

            l1 = tk.Label(self.window1, text=f'Enter User Name:')
            l1.place(relx=0.5, rely=0.7, anchor=tk.CENTER)
            e1 = tk.Entry(self.window1, textvariable=self.user_var)
            e1.place(relx=0.5, rely=0.8, anchor=tk.CENTER)

        def verifiy_user():
            df_user = self.dwh.select_from_db(select_query=f"""SELECT V_PERSONAL.Personal,
            V_PERSONAL.Name,P.LOGIN FROM [APplus{self.system}].[dbo].V_PERSONAL
            left join PERSONAL P on P.PERSONAL = V_PERSONAL.PERSONAL""",engine=self.source_engine)
            if len(df_user[df_user['LOGIN']==self.user_var.get()]) > 0:
                self.user_nr = df_user[df_user['LOGIN']==self.user_var.get()]['Personal'].iloc[0]
                self.process_data()
            else:
                messagebox.showerror("Error", f"User: {self.user_var.get()} not found in Database")


        def chose_type(string_var):
            [x.destroy() for x in self.widget_list]
            self.esg_type = tk.StringVar()
            vals2 = self.df_dropdown[self.df_dropdown['FirmaName']==self.company.get()]['ESG_Type'].unique().tolist()
            self.option_menu2 = tk.OptionMenu(self.window1, self.esg_type, *vals2, command=continue_button)
            self.option_menu2.place(relx=0.75, rely=0.6, anchor=tk.CENTER)
            l = tk.Label(self.window1, text=f'Select ESG-Type:')
            l.place(relx=0.75, rely=0.5, anchor=tk.CENTER)
            self.widget_list.append(self.option_menu2)



        vals = self.df_dropdown['FirmaName'].unique().tolist()
        self.company = tk.StringVar()
        option_menu = tk.OptionMenu(self.window1,self.company,*vals,  command=chose_type)
        option_menu.place(relx=0.25, rely=0.6, anchor=tk.CENTER)
        l = tk.Label(self.window1, text=f'Select Object:')
        l.place(relx=0.25, rely=0.5, anchor=tk.CENTER)





    def process_data(self):

        self.insert = []
        filtered = self.df_dropdown[(self.df_dropdown['FirmaName']==self.company.get()) &(self.df_dropdown['ESG_Type']==self.esg_type.get())]
        mandant = filtered['Mandant'].iloc[0]
        adresse = filtered['Adresse'].iloc[0]
        country = filtered['Firma'].iloc[0]
        self.anp_pos_nr = int(self.anp_pos_nr['THIS'].iloc[0].split("ANN")[1])+1
        for i, x in self.df.iterrows():
            for j, anp_data in x.items():
                cols = [mandant, self.user_nr, f'{datetime.datetime.strftime(self.now, "%Y-%m-%d %H:%M:%S")}', self.user_nr,
                        f'{datetime.datetime.strftime(self.now, "%Y-%m-%d %H:%M:%S")}']
                if j == 'Date':
                    year = str(anp_data.year)
                    month = str(anp_data.month).zfill(2)

                elif j != 'Date' and '(' in j:
                    if not np.isnan(anp_data):
                        cols.extend([j.split("(")[1][:-1], anp_data, None, None, None,
                                     j.split("(")[0].strip(),
                                     None, None, None, None,
                                     country,
                                     None, "Complementary Info  0&M Report", None, 'ANN',
                                     None,
                                     None, adresse, "O&M_Report", None,
                                     year, month])
                        self.insert.append(cols)


        self.insert_in_dwh()

    def insert_in_dwh(self):

        df_source = self.dwh.select_from_db(select_query=f"""SELECT * FROM [APplus{self.system}].[dbo].[ANP_ESG_POS]""",
                                       engine=self.source_engine)

        df_source = df_source.drop(columns=['id', 'timestamp'])
        new_rows = pd.DataFrame(self.insert, columns=df_source.columns)
        compare=['ANP_DATA']
        pks = ['ANP_FUEL_TYPE','ANP_YEAR','ANP_MONTH','ANP_OBJECT']

        new_rows['source']='excel'
        df_source['source'] ='source'

        upt_rows = (pd.concat([new_rows, df_source])
               .drop_duplicates(keep=False, subset=compare+pks))

        upt_rows = upt_rows[(upt_rows.duplicated(keep=False, subset=pks))&(upt_rows['source']=='excel')].drop(columns='source')

        new_rows = pd.concat([new_rows, df_source]).drop_duplicates(subset=pks,keep=False).drop(columns='source')

        new_rows['ANP_ESG_POS'] = new_rows['ANP_ESG_POS']+ [str(self.anp_pos_nr+i) for i in range(len(new_rows))]


        if len(upt_rows) > 0:
            msg = messagebox.askyesno("Attention",f"Do you want to update {len(upt_rows)} entries? (New Entries are inserted anyways)")
            if msg:
                try:
                    self.update_records(destination_table=f"[APplus{self.system}].[dbo].[ANP_ESG_POS]"
                                            ,update_entries=[x for i,x in upt_rows.iterrows()],columns=upt_rows.columns,primary_key=pks,
                                            engine=self.source_engine)
                    messagebox.showinfo("Success", f"Updated {len(upt_rows)} Row in Database")
                except Exception as e:
                    messagebox.showerror("Error", f"Failure when updating:  {str(e)}")

        if len(new_rows) > 0:
            self.dwh_engine = self.dwh.connect_to_db(server=self.dwh.config_json['DWH_SERVER'],
                                                        username=self.dwh.config_json['DWH_USERNAME'],
                                                        password=self.dwh.config_json['DWH_PASSWORD'],
                                                        database=self.dwh.config_json['DWH_DATABASE'])
            try:
                self.dwh.insert_into_db(destination_table=f"[APplus{self.system}].[dbo].[ANP_ESG_POS]",
                                   df_insert=new_rows, engine=self.source_engine, add_time_cols=False)
                self.anp_pos_nr += len(new_rows)
                self.execute_alter(server=self.dwh.config_json['SOURCE_SERVER'],
                                   username=self.dwh.config_json['SOURCE_USERNAME'],
                                   password=self.dwh.config_json['SOURCE_PASSWORD'],
                                   database=self.dwh.config_json['SOURCE_DATABASE'],
                                   query=f"UPDATE [APplus{self.system}].[dbo].[NUMMER] SET THIS='ANN{str(self.anp_pos_nr-1)}' WHERE TABELLE='ANP_ESG_POS' and GUELTIGAB >= '2024-01-01'")

                msg_box = messagebox.showinfo("Success",
                                              f"Inserted {len(new_rows)} Rows in Database")
            except Exception as e:
                msg_box = messagebox.showerror("Error",
                                              f"The following Error occured: {str(e)}")


            self.window1.destroy()

        else:
            messagebox.showinfo("Info", "No new Entries found!")


    def __init__(self,system):
        self.now = datetime.datetime.now()
        self.system = system
        base_path = os.path.dirname(os.path.abspath(__file__))
        esg_param = os.path.join(base_path, 'ESG')
        self.dwh = DWH(self.now, esg_param, False)
        #self.dwh = DWH(self.now, sys.argv[2], False)

        self.window1 = tk.Tk()
        self.window1.geometry("600x600")
        self.photo_bg =tk.PhotoImage(file = esg_param+"\\CCE.png")
        #self.photo_bg = tk.PhotoImage(file=sys.argv[2] + "\\CCE.png")
        self.background_label = tk.Label(self.window1, image=self.photo_bg)
        self.background_label.place(relx=0.5,rely=0.25, anchor=tk.CENTER)

        self.window1.title('0&M Report Importer')
        self.create_button = tk.Button(text="Select Excel File", command=self.select_file)
        self.create_button.place(relx=0.5, rely=0.5, anchor=tk.CENTER)
        self.widget_list = []
        self.l1 = tk.Label(self.window1,text="Created by Alexander Huber\na.huber@cce-holding.com",font=font.Font(size=8))
        self.l1.place(relx=0.5,rely=0.9, anchor=tk.CENTER)
        self.widget_list.append(self.background_label)




        self.source_engine = self.dwh.connect_to_db(server=self.dwh.config_json['SOURCE_SERVER'],
                                          username=self.dwh.config_json['SOURCE_USERNAME'],
                                          password=self.dwh.config_json['SOURCE_PASSWORD'],
                                          database=self.dwh.config_json['SOURCE_DATABASE'])


        self.window1.mainloop()


if __name__ == "__main__":

    gui = TK_GUI("Test7")
