import pandas as pd
import numpy as np
import sys
from dwh_lib import DWH
import datetime

excel_path, esg_param = sys.argv[1:]
insert = []
df = pd.read_excel(excel_path, header=13)
df.drop(columns='Unnamed: 0',inplace=True)
df.columns = ['Date'] + list(df.columns[1:])
total_index = df[df['Date'] == 'Total'].index[0]
df.drop(index=df.iloc[total_index-1:].index, inplace=True)
df.reset_index(inplace=True,drop=True)
now = datetime.datetime.now()

for i,x in df.iterrows():
    for j,anp_data in x.items():
        cols = ['Mandant', '78', f'{datetime.datetime.strftime(now,"%Y-%m-%d %H:%M:%S")}', '78', f'{datetime.datetime.strftime(now,"%Y-%m-%d %H:%M:%S")}']
        if j == 'Date':
            year = str(anp_data.year)
            month = str(anp_data.month).zfill(2)

        elif j!='Date' and '(' in j:
            if not np.isnan(anp_data):
                cols.extend([j.split("(")[1][:-1],anp_data,"ANP_SCOPE","ANP_TYPE","ANP_SUBTYPE",j.split("(")[0].strip(),
                             "ANP_TRANSACTION","ANP_BEZEICHNUNG","ANP_COMMENT","ANP_ASSET", "ANP_COUNTRY_UNIT",
                             "ANP_TARGET", "Complementary Info  0&M Report","ANP_GROUP", 'ANNTODO', "ANP_FUTURE_TARGET",
                             "ANP_FUTURE_YEAR_TARGET_RELEVANT", "ANP_OBJECT", "O&M_Report", "ANP_CLARIFICATION", year, month])
                insert.append(cols)



dwh = DWH(now,esg_param,False)
source_engine = dwh.connect_to_db(server=dwh.config_json['SOURCE_SERVER'],
                                username=dwh.config_json['SOURCE_USERNAME'],
                                password=dwh.config_json['SOURCE_PASSWORD'],
                                database=dwh.config_json['SOURCE_DATABASE'])

df_source = dwh.select_from_db(select_query="""SELECT * FROM [APplusProd7].[dbo].[ANP_ESG_POS]""", engine=source_engine)
df_columns = df_source.columns.drop(labels=['id','timestamp'])
new_rows = pd.DataFrame(insert, columns=df_columns)

dwh.insert_into_db(destination_table="[APplusProd7].[dbo].[ANP_ESG_POS]",
                   df_insert=new_rows, engine=source_engine,add_time_cols=False)
