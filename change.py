import sys
from dwh_lib import DWH
import datetime
import pandas as pd
import numpy as np
from tqdm import tqdm
deletes = []
updates = []

timestamp = datetime.datetime.now()
dwh = DWH(timestamp, "U:\\Json_Files\\Project_All", True)
dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'],
                                username=dwh.config_json['DWH_USERNAME'],
                                password=dwh.config_json['DWH_PASSWORD'],
                                database=dwh.config_json['DWH_DATABASE'])

project_pos = dwh.select_from_db("""SELECT DISTINCT Project_Name_Pos  FROM [DWH].masterdata.project_all""", dwh_engine)
scd_t2_columns = [x for x in dwh.config_json['SCD2'].split(",") if len(x) > 0] if len(
                    dwh.config_json['SCD2']) > 0 else []
scd_t1_columns = [x for x in dwh.config_json['SCD1'].split(",") if len(x) > 0] if len(
    dwh.config_json['SCD1']) > 0 else []  # get all the scd type 1 columns
unique_columns = dwh.config_json['DWH_UNIQUE_ENTRIES'].split(",")  # get the primary key(s)

df_all = dwh.select_from_db(f"""SELECT * FROM [DWH].masterdata.project_all """, dwh_engine)
df_all = df_all.fillna(np.nan).replace([np.nan], [None])
i=0
for pos in tqdm(project_pos['Project_Name_Pos']):
    df = df_all[df_all['Project_Name_Pos'] == pos]
    df2 = df.copy()
    if len(df) > 1 and not np.all(df['ANP_CURRENT_OWNERSHIP'].unique()==None):
        newest_entry = df[df['valid_to'].isna()]
        if len(newest_entry) > 0:
            df2 = df2[~df2['valid_to'].isna()]
            df_del = df[~df['surkey'].isin(pd.concat([newest_entry, df2]).drop_duplicates(keep=False,subset=scd_t2_columns + scd_t1_columns + unique_columns)['surkey'].tolist())]
            if len(df_del) > 0:
                df_del = df_del.sort_values(by='surkey')
                valid_from = df_del.iloc[0]['valid_from']
                df_del = df_del.drop(index=newest_entry.index)
                drop_list = df_del['surkey'].tolist()
                for sur in drop_list:
                    deletes.append(f"DELETE FROM [DWH].masterdata.project_all WHERE [surkey]={sur}")
                updates.append(f"UPDATE [DWH].masterdata.project_all SET [valid_from] = '{valid_from.strftime("%Y-%m-%d %H:%M:%S")}' WHERE surkey={newest_entry['surkey'].iloc[0]}")
    i+=1

print("\n".join(updates))
print("\n".join(deletes))






