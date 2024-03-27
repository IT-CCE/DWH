import datetime
import sys
import msal
import pandas as pd
import requests

#sys.path.insert(1, 'U:\\DB_new')
from dwh_lib import DWH
import numpy as np

timestamp = datetime.datetime.now()


################################################### License ###########################################################

dwh = DWH(timestamp, sys.argv[1], False)

client_id = dwh.config_json['client_id']
client_secret = dwh.config_json['client_secret']
authority = dwh.config_json['authority']
scope = ['https://graph.microsoft.com/.default']

client = msal.ConfidentialClientApplication(client_id, authority=authority, client_credential=client_secret)
token_result = client.acquire_token_silent(scope, account=None)
if token_result:
    access_token = 'Bearer ' + token_result['access_token']
else:
    token_result = client.acquire_token_for_client(scopes=scope)
    access_token = 'Bearer ' + token_result['access_token']

headers = {
    "Authorization": access_token
}


def get_result(url, access_value='value'):
    #i = 1
    graph_results = []
    while True:
        graph_result = requests.get(url=url, headers=headers).json()

        graph_results.extend(graph_result[access_value])
        if '@odata.nextLink' in graph_result:
            url = graph_result['@odata.nextLink']
        else:
            break
        #print(f"Result {i}")
        #i += 1
    return graph_results


graph_results = get_result(url="https://graph.microsoft.com/v1.0/groups?$select=id,displayName")
license_ids = [(x['displayName'], x['id']) for x in graph_results if 'License' in x['displayName']]

group_members = {}
for group, license_id in license_ids:
    all_users_in_group = [x['id'] for x in
                          get_result(url=f"https://graph.microsoft.com/v1.0/groups/{license_id}/members?"
                                         f"$select=id,onPremisesSyncEnabled") if x['onPremisesSyncEnabled'] == True]
    group_members[group] = all_users_in_group

all_employees = [x['id'] for x in get_result(url='https://graph.microsoft.com/v1.0/users?$select=id,companyName,mail')]

user_list = []
header = ['id', 'displayName', 'jobTitle', 'userPrincipalName', 'LOGIN'] + list(group_members.keys())
for user in get_result(url="https://graph.microsoft.com/v1.0/users?$filter=onPremisesSyncEnabled in ('True')"):
    if user['id'] in all_employees or 'Administrator' in user['displayName']:
        user_list.append([user['id'], user['displayName'], user['jobTitle'], user['userPrincipalName'],
                          user['userPrincipalName'].split("@")[0]] + [user['id'] in v for k, v in
                                                                      group_members.items()])

df_license = pd.DataFrame(user_list, columns=header)




source_engine = dwh.connect_to_db(server=dwh.config_json['SOURCE_SERVER'],
                                  username=dwh.config_json['SOURCE_USERNAME'],
                                  password=dwh.config_json['SOURCE_PASSWORD'],
                                  database=dwh.config_json[
                                      'SOURCE_DATABASE'])  # connect to database | DWH

source_select_query = dwh.read_query(query_type='SOURCE_QUERY')
df_license2 = dwh.select_from_db(select_query=source_select_query,
                                 engine=source_engine)

df_license2 = df_license2.rename(columns={'APplus Lesend': 'License APplus Lesend'})

df_license2['License APplus Schreibend'] = False

df_license2['License APplus Lesend'] = df_license2['License APplus Lesend'].astype(bool)

df_license2.loc[df_license2['Keine Lizenz']==1,'License APplus Schreibend'] = False
df_license2.loc[df_license2['Keine Lizenz']==1,'License APplus Lesend'] = False


df_license2.loc[(df_license2['Keine Lizenz']==0) & ((df_license2['License APplus Lesend'] == False) | (df_license2['License APplus Lesend'] == 0)),'License APplus Lesend'] = False
df_license2.loc[(df_license2['Keine Lizenz']==0) & ((df_license2['License APplus Lesend'] == False) | (df_license2['License APplus Lesend'] == 0)),'License APplus Schreibend'] = True

df_license2.loc[(df_license2['Keine Lizenz']==0) & ((df_license2['License APplus Lesend'] == True) | (df_license2['License APplus Lesend'] == 1)),'License APplus Lesend'] = True
df_license2.loc[(df_license2['Keine Lizenz']==0) & ((df_license2['License APplus Lesend'] == True) | (df_license2['License APplus Lesend'] == 1)),'License APplus Schreibend'] = False

df_license2.loc[df_license2[df_license2['LOGIN'] == "j.bobee"].index, 'LOGIN'] = 'ext.jbobee'
df_license2.loc[df_license2[df_license2['LOGIN'] == "c.ondraschek"].index, 'LOGIN'] = 'c.mostbauer'
df_license['LOGIN'] = df_license['LOGIN'].apply(lambda x: str(x).lower())
df_license2['LOGIN'] = df_license2['LOGIN'].apply(lambda x: str(x).lower())


df_license = pd.merge(df_license, df_license2, how='left', on='LOGIN')
# check = pd.merge(df_license2, df_license, how='left', on='LOGIN')

## df_license[list(group_members.keys())] = df_license[list(group_members.keys())].fillna(False)
## df_license = df_license[df_license[list(group_members.keys())].any(axis=1)]
##
## df_license['timestamp']=df_license['timestamp'].apply(lambda x: datetime.datetime.strftime(x,"%Y-%m-%d %H:%M:%S"))
##
## print(df_license[df_license['Firmenname'].isna()])
##
## df_license[df_license['Firmenname'].isna()][['displayName','LOGIN','Firmenname','Kostenstelle']].fillna(np.nan).replace([np.nan], [None]).to_csv("U:\\Billing_New\\user_neu.csv")
df_license = df_license.fillna(np.nan).replace([np.nan], [None])
df_license = df_license.rename(columns={'userPrincipalName': 'email',
                                        ' License Deepl Advanced': 'License Deepl Advanced',
                                        ' License Adobe Standard': 'License Adobe Standard',
                                        ' License Meteonorm': 'License Meteonorm',
                                        ' License Deepl Starter': 'License Deepl Starter',
                                        ' License ChatGPT': 'License ChatGPT',
                                        ' License Deepl Ultimate': 'License Deepl Ultimate',
                                        ' License Adobe Creative Cloud': 'License Adobe Creative Cloud',
                                        ' License Adobe Pro': 'License Adobe Pro'})


df_license_cost = dwh.select_from_db(select_query=f"SELECT * FROM [DWH].[billing].[License_Price]", engine=dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'],
                                  username=dwh.config_json['DWH_USERNAME'],
                                  password=dwh.config_json['DWH_PASSWORD'],
                                  database=dwh.config_json[
                                      'DWH_DATABASE']))

cols = [x for x in list(df_license.columns) if 'License' in x or 'APplus' in x]
def calc_cost(x):
    all_entries = df_license_cost[df_license_cost['Name'].isin(x[cols].index[x[cols] == True])]
    all_entries['is_valid'] = all_entries['valid_to'].apply(lambda x: x>=timestamp)
    return sum(all_entries[all_entries['is_valid']==True].sort_values(by=['Name', 'valid_to']).groupby('Name').first()['Rate'])



df_license['LizenzKosten'] = df_license.apply(calc_cost,axis=1)

dwh.execute_plain_insert_source(df_source=df_license)


################################################### License ###########################################################
################################################### PC ################################################################

dwh_pc = DWH(timestamp, sys.argv[2], False)

df_pc_price = dwh_pc.select_from_db(select_query=f"SELECT * FROM [DWH].[billing].[PC_Price]", engine=dwh_pc.connect_to_db(server=dwh_pc.config_json['DWH_SERVER'],
                                  username=dwh_pc.config_json['DWH_USERNAME'],
                                  password=dwh_pc.config_json['DWH_PASSWORD'],
                                  database=dwh_pc.config_json[
                                      'DWH_DATABASE']))


header = ['id', 'displayName', 'accountEnabled', 'operatingSystem', 'operatingSystemVersion', 'trustType',
          'registeredOwnersID','username','managementType', 'isCompliant', 'registrationDateTime',
          'approximateLastSignInDateTime', 'deviceId', 'isManaged', 'profileType', 'model']
all_devices = [(x['id'], x['displayName'], x['accountEnabled'], x['operatingSystem'], x['operatingSystemVersion']
                , x['trustType'], x['registeredOwners'][0]['id'] if len(x['registeredOwners']) > 0 else None,
                x['registeredOwners'][0]['userPrincipalName'].split("@")[0] if len(x['registeredOwners']) > 0 else None,
                x['managementType'],
                x['isCompliant'],x['registrationDateTime'], x['approximateLastSignInDateTime'], x['deviceId'], x['isManaged'],
                x['profileType'], x['model']) for x in
               get_result(url='https://graph.microsoft.com/v1.0/devices?$expand=registeredOwners')
               if x['managementType'] == 'MDM' and x['operatingSystem'] == 'Windows']
all_devices = pd.DataFrame(all_devices, columns=header)

def get_model_type(type_str):
    if type_str is None:
        return None
    if 'yoga' in type_str or 'Yoga' in type_str or 'Surface Pro' in type_str or 'surface pro' in type_str:
        return 'Traveler'

    if 'Surface Go' in type_str or 'surface go' in type_str:
        return 'Standard'

    found_model = [x.split(" ")[1] for x in all_models if type_str[:4] in x]
    if len(found_model) > 0:
        if 'E' in found_model[0] or 'L' in found_model[0] or 'V' in found_model[0]:
            return 'Standard'
        elif 'P' in found_model[0]:
            return 'Performance'
        elif 'X' in found_model[0]:
            return 'Traveler'
    return None

all_models = [x['name'] for x in requests.get("https://download.lenovo.com/bsco/public/allModels.json").json()]

all_devices['systemLabels']=all_devices['model'].apply(lambda x: get_model_type(x))



all_devices.loc[all_devices[all_devices['trustType'] == "AzureAd"].index, 'trustType'] = 'Entra joined'
all_devices.loc[all_devices[all_devices['trustType'] == "ServerAd"].index, 'trustType'] = 'Entra hybrid joined'
all_devices.loc[all_devices[all_devices['trustType'] == "Workplace"].index, 'trustType'] = 'Entra registered'
all_devices = all_devices[all_devices['trustType']=='Entra hybrid joined']


def calc_cost_2(x):
    all_entries = df_pc_price[df_pc_price['Name'].isin([x['systemLabels']])]
    all_entries['is_valid'] = all_entries['valid_to'].apply(lambda x: x>=timestamp)
    return sum(all_entries[all_entries['is_valid']==True].sort_values(by=['Name', 'valid_to']).groupby('Name').first()['Rate'])


all_devices['PcKosten'] = all_devices.apply(calc_cost_2,axis=1)
dwh_pc.execute_plain_insert_source(df_source=all_devices)

################################################### PC ################################################################
################################################### Infra #############################################################

dwh_infra = DWH(timestamp, sys.argv[3], False)

df_infrastructure = pd.read_excel("Y:\\IT\\13_Controls KPIs\\Budget\\Infrastructure.xlsx", header=0)
df_infrastructure_price = dwh_infra.select_from_db(select_query=f"SELECT * FROM [DWH].[billing].[Infrastructure_Price]", engine=dwh_infra.connect_to_db(server=dwh_infra.config_json['DWH_SERVER'],
                                  username=dwh_infra.config_json['DWH_USERNAME'],
                                  password=dwh_infra.config_json['DWH_PASSWORD'],
                                  database=dwh_infra.config_json[
                                      'DWH_DATABASE']))
cols = ['FW','SW24','SW48','AP']
def calc_cost_3(x):

    df_infrastructure_price.loc[:,'Amount'] = x[cols].values
    df_infrastructure_price['is_valid'] = df_infrastructure_price['valid_to'].apply(lambda x: x>=timestamp)
    df_infrastructure_price['cost'] = df_infrastructure_price['Amount'] * df_infrastructure_price['Rate']
    return sum(df_infrastructure_price[df_infrastructure_price['is_valid']==True].sort_values(by=['Name', 'valid_to']).groupby('Name').first()['cost'])


df_infrastructure['Infrastructure_Cost'] = df_infrastructure.apply(calc_cost_3,axis=1)
df_infrastructure = df_infrastructure.fillna(np.nan).replace([np.nan], [None])
dwh_infra.execute_plain_insert_source(df_infrastructure)

################################################### Infra #############################################################





