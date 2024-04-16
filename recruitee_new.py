import datetime
import sys
import time
import traceback

import numpy as np
import pandas as pd
import requests

from dwh_lib import DWH



def get_data(url, mode=1):
    if mode==1:
        json_dicts = []
        dict_length = 1
        page_nr = 1
        while dict_length > 0:
            url = url+f"?limit=500&page={page_nr}"

            headers = {
                "accept": "application/json",
                "authorization": dwh.config_json['TOKEN']
            }

            response = requests.get(url, headers=headers)
            if response.status_code == 200:
                response = response.json()
                if len(response['hits']) > 0:
                    dict_length = len(response['hits'])
                    json_dicts.append(response)
                else:
                    dict_length = 0
            page_nr += 1

        all_data = []
        for dictionary in json_dicts:
            all_data.extend(dictionary['hits'])
    else:
        headers = {
            "accept": "application/json",
            "authorization": dwh.config_json['TOKEN']
        }

        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            all_data = response.json()

    return all_data

if __name__ == '__main__':
    try:
        start_time = time.time()
        dwh = DWH(datetime.datetime.now(), sys.argv[1], True)

        dwh_engine = dwh.connect_to_db(server=dwh.config_json['DWH_SERVER'], username=dwh.config_json['DWH_USERNAME'],
                                       password=dwh.config_json['DWH_PASSWORD'],
                                       database=dwh.config_json['DWH_DATABASE'])
        json_dicts = []
        dict_length = 1
        i = 1
        all_hits = get_data("https://api.recruitee.com/c/78057/search/new/candidates")
        all_notes = get_data("https://api.recruitee.com/c/78057/search/new/notes")



        # url3 = "https://api.recruitee.com/c/78057/custom_fields/candidates/66053841/fields"
        #
        # headers3 = {
        #     "accept": "application/json",
        #     "authorization": dwh.config_json['TOKEN']
        # }
        #
        # response3 = requests.get(url3, headers=headers3)
        #
        # response3 = response3.json()
        #
        # print(response3)


        applicants = []

        cols = ['employee_id', 'created_at', 'admin_id', 'phones', 'emails', 'name', 'photo_url', 'source',
                'positive_ratings',
                'disqualified', 'disqualified_at', 'disqualified_by', 'disqualified_by_name', 'disqualify_kind',
                'disqualify_reason',
                'hired_at', 'is_hired', 'job_start', 'offer_id', 'offer_title', 'offer_status', 'stage_id',
                'stage_name', 'source_name',
                'source_id', 'tag_name', 'tag_id', 'talent_pool', 'report_notes']

        for i, hit in enumerate(all_hits):
            applicant = []
            applicant.extend([hit['created_at'], hit['admin_id'], ", ".join(hit['phones']), ", ".join(hit['emails']),
                              hit['name'], hit['photo_thumb_url'], hit['source'], hit['positive_ratings']])

            cur_notes = [x for x in all_notes if hit['id'] == x['candidate']['id']]


            stage_id, stage_name = None, None
            disqualified, disqualified_at, disqualified_by, disqualified_by_name, disqualify_kind, disqualify_reason = None, None, None, None, None, None
            hired_at, is_hired, offer_id, talent_pool, offer_title, offer_status = None, None, 0, None, "Initiativ", None
            source_name, source_id = None, None
            tag_name, tag_id = None, None
            notes, job_start = None, None

            if len(hit['sources']) > 0:
                if 'name' in hit['sources'][0]:
                    source_name = hit['sources'][0]['name']
                if 'id' in hit['sources'][0]:
                    source_id = hit['sources'][0]['id']

            if len(hit['tags']) > 0:
                if 'name' in hit['tags'][0]:
                    tag_name = hit['tags'][0]['name']
                if 'id' in hit['tags'][0]:
                    tag_id = hit['tags'][0]['id']

            placements = []

            if len(hit['placements']) > 0:
                placements = hit['placements']

            for placement in placements:
                if placement['offer']['kind'] == 'talent_pool':
                    talent_pool = placement['offer']['title'] if talent_pool is None else talent_pool + ", " + \
                                                                                          placement['offer']['title']
                    placements.remove(placement)

            if len(placements) == 0:
                applicant.insert(0, hit['id'])
                applicant.extend([disqualified, disqualified_at, disqualified_by, disqualified_by_name, disqualify_kind,
                                  disqualify_reason,
                                  hired_at, is_hired, job_start, offer_id, offer_title, offer_status, stage_id,
                                  stage_name,
                                  source_name, source_id, tag_name, tag_id, talent_pool, notes])
                applicants.append(applicant)

            for placement in placements:
                applicant_copy = applicant.copy()
                disqualified, disqualified_at, disqualified_by, disqualified_by_name, disqualify_kind, disqualify_reason = None, None, None, None, None, None
                hired_at, is_hired, offer_id, offer_title, offer_status, job_start = None, None, None, None, None, None
                notes = None
                applicant_copy.insert(0, hit['id'])
                if 'disqualified' in placement:
                    disqualified = placement['disqualified']
                if 'disqualified_at' in placement:
                    disqualified_at = placement['disqualified_at']
                if 'disqualified_by' in placement:
                    disqualified_by = placement['disqualified_by']
                if 'disqualified_by_name' in placement:
                    disqualified_by_name = placement['disqualified_by_name']
                if 'disqualify_kind' in placement:
                    disqualify_kind = placement['disqualify_kind']
                if 'disqualify_reason' in placement:
                    disqualify_reason = placement['disqualify_reason']
                if 'hired_at' in placement:
                    hired_at = placement['hired_at']
                if 'is_hired' in placement:
                    is_hired = placement['is_hired']
                if 'job_starts_at' in placement:
                    job_start = placement['job_starts_at']
                if 'offer' in placement:

                    if 'id' in placement['offer']:
                        offer_id = placement['offer']['id']
                    if 'title' in placement['offer']:
                        offer_title = placement['offer']['title']
                    if 'status' in placement['offer']:
                        offer_status = placement['offer']['status']

                if placement['stage'] is not None:
                    if 'id' in placement['stage']:
                        stage_id = placement['stage']['id']
                    if 'name' in placement['stage']:
                        stage_name = placement['stage']['name']

                combined_note = ""
                if len(cur_notes) > 0:
                    for note in cur_notes:
                        if '@Report' in note['body_html']:
                            combined_note += f"{note['body_html']}\n"

                if len(combined_note) == 0:
                    notes = None
                else:
                    notes = combined_note

                applicant_copy.extend(
                    [disqualified, disqualified_at, disqualified_by, disqualified_by_name, disqualify_kind,
                     disqualify_reason,
                     hired_at, is_hired, job_start, offer_id, offer_title, offer_status, stage_id, stage_name,
                     source_name, source_id, tag_name, tag_id, talent_pool, notes])
                applicants.append(applicant_copy)

        df_source = pd.DataFrame(applicants)
        df_source.columns = cols
        df_source['created_at'] = df_source['created_at'].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") if x is not None else None)
        df_source['disqualified_at'] = df_source['disqualified_at'].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") if x is not None else None)
        df_source['hired_at'] = df_source['hired_at'].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") if x is not None else None)
        df_source['job_start'] = df_source['job_start'].apply(
            lambda x: datetime.datetime.strptime(x, "%Y-%m-%dT%H:%M:%SZ") if x is not None else None)


        def subtract(x):
            return (x.iloc[0].replace(hour=0, minute=0, second=0) - x.iloc[1].replace(hour=0, minute=0,
                                                                                      second=0)).days if x.iloc[
                                                                                                             0] is not None else None


        df_source['time_to_hire'] = df_source[['hired_at', 'created_at']].apply(lambda x: subtract(x), axis=1)
        df_source['time_to_start'] = df_source[['job_start', 'hired_at']].apply(lambda x: subtract(x), axis=1)

        df_source['offer_notes'] = ""
        df_source['department'] = ""


        offer_ids = list(df_source['offer_id'].unique())
        offer_ids.remove(0)

        for id in offer_ids:
            json_data = get_data(f"https://api.recruitee.com/c/78057/offers/{id}/notes", mode=2)
            if len(json_data['notes']) > 0:
                df_source.loc[df_source['offer_id']==id,"offer_notes"] = "<br>".join([x["body_html"] for x in json_data['notes']])
            json_data2 = get_data(f"https://api.recruitee.com/c/78057/offers/{id}", mode=2)
            if 'department' in json_data2['offer']:
                df_source.loc[df_source['offer_id']==id,'department'] = json_data2['offer']['department']


        df_source = df_source.fillna(np.nan).replace([np.nan], [None])
        dwh.execute_source(df_source=df_source)
        print("Done")
    except Exception as e:
        ex_type, ex_value, ex_traceback = sys.exc_info()
        trace_back = traceback.extract_tb(ex_traceback)
        last_track_back = trace_back[-1]

        job_select_query, job_create_query = dwh.read_query(query_type='JOB_QUERY')
        dwh.create_db_if_not_exists(sql_select_query=job_select_query, sql_create_query=job_create_query,
                                    engine=dwh_engine)
        df_job_q = dwh.select_from_db(select_query=job_select_query,
                                      engine=dwh_engine)  # get entries from job database
        dest_table_job = dwh.dest_table(select_query=job_select_query)  # get table name for job table
        job_columns = dwh.table_columns(df_job_q)  # get table columns for job table
        exception_string = (f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
                            f"Exception type: {ex_type}\nException value: {ex_value}")
        df_job = pd.DataFrame([[dwh.config_json['JOB_NAME'], -1, -1, -1, exception_string, dwh.timestamp]],
                              columns=job_columns[1:])
        # create entry for failed job
        dwh.insert_into_db(destination_table=dest_table_job, df_insert=df_job, engine=dwh_engine,
                           first_insert=False,
                           add_time_cols=False)  # insert the failure row
        print("Failed")
        print(f"Line Nr: {last_track_back[1]}\nLine: {last_track_back[3]}\n"
              f"Exception type: {ex_type}\nException value: {ex_value}")
