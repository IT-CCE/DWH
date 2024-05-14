from email.mime.multipart import MIMEMultipart

from dwh_lib import DWH
import os
import glob
import datetime
import time
import sys
import smtplib
from email.message import EmailMessage
import pandas as pd
import json
from glob import glob
from email.mime.text import MIMEText

def read_config(json_file):
    search = sorted(glob(os.path.join(sys.argv[1], "*.json")))
    for i, file in enumerate(search):
        if os.path.basename(file) == json_file:
            with open(file, 'r', encoding='utf-8') as j:
                return json.loads(j.read())


def send_mail(To,html):
    msg = MIMEMultipart()
    host = "mail.cc-energy.com"
    port = 25

    msg['Subject'] = "Automatic Notification"
    msg['From'] = 'it@cce-holding.com'
    msg['To'] = To

    msg.attach(MIMEText(html, 'html'))
    smtpObj = smtplib.SMTP(host=host, port=port)
    smtpObj.send_message(msg)


if __name__ == '__main__':
    config_note = read_config('CONFIG_Notification.json')
    now = datetime.datetime.now()
    for event, event_desc in config_note.items():
        dwh = DWH(now, event_desc['Table_Path'], True)
        df_dwh = dwh.get_df_dwh()
        df_dwh = df_dwh.query(event_desc['Condition'])
        df_dwh = df_dwh[df_dwh['timestamp'] >= now-datetime.timedelta(days=1)]

        source = DWH(now, sys.argv[1], False)
        df_source = source.get_df_custom("SELECT * FROM [TODO]", db_type='source')

        users = df_source[(df_source['Subscription'] == 'event') & (df_source[df_source['valid_to'] <= now])]['User']

        if len(df_dwh) > 0 and len(users) > 0:
            if event == 'Milestone(s) achieved' or event == "Milestone 4 achieved":
                msg = '\n'.join([f"<li> Milestone: {x} achieved in Project: {y}</li>" for x,y in zip(df_dwh['project_pos_name'].tolist(),df_dwh['PROJEKT_Name'].tolist())])
                html = f'''<h1> {event} </h1><ul>{msg}</ul>'''
            else:
                html=""

            html += """<p>Please note that this message has been automatically generated. If you have any questions or require further assistance, feel free to contact:</p>
                    <ul>
                    <li>Email: <a href="mailto:a.huber@cce-holding.com">a.huber@cce-holding.com</a></li>
                    <li>IT: <a href="mailto:it@cce-holding.com">it@cce-holding.com</a></li>
                    </ul>
                    <p>Thank you for your understanding.</p>
                    <p>Best regards,<br>
                    Your IT-Team</p>"""
            send_mail(To=','.join(users), html=html)