import json
import sys

from dwh_lib import DWH
from datetime import datetime,timedelta
from glob import glob
import os
import subprocess
import calendar

def read_jsons(path) -> dict:
    """
    Reads the json files with the path given in the argument
    :return: None
    """

    search = sorted(glob(os.path.join(path, "*.json")))
    for i, file in enumerate(search):
        if os.path.basename(file) == 'CONFIG.json':
            with open(file, 'r', encoding='utf-8') as j:
                return json.loads(j.read())

if __name__ == '__main__':
    today = datetime.now()
    config_json = read_jsons(sys.argv[1])
    for name,args in config_json.items():
        if args['Is_Active'] == "True":
            if args['Trigger'] == 'Daily':
                exit_code = subprocess.call(args['Executable'] + " " +args['Arguments'])
            elif args['Trigger'] == 'Weekly' and today.weekday() == 0:
                exit_code = subprocess.call(args['Executable'] + " " + args['Arguments'])
            elif args['Trigger'] == 'First of Month' and today.day == 1:
                exit_code = subprocess.call(args['Executable'] + " " + args['Arguments'])
            elif args['Trigger'] == 'Stock' and (today.weekday() == 6 or today.day == calendar.monthrange(today.year, today.month)[1]):
                exit_code = subprocess.call(args['Executable'] + " " + args['Arguments'])

