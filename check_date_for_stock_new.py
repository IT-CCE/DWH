import datetime
import sys
import calendar
from dwh_lib import DWH

today = datetime.datetime.today()
if today.weekday() == 6 or today.day==calendar.monthrange(today.year, today.month)[1]:
    dwh = DWH(today, sys.argv[1], False)
    dwh.execute_source_dwh()