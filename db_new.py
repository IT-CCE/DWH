import sys

from dwh_lib import DWH
import datetime


if __name__ == "__main__":
    timestamp = datetime.datetime.now()
    if len(sys.argv) == 2:
        dwh = DWH(timestamp, sys.argv[1], True)
    else:
        dwh = DWH(timestamp, sys.argv[1], False)
    dwh.execute_source_dwh()
