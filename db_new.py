import sys

from dwh_lib import DWH
import datetime


def main(args):
    timestamp = datetime.datetime.now()
    if len(args) == 2:
        dwh = DWH(timestamp, args[1], True)
    else:
        dwh = DWH(timestamp, args[1], False)
    dwh.execute_source_dwh()
    return 0

if __name__ == '__main__':
    main(sys.argv)
