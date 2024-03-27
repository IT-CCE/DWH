import sys

from dwh_lib import DWH
import datetime
from tqdm import tqdm


def main(args):

    for i in tqdm(range(1,120)):
        timestamp = datetime.datetime.now()
        timestamp = timestamp - datetime.timedelta(days=i)
        if len(args) == 2:
            dwh = DWH(timestamp, args[1], True)
        else:
            dwh = DWH(timestamp, args[1], False)
        dwh.execute_source_dwh()

if __name__ == '__main__':
    main(sys.argv)
