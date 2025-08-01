import os
import datetime
import time

import EodHistory
import EodToday
import datetime
import pytz
import traceback

while(True):
    nyc_datetime = datetime.datetime.now(pytz.timezone('US/Eastern'))
    #if nyc_datetime.hour in [9, 10, 11, 12, 13, 14, 15, 16] and nyc_datetime.weekday() in [0, 1, 2, 3, 4]:
    # if nyc_datetime.hour in [4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16] and nyc_datetime.weekday() in [0, 1, 2, 3, 4, 5, 6]:

    try:
        EodHistory.EodHistory(os.environ.get("EOD_USERNAME"), os.environ.get("EOD_PASSWORD"), 'NASDAQ',180, False)
        EodToday.EodToday(os.environ.get("EOD_USERNAME"), os.environ.get("EOD_PASSWORD"), 'NASDAQ', False)

        EodHistory.EodHistory(os.environ.get("EOD_USERNAME"), os.environ.get("EOD_PASSWORD"), 'NYSE', 180, False)
        EodToday.EodToday(os.environ.get("EOD_USERNAME"), os.environ.get("EOD_PASSWORD"), 'NYSE', False)

        time.sleep(3600)  # Sleep for 1 hour (3600 seconds)

    except Exception as e:
        now = datetime.datetime.now()
        print("{}  an exception occurred".format(now))
        traceback.print_exc()