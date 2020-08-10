# -*- coding: utf-8 -*-
import os
import sys
sys.setrecursionlimit(10000000)
import schedule
import time
import datetime
def pachong():
    if __name__ == '__main__':
        # os.system('python bloomberg.py')
        # os.system('python bloomberg_home.py')
        # os.system('python bloomberg_new.py')
        os.system('python nytimes.py')
    print('爬虫已经工作完毕！')
schedule.every(1).minutes.do(pachong)
# schedule.every(6).hours.do(pachong)
# schedule.every().day.at("10:30").do(pachong)
# schedule.every().monday.do(pachong)
# schedule.every().wednesday.at("13:15").do(pachong)
# schedule.every().minute.at(":17").do(pachong)

while True:
    schedule.run_pending()
    time.sleep(5)
    now = datetime.datetime.now()
    print(now.hour, now.minute)