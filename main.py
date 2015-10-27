__author__ = 'alex'

#!/usr/bin/env python
# -*- coding: utf-8 -*-
# 2015.10.25

import os
import requests
import json
import thread
import time
import re
from datetime import datetime
import csv
import copy


header_info = {
        'User-Agent':'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/43.0.2357.130 Safari/537.36',
        'Host':'news.163.com',
        'Connection':'keep-alive'
        }

# global configure
configure = {}

# task URLS
spider_tasks = []
spider_tasks_queue = []

# result queue
result_queue = []


def load_configure():
    text_file = open("configure.json", "r")
    whole_thing = text_file.read()
    global configure
    configure  = json.loads(whole_thing)


def init_task_urls():
    # "http://news.163.com/"
    global spider_tasks
    spider_tasks.append("http://news.163.com/")
    spider_tasks_queue.append("http://news.163.com/")


def generate_url_task(thread_name):
    global spider_tasks_queue
    global spider_tasks

    while True:
        spider_tasks_queue = copy.deepcopy(spider_tasks)
        time_interval = 10
        time.sleep(time_interval)


def spider_task_thread(thread_name):
    global spider_tasks_queue
    global result_queue

    while True:
        if spider_tasks_queue:
            task_url = spider_tasks_queue.pop()
            http_handle = requests.get(task_url, headers=header_info, timeout=60)
            if http_handle.status_code == 200:
                data = http_handle.text
                #     <a class="ac01" href="http://news.163.com/15/1026/18/B6SEGPVC00014JB5.html">test</a>
                # TODO: add the regular express to configure.json
                match = re.findall(r'<a class="ac01" href="(.*?)".*>(.*)</a>', data)
                if match:
                    for link, title in match:
                        print datetime.now().isoformat()
                        print "%s -> %s" % (link, title)
                        # date, link, title, source
                        result_data = [datetime.now().isoformat(), link, title.encode("utf-8"), task_url]
                        result_queue.append(result_data)
            else:
                print "response code:", http_handle.status_code

        # TODO: use the configure.json
        time_interval = 10
        time.sleep(time_interval)


# TODO: add the Persistence to topnews.csv
def save_to_csv_task(thread_name):
    global result_queue
    while True:
        time.sleep(10)
        if result_queue:
            with open("top_news.csv", "a") as myfile:
                writer = csv.writer(myfile)
                while result_queue:
                    writer.writerow(result_queue.pop())
                myfile.close()


if __name__ == '__main__':
    # 1. load the configure
    # TODO: add the auto load configure monitor
    load_configure()
    print configure

    # 2. init the task URLS
    init_task_urls()
    thread.start_new_thread(generate_url_task , ("generate_url_task",))

    # 3. sync thread
    thread.start_new_thread(save_to_csv_task , ("save_to_csv_task",))

    # 4. worker thread
    try:
        # TODO: use the configure.json
        THREAD_NUMBER = 4
        for i in range(THREAD_NUMBER):
            thread.start_new_thread(spider_task_thread , ("spider_task",))
    except:
       print "Error: unable to start thread"

    # main thread
    while True:
        time.sleep(60)