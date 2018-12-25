# -*- coding: utf-8 -*-

from gevent import monkey; monkey.patch_all()
import gevent
from gevent.queue import *

from io import BytesIO
import requests
import argparse
import sys
sys.path.insert(0, '/workspace/serving/python')
from evals.eval import *
import os
import json
import urllib
import time

def parse():
    args = argparse.ArgumentParser(description='inference')
    args.add_argument('--urlfile', type=str, required=True, help='url file')
    args.add_argument('--log', type=str, required=True, help='log file')
    args.add_argument('--n-producer', '-n', type=int, default=4,
        help='图片拉取并发数, 默认4')
    args.add_argument('--config', type=str, default='./config.json',
        help='配置文件')
    return args.parse_args()

# ----------------- infer module ----------------------
def make_reqs_body(img_batch):
    reqs = []
    for img in img_batch:
        item = {'data': {'uri': None, 'body': img[-1]}}
        reqs.append(item)
    return reqs

def init_model(config):
    '''模型初始化'''
    model, code, msg = create_net(config)
    if code != 0:
        print("init model error : %d ---> %s"%(code, msg))
        exit()
    return model

def infer_image_batch(model, reqs_body):
    rets, code, mess = net_inference(model, reqs_body)
    if code !=0:
        print("inference error : %d , %s"%(code, mess))
    return rets

def producer(img_queue, url_list, batch_size):
    cur_pid = os.getpid()
    num_urls = len(url_list)
    item_batch = []
    for i, url in enumerate(url_list):
        if len(item_batch) == batch_size:
            print("Produce %d, [%d / %d] "% (cur_pid, i, num_urls))
            img_queue.put(item_batch)
            item_batch = []
        try:
            #img = urllib.urlopen(url).read()
            img = requests.get(url, timeout=30).content
            if img is not None:
                item = (url, img)
                item_batch.append(item)
        #except urllib.error.URLError as e:
        except Exception as e:
            print("Exception: %s" % e)
    if len(item_batch) > 0:
        img_queue.put(item_batch)

def consumer(config, img_queue, batch_size, log_file):
    model = init_model(config)
    with open(log_file, 'w') as f:
        while True:
            print('Consumer: %d, Queue size: [%d(batches) : %d(images)]'%(os.getpid(), img_queue.qsize(), img_queue.qsize() * batch_size))
            item_batch = img_queue.get()
            # processing
            reqs_body = make_reqs_body(item_batch)
            rets = infer_image_batch(model, reqs_body)
            for i, ret in enumerate(rets):
                item = {}
                key = list(item_batch[i])[0]
                item[key] = ret['result']
                f.write(json.dumps(item))
                f.write('\n')
                f.flush()
            img_queue.task_done()

if __name__ == "__main__":
    print("Main Process : ", os.getpid())
    args = parse()
    urlfile = args.urlfile
    num_producer = args.n_producer
    log_file = args.log

    urls = []
    with open(urlfile, 'r') as f:
        for line in f:
            urls.append(line.strip())

    cfg = args.config
    with open(cfg, 'r') as f:
        config = json.load(f)
    batch_size = config['batch_size']

    num_urls = len(urls) 
    num_per_producer = num_urls // num_producer

    img_queue = JoinableQueue(maxsize=1024)

    c = gevent.spawn(consumer, config, img_queue, batch_size, log_file)

    producer_list = []
    for i in range(num_producer):
        if i == num_producer - 1:
            sub_urls = urls[i * num_per_producer :]
        else:
            sub_urls = urls[i * num_per_producer : (i + 1) * num_per_producer]
        p = gevent.spawn(producer, img_queue, sub_urls, batch_size)
        producer_list.append(p)

    gevent.joinall(producer_list)
    img_queue.join()
    gevent.killall([c])
    print("Done.")


