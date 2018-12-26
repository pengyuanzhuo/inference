#-*- coding: utf-8 -*-

import json
import argparse

def parse():
    args = argparse.ArgumentParser(description='url布尔操作')
    args.add_argument('--urlfile_a', '-a', type=str, required=True, 
        help='url文件A, 每行一个url')
    args.add_argument('--urlfile_b', '-b', type=str, required=True, 
        help='url文件B, 每行一个包含url字段的json')
    args.add_argument('--operate', '-o', type=str, choices=['diff'],
        help='操作. diff差集')
    args.add_argument('--urlfile_c', '-c', type=str, required=True,
        help='输出文件')
    return args.parse_args()

def bool_operate(urla, urlb, urlc, op='diff'):
    f_a = open(urla, 'r')
    a_list = []
    for line_a in f_a:
        a_list.append(line_a.strip())
    f_a.close()

    f_b = open(urlb, 'r')
    b_list = []
    for line_b in f_b:
        url = list(json.loads(line_b.strip()).keys())[0]
        b_list.append(url)
    f_b.close()

    with open(urlc, 'w') as f_c:
        if op == 'diff':
            diff = set(a_list).difference(set(b_list))
            for url in diff:
                f_c.write("%s\n"%url)

if __name__ == "__main__":
    args = parse()
    urla = args.urlfile_a
    urlb = args.urlfile_b
    op = args.operate
    urlc = args.urlfile_c

    bool_operate(urla, urlb, urlc, op)



