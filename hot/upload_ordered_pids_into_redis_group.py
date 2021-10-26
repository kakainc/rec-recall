# -*- coding: utf-8 -*-

import argparse
import codecs

from dbs import redis_db
from utils import dingding_robot


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('infile_path')
    parser.add_argument('prefix')
    parser.add_argument('key_index_list', nargs='+', type=int)
    parser.add_argument('--pid_index', type=int, default=0)
    parser.add_argument('--score_index', type=int, default=1)
    parser.add_argument('--top', type=int, default=0)
    parser.add_argument('--min_limit', type=int, default=200)
    parser.add_argument('--expiration', type=int, default=86400 * 30)

    args = parser.parse_args()

    return args


def warning(content):
    dingding_robot.send_warning(content, phone_numbers=['123456789'])


if __name__ == '__main__':
    args = get_args()
    key_map = dict()

    key_index_list = args.key_index_list
    if len(key_index_list) == 0:
        raise BaseException('not input key_index_list')

    with codecs.open(args.infile_path, 'r', 'utf8') as file_handler:
        for line in file_handler:
            info = line.strip('\n').split(',')

            key_list = []
            for key_index in key_index_list:
                key_list.append(info[key_index])

            key = '_'.join(key_list)
            if len(key) == 0:
                continue
            key_map.setdefault(key, [])

            pid = int(info[args.pid_index])
            score = float(info[args.score_index])
            key_map[key].append((pid, score))

    for key, pool in key_map.items():
        values = sorted(pool, key=lambda x: x[1], reverse=True)
        index_name = args.prefix + key
        top = args.top
        if top > 0:
            values = values[:top]

        if args.min_limit > len(values):
            warning('{0} 帖子不足，拒绝更新'.format(index_name))

        r = redis_db.get_index_redis()

        pid_scores = ','.join(["%d:%f" % (k, v) for k, v in values])

        expiration = args.expiration
        r.set(index_name, pid_scores, ex=expiration)
