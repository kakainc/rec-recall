# -*- coding:utf-8 -*-

import sys

from dbs import redis_db


def update_to_redis(file_path, prefix):
    cf_redis = redis_db.get_cf_redis()
    with open(file_path, 'r') as file_handler:
        for line in file_handler:
            info = line.strip().split('\x01')
            if len(info) == 1:
                info = line.strip().split(' ')
            if len(info) != 2:
                continue

            value = info[1]
            temp_list = value.split(',')
            if len(temp_list) > 100:
                value = ','.join(temp_list[:100])

            key = prefix + info[0]
            cf_redis.setex(key, 2 * 86400, value)


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 3:
        print("lack args")
    file_path = args[1]
    prefix = args[2]

    update_to_redis(file_path, prefix)

    print("i2i build finish")
