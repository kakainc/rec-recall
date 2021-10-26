# -*- coding: utf-8 -*-

import redis

INDEX_REDIS_HOST = ''
INDEX_REDIS_PORT = 6379
INDEX_REDIS_AUTH = ''

OFFLINE_REDIS_HOST = ''
OFFLINE_REDIS_PORT = 6379
OFFLINE_REDIS_AUTH = ''

CF_REDIS_HOST = ''
CF_REDIS_PORT = 6379
CF_REDIS_AUTH = ''


def get_redis(host, port, db=0, password=''):
    pool = redis.ConnectionPool(host=host, port=port, db=db, password=password)
    redis_conn = redis.StrictRedis(connection_pool=pool)
    return redis_conn


def get_index_redis():
    return get_redis(INDEX_REDIS_HOST, INDEX_REDIS_PORT, 0, INDEX_REDIS_AUTH)


def get_offline_redis():
    return get_redis(OFFLINE_REDIS_HOST, OFFLINE_REDIS_PORT, 0, OFFLINE_REDIS_AUTH)


def get_cf_redis():
    return get_redis(CF_REDIS_HOST, CF_REDIS_PORT, 0, CF_REDIS_AUTH)