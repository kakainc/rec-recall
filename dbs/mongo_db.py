# -*- coding: utf-8 -*-

import pymongo

# recommend_pool
DB_RECPOOL_HOST = ['']
DB_RECPOOL_PORT = 3717
DB_RECOMMENDPOOL = 'recommend_pool'
COL_RECOMMENDPOOL = 'recommend_pool'
NEWPOST_BOOTPOOL = 'newpost_step_boot'

# recommend
DB_RECOMMEND_HOST = ['dds-t4na87056b54f4941.mongodb.singapore.rds.aliyuncs.com', 'dds-t4na87056b54f4942.mongodb.singapore.rds.aliyuncs.com']
DB_RECOMMEND_PORT = 3717
DB_RECOMMEND = 'recommend'
COL_STANDARD_POST = 'post_standard'


def get_col(host, port, db_name, col_name, replica_set):
    c = pymongo.MongoClient(host, port, replicaSet=replica_set)
    c.admin.authenticate("root", "")
    db = c.get_database(db_name)
    col = db.get_collection(col_name)
    return col


def get_col_recommend_pool():
    return get_col(DB_RECPOOL_HOST, DB_RECPOOL_PORT, DB_RECOMMENDPOOL, COL_RECOMMENDPOOL, "")


def get_col_newpost_step_boot():
    return get_col(DB_RECPOOL_HOST, DB_RECPOOL_PORT, DB_RECOMMENDPOOL, NEWPOST_BOOTPOOL, "")


def get_col_post_standard():
    return get_col(DB_RECOMMEND_HOST, DB_RECOMMEND_PORT, DB_RECOMMEND, COL_STANDARD_POST, "")
