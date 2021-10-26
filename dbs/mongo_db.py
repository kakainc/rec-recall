# -*- coding: utf-8 -*-

import pymongo

# recommend_pool
DB_RECPOOL_HOST = ['']
DB_RECPOOL_PORT = 3717
DB_RECOMMENDPOOL = 'recommend_pool'
COL_RECOMMENDPOOL = 'recommend_pool'
NEWPOST_BOOTPOOL = 'newpost_step_boot'


def get_col(host, port, db_name, col_name, replica_set):
    c = pymongo.MongoClient(host, port, replicaSet=replica_set)
    c.admin.authenticate("root", "b7sameEdxrfDiu3a")
    db = c.get_database(db_name)
    col = db.get_collection(col_name)
    return col


def get_col_recommend_pool():
    return get_col(DB_RECPOOL_HOST, DB_RECPOOL_PORT, DB_RECOMMENDPOOL, COL_RECOMMENDPOOL, "")


def get_col_newpost_step_boot():
    return get_col(DB_RECPOOL_HOST, DB_RECPOOL_PORT, DB_RECOMMENDPOOL, NEWPOST_BOOTPOOL, "")
