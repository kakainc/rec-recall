# -*- coding: utf-8 -*-

from odps import ODPS


def get_odpscmd():
    access_id = ''
    access_key = ''
    default_project = 'data'
    endpoint = ''
    odps = ODPS(access_id, access_key, default_project, endpoint=endpoint)
    return odps
