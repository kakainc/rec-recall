# -*- coding: utf-8 -*-

import requests
import json

from retry import retry
from requests.exceptions import RequestException


@retry(Exception, tries=3, delay=3)
def get_all_subarea():
    subarea_url = 'http://xxxxx.net/topic/httpapi/subarea_get_all'
    response = requests.post(subarea_url)
    content = json.loads(response.content)
    if content.get('ret', 0) != 1:
        raise RequestException
    p_list = content.get('data', {}).get('list', [])
    if len(p_list) == 0:
        raise RequestException

    part2_info_map = dict()
    for part_info in p_list:
        part1_id = part_info.get('_id', 0)
        part1_ct = part_info.get('ct', 0)
        part1_name = part_info.get('name', '').replace('\t', ' ')
        child_list = part_info.get('child_list', [])
        status = part_info.get('status', 100)
        if part1_id < 0 or status < 0:
            continue

        if not isinstance(child_list, list):
            continue
        for child_info in child_list:
            part2_id = child_info.get('_id', 0)
            part2_ct = child_info.get('ct', 0)
            part2_name = child_info.get('name', '').replace('\t', '')
            status = child_info.get('status', 100)
            if part2_id < 0 or status < 0:
                continue

            part2_info_map[part2_id] = {'part2_name': part2_name, 'part2_ct': part2_ct,
                                        'part1_id': part1_id, 'part1_name': part1_name, 'part1_ct': part1_ct}

    return part2_info_map


@retry(Exception, tries=3, delay=3)
def get_part_tids(part_ids):
    partid_2_tids_url = 'http://xxxxx.net/topic/httpapi/get_tids_by_partids'
    response = requests.post(partid_2_tids_url, data=json.dumps({"partids": part_ids}))
    content = json.loads(response.content)
    if content.get('ret', 0) != 1:
        raise RequestException
    data = content.get('data', [])
    return data


def get_tid_part_dict():
    tid_part_dict = dict()
    part2_info_map = get_all_subarea()
    for part2_id, part2_info in part2_info_map.items():
        part1_id = part2_info['part1_id']
        part_datas = get_part_tids([part2_id])
        for part_data in part_datas:
            for part2_id, tids in part_data.items():
                for tid in tids:
                    tid_part_dict[tid] = {'part2_id': part2_id, 'part1_id': part1_id}
    return tid_part_dict


def get_tid_part2_dict():
    tid_part2_dict = dict()
    part2_info_map = get_all_subarea()
    for part2_id, part2_info in part2_info_map.items():
        part_datas = get_part_tids([part2_id])
        for part_data in part_datas:
            for part2_id, tids in part_data.items():
                for tid in tids:
                    tid_part2_dict[tid] = part2_id
    return tid_part2_dict
