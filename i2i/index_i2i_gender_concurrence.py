#!/usr/bin/env python
# -*- coding:utf-8 -*-

import sys
import math

from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED, ALL_COMPLETED

from dbs import redis_db
from dbs import mongo_db
from utils import topic_helper


def get_post_standard():
    col_post_standard = mongo_db.get_col_post_standard()
    docs = col_post_standard.find()
    post_standard = dict()
    for doc in docs:
        pcnt = doc.get('pcnt', 0)
        if pcnt < 10:
            continue
        post_standard[doc['_id']] = doc
    return post_standard


def meet_post_standard(part2_id, ptype, post_standard, download_rate, like_rate):
    if ptype == 5:
        key = '_'.join(map(str, ['video', part2_id]))
    else:
        key = 'img'

    standard = post_standard.get(key, {}).get('gender_standard', {}).get('a', {})
    if not standard:
        download_rate_standard = 9.0
        like_rate_standard = 9.0
    else:
        download_rate_standard = min(math.floor(standard.get('download_rate', 9.0)), 20.0)
        like_rate_standard = min(math.floor(standard.get('like_rate', 9.0)), 20.0)

    if (download_rate >= download_rate_standard and like_rate >= like_rate_standard) or (
            download_rate >= 9.0 and like_rate >= 9.0) or (ptype != 5 and download_rate >= 9.0):
        return True
    else:
        return False


def get_post_type_tid(post_ptype_tid_file_path):
    post_ptype_tid = dict()
    with open(post_ptype_tid_file_path, 'r') as f:
        for line in f:
            info = line.strip('\n').split(',')
            if len(info) != 3:
                continue
            pid, ptype, tid = map(int, info)
            post_ptype_tid[pid] = (ptype, tid)
    return post_ptype_tid


def get_post_rec_info(pid):
    key = 'fea_post_res_pre_{0}'.format(pid)
    offline_redis = redis_db.get_offline_redis()
    info = offline_redis.hgetall(key)
    if not info:
        return pid, {}

    gender_post_rec_info = {
        'gender0': {
            'rec': int(info.get('s0_rec', 0)),
            'sume': int(info.get('s0_sume', 0)),
            'gsume': int(info.get('s0_gsume', 0)),
            'detail': int(info.get('s0_detail', 0)),
            'share': int(info.get('s0_share', 0)),
            'like': int(info.get('s0_like', 0)),
            'download': int(info.get('s0_download', 0)),
            'review': int(info.get('s0_review', 0)),
            'play_video': int(info.get('s0_play_video', 0)),
            'play_dur': int(info.get('s0_play_dur', 0)) / 1000,
        },
        'gender1': {
            'rec': int(info.get('s1_rec', 0)),
            'sume': int(info.get('s1_sume', 0)),
            'gsume': int(info.get('s1_gsume', 0)),
            'detail': int(info.get('s1_detail', 0)),
            'share': int(info.get('s1_share', 0)),
            'like': int(info.get('s1_like', 0)),
            'download': int(info.get('s1_download', 0)),
            'review': int(info.get('s1_review', 0)),
            'play_video': int(info.get('s1_play_video', 0)),
            'play_dur': int(info.get('s1_play_dur', 0)) / 1000,
        },
        'gender2': {
            'rec': int(info.get('s2_rec', 0)),
            'sume': int(info.get('s2_sume', 0)),
            'gsume': int(info.get('s2_gsume', 0)),
            'detail': int(info.get('s2_detail', 0)),
            'share': int(info.get('s2_share', 0)),
            'like': int(info.get('s2_like', 0)),
            'download': int(info.get('s2_download', 0)),
            'review': int(info.get('s2_review', 0)),
            'play_video': int(info.get('s2_play_video', 0)),
            'play_dur': int(info.get('s2_play_dur', 0)) / 1000,
        }
    }

    post_rec_info = {
        'rec': int(info.get('rec', 0)),
        'sume': int(info.get('sume', 0)),
        'gsume': int(info.get('gsume', 0)),
        'detail': int(info.get('detail', 0)),
        'share': int(info.get('share', 0)),
        'like': int(info.get('like', 0)),
        'download': int(info.get('download', 0)),
        'review': int(info.get('review', 0)),
        'play_video': int(info.get('play_video', 0)),
        'play_dur': int(info.get('play_dur', 0)) / 1000,
        'gender': gender_post_rec_info
    }

    return pid, post_rec_info


def get_all_features(file_path):
    pids = {}
    with open(file_path, 'r') as file_handler:
        for line in file_handler:
            info = line.strip().split('\x01')
            if len(info) == 1:
                info = line.strip().split(' ')
            if len(info) != 2:
                continue

            pid1 = long(info[0])
            pids[pid1] = 1
            value = info[1]
            temp_list = value.split(',')
            for item in temp_list:
                pid2 = long(item.split(':')[0])
                pids[pid2] = 1
    pid2features = {}

    all_task = []
    with ThreadPoolExecutor(5) as executor:
        for pid, cnt in pids.items():
            all_task.append(executor.submit(get_post_rec_info, pid))

        wait(all_task, return_when=ALL_COMPLETED)
        for future in as_completed(all_task):
            pid, feature = future.result()
            pid2features[pid] = feature

    return pid2features


def update_to_redis(file_path, pid2features, prefix, post_ptype_tid, tid_part2_map, post_standard, ptype='video'):
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
            # if len(temp_list) > 1000:
            #     temp_list = temp_list[:1000]
            g_all_list = []
            g1_list = []
            g2_list = []
            for item in temp_list:
                pid1 = long(item.split(':')[0])
                tid1 = post_ptype_tid.get(pid1, (0, 0))[1]
                part2_id = tid_part2_map.get(tid1, 72)

                g_all_feature = pid2features.get(pid1, {})
                if not g_all_feature:
                    continue
                g1_feature = pid2features.get(pid1, {}).get('gender', {}).get('gender1', {})
                g2_feature = pid2features.get(pid1, {}).get('gender', {}).get('gender2', {})

                p_type = 5 if ptype == 'video' else 2
                rec = g_all_feature['rec']
                download = g_all_feature['download']
                like = g_all_feature['like']
                download_rate = 1000 * download / (1.0 + rec)
                like_rate = 1000 * like / (1.0 + rec)
                meet = meet_post_standard(part2_id, p_type, post_standard, download_rate, like_rate)

                if g_all_feature:
                    rec = g_all_feature['rec']
                    sume = g_all_feature['sume']
                    download = g_all_feature['download']
                    like = g_all_feature['like']
                    play_video = g_all_feature['play_video']
                    play_dur = g_all_feature['play_dur']
                    ctr = sume / (rec + 1.0)
                    download_rate = 1000 * download / (1.0 + rec)
                    like_rate = 1000 * like / (1.0 + rec)
                    avg_play_dur = play_dur * 1.0 / play_video if play_video > 0 else 0

                    if ptype == 'video' and ctr > 0.35 and (
                            (download_rate > 3 and like_rate >= 5) or (like_rate >= 9 and avg_play_dur >= 15) or meet):
                        g_all_list.append(item)
                    if ptype == 'img' and ctr > 0.25 and ((download_rate > 3 and like_rate >= 5) or meet):
                        g_all_list.append(item)
                if g1_feature:
                    rec = g1_feature['rec']
                    sume = g1_feature['sume']
                    download = g1_feature['download']
                    like = g1_feature['like']
                    play_video = g1_feature['play_video']
                    play_dur = g1_feature['play_dur']
                    ctr = sume / (rec + 1.0)
                    download_rate = 1000 * download / (1.0 + rec)
                    like_rate = 1000 * like / (1.0 + rec)
                    avg_play_dur = play_dur * 1.0 / play_video if play_video > 0 else 0
                    if ptype == 'video' and ctr > 0.35 and (
                            (download_rate > 3 and like_rate >= 5) or (like_rate >= 9 and avg_play_dur >= 15) or meet):
                        g1_list.append(item)
                    if ptype == 'img' and ctr > 0.25 and ((download_rate > 2.5 and like_rate >= 5) or meet):
                        g1_list.append(item)
                if g2_feature:
                    rec = g2_feature['rec']
                    sume = g2_feature['sume']
                    download = g2_feature['download']
                    like = g2_feature['like']
                    play_video = g1_feature['play_video']
                    play_dur = g1_feature['play_dur']
                    ctr = sume / (rec + 1.0)
                    download_rate = 1000 * download / (1.0 + rec)
                    like_rate = 1000 * like / (1.0 + rec)
                    avg_play_dur = play_dur * 1.0 / play_video if play_video > 0 else 0
                    if ptype == 'video' and ctr > 0.32 and (
                            (download_rate > 3 and like_rate >= 5) or (like_rate >= 9 and avg_play_dur >= 15) or meet):
                        g2_list.append(item)
                    if ptype == 'img' and ctr > 0.25 and ((download_rate > 2.5 and like_rate >= 5) or meet):
                        g2_list.append(item)

            value = ','.join(g_all_list[:100])
            value1 = ','.join(g1_list[:100])
            value2 = ','.join(g2_list[:100])

            key = 'g0_' + prefix + info[0]
            key1 = 'g1_' + prefix + info[0]
            key2 = 'g2_' + prefix + info[0]

            with cf_redis.pipeline(transaction=False) as p:
                p.setex(key, 2 * 86400, value).setex(key1, 2 * 86400, value1).setex(key2, 2 * 86400, value2)
                p.execute()


if __name__ == "__main__":
    args = sys.argv
    if len(args) < 3:
        print("lack args")
        exit(1)

    file_path = args[1]
    prefix = args[2]
    ptype = args[3]
    post_ptype_tid_file_path = args[4]

    tid_part2_map = topic_helper.get_tid_part2_dict()
    post_standard = get_post_standard()

    pid2features = get_all_features(file_path)
    print(len(pid2features))
    post_ptype_tid = get_post_type_tid(post_ptype_tid_file_path)
    update_to_redis(file_path, pid2features, prefix, post_ptype_tid, tid_part2_map, post_standard, ptype)

    print("i2i build finish")
