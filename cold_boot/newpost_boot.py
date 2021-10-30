# -*- coding: utf-8 -*-

import rule_engine

import time
import yaml
import json
import sys
import logging

from dbs import redis_db, mongo_db
from utils import topic_helper

logging.basicConfig(stream=sys.stdout, level=logging.INFO)
logger = logging.getLogger()

KEY = "index_newposts_boot"
WEIGHT_KEY = "index_newposts_boot_gender_weight"


def get_step_rules_conf(rule_path):
    with open(rule_path, encoding='utf-8') as f:
        step_rules_conf = yaml.safe_load(f)
        for rule_type, step_rules in step_rules_conf.items():
            for i, step_rule in enumerate(step_rules):
                rule = step_rule.get('rule', '')
                if rule:
                    step_rule['rule_engine'] = rule_engine.Rule(rule)
    return step_rules_conf


def step_cond(step_rules_conf, step, post_base_info, post_rt_info):
    pid = post_base_info['pid']
    ptype = post_base_info['ptype']
    vdur = post_base_info.get('vdur', 0)
    ct = post_base_info.get('ct', 0)
    step_boot = int(post_base_info.get('step_boot', 0))
    step_boot_t = int(post_base_info.get('step_boot_t', 0))
    mid = post_base_info['mid']
    is_mover = post_base_info['is_mover']
    mover_support = post_base_info['mover_support']
    part1_id = post_base_info['part1_id']
    part2_id = post_base_info['part2_id']

    rec = int(post_rt_info.get('rec', 0))
    sume = int(post_rt_info.get('sume', 0))
    gsume = int(post_rt_info.get('gsume', 0))
    download = int(post_rt_info.get('download', 0))
    like = int(post_rt_info.get('like', 0))
    share = int(post_rt_info.get('share', 0))
    review = int(post_rt_info.get('review', 0))
    detail = int(post_rt_info.get('detail', 0))

    ctr = sume*1.0/rec if rec else 0
    play_video = int(post_rt_info.get('play_video', 0))
    play_dur = int(post_rt_info.get('play_dur', 0))/1000.0

    avg_play_dur = 0
    play_rate = 0
    if ptype == 5 and play_video > 0:
        avg_play_dur = play_dur / play_video
        if vdur > 0:
            play_rate = avg_play_dur / vdur

    video_play_score = 0
    if ptype == 5 and avg_play_dur >= 10 and play_rate >= 0.2:
        video_play_score = 1
    elif ptype == 5 and avg_play_dur < 10 and play_rate < 0.2:
        video_play_score = -1

    download_rate = download / rec if rec else 0
    like_rate = like / rec if rec else 0

    rule_type = 'video' if ptype == 5 else 'img'

    post_rule_info = {
        'pid': pid,
        'rec': rec,
        'vdur': vdur,
        'sume': sume,
        'gsume': gsume,
        'download': download,
        'like': like,
        'share': share,
        'review': review,
        'detail': detail,

        'ctr': ctr,
        'play_rate': play_rate,
        'avg_play_dur': avg_play_dur,
        'video_play_score': video_play_score,

        'download_rate': download_rate,
        'like_rate': like_rate,

        't': int(time.time()),
        'ct': ct,
        'step_boot_t': step_boot_t,
        'step_boot': step_boot,

        'mid': mid,
        'is_mover': is_mover,
        'mover_support': mover_support,
        'part1_id': part1_id,
        'part2_id': part2_id,
    }

    if step >= len(step_rules_conf[rule_type]):
        # 阶梯到顶
        return False

    engine = step_rules_conf[rule_type][step].get('rule_engine', '')

    match = engine.matches(post_rule_info) if engine else True

    return match


def get_post_rt_info(pid, offline_redis):
    rt_key = 'fea_post_res_pre_{0}'.format(pid)
    post_rt_info = offline_redis.hgetall(rt_key)
    return post_rt_info


def get_gender_weight(post_rt_info, tid, ptype):
    s1_rec = int(post_rt_info.get('s1_rec', 0))
    s1_download = int(post_rt_info.get('s1_download', 0))
    s1_like = int(post_rt_info.get('s1_like', 0))
    s1_play_video = int(post_rt_info.get('s1_play_video', 0))
    s1_play_dur = int(post_rt_info.get('s1_play_dur', 0))
    s1_sume = int(post_rt_info.get('s1_sume', 0))

    s2_rec = int(post_rt_info.get('s2_rec', 0))
    s2_download = int(post_rt_info.get('s2_download', 0))
    s2_like = int(post_rt_info.get('s2_like', 0))
    s2_play_video = int(post_rt_info.get('s2_play_video', 0))
    s2_play_dur = int(post_rt_info.get('s2_play_dur', 0))
    s2_sume = int(post_rt_info.get('s2_sume', 0))

    s1_score = (s1_download + s1_like) * 1.0 / s1_rec if s1_rec else 0
    s2_score = (s2_download + s2_like) * 1.0 / s2_rec if s2_rec else 0

    if ptype == 5 or (s1_play_video > 0 or s2_play_video > 0):
        if s1_play_video > 0 and s2_play_video > 0:
            s1_score = s1_play_dur * 1.0 / s1_play_video
            s2_score = s2_play_dur * 1.0 / s2_play_video
    elif 0 < ptype < 5:
        if s1_rec > 0 and s2_rec > 0:
            s1_score = s1_sume * 1.0 / s1_rec
            s2_score = s2_sume * 1.0 / s2_rec

    s1_weight, s2_weight = 1.0, 1.0
    if s1_score and s2_score:
        s1_weight = '%.4f' % (s1_score / s2_score)
        s2_weight = '%.4f' % (s2_score / s1_score)
    elif s1_score > 0 and s2_score == 0:
        s1_weight = 1.2
    elif s1_score == 0 and s2_score > 0:
        s2_weight = 1.5
        s1_weight = 0.8 if s1_rec >= 20 else s1_weight

    return ':'.join(map(str, [s1_weight, s2_weight]))


def get_left_expose(pid, step, ptype, current_rec, step_rules_conf, col_recpool):
    """
    计算到达下一层级还需的曝光数
    :param pid:
    :param step:
    :param ptype:
    :param current_rec: 当前曝光数
    :param step_rules_conf: 层级信息
    :param col_recpool:
    :return: 到达下一层级还需的曝光数
    """

    step_rules = step_rules_conf['video'] if ptype == 5 else step_rules_conf['img']
    level = 0
    for i, step_rule in enumerate(step_rules):
        if current_rec >= step_rule['expose_to']:
            level = i + 1
        else:
            break
    if step >= level:
        left_expose = step_rules[step]['expose_to'] - max(current_rec, step_rules[step-1]['expose_to']) if step > 0 else step_rules[step]['expose_to'] - current_rec
    else:
        col_recpool.update_one({'_id': pid}, {"$set": {"step_boot": level * 10, "step_boot_t": int(time.time())}})
        col_newpost_step_boot.update_one({'_id': pid}, {"$set": {"step_boot": level * 10, "step_boot_t": int(time.time()), "real_step": step*10}}, upsert=True)
        logger.info('pid:{0},ptype:{1},level:{2},step_boot=level*10'.format(pid, ptype, level))
        left_expose = 0
    logger.info('pid:{0},ptype:{1},step:{2},level:{3},rec:{4},left_expose:{5}'.format(pid, ptype, step, level, current_rec, left_expose))
    return left_expose


def load_posts_from_recpool(step_rules_conf, col_recpool, offline_redis, col_newpost_step_boot, tid_part_map):
    unfinish = dict()
    rec_pids = set()
    pid_2_tid = dict()
    pid_2_ptype = dict()
    gender_weight = dict()

    now = int(time.time())
    start_time = now - 3 * 86400
    start_ct = now - 3 * 86400

    # 搬运工扶持帖子
    mover_support_pids = dict()

    docs = col_recpool.find({"ct": {"$gt": start_time}, "rec_t": {"$gt": start_ct}, 'status': {'$gte': 2}})
    for doc in docs:
        pid = int(doc.get('_id', 0))
        ct = int(doc.get('ct', 0))
        rec_t = int(doc.get('rec_t', 0))
        rec = doc.get('score_values', {}).get('rec', 0)
        tid = doc.get('tid', 0)
        ptype = doc.get('ptype', 0)
        gid = doc.get('gid', 0)
        source = doc.get('source', '')
        step_boot = doc.get('step_boot', 0)
        step_boot_t = doc.get('step_boot_t', 0)
        vdur = doc.get('vdur', 0)
        mid = doc.get('mid', 0)
        mover_support = doc.get('mover_support', 0)

        author = doc.get('author', {})
        mover_level = author.get('mover_level', 0)
        mover_part_id = author.get('mover_part_id', 0)

        part_id = tid_part_map.get(tid, {})
        part1_id, part2_id = 0, 0
        if part_id:
            part1_id = part_id['part1_id']
            part2_id = part_id['part2_id']

        if rec_t - ct > 3600:
            step_boot_info = col_newpost_step_boot.find_one({'_id': pid})
            if step_boot_info:
                step_boot = step_boot_info.get('step_boot', 0)
                step_boot_t = step_boot_info.get('step_boot_t', 0)
                mover_support = step_boot_info.get('mover_support', 0)

        # 资源型帖子过滤
        if int(part2_id) == 80:
            continue

        # 无意义拍摄过滤
        resource_status = doc.get('resource_status', 0)
        if resource_status == -20:
            continue

        subarea_status = doc.get('subarea_status', 0)
        subarea_types = doc.get('subarea_types', [])

        if subarea_status > 0 and len(subarea_types) > 0 and int(step_boot % 10) == 0:
            continue

        if not gid:
            continue

        # 高曝光同gid帖子不再分发
        similar_posts_expose = []
        same_gids = col_recpool.find({'gid': gid})
        for item in same_gids:
            same_pid = item['_id']
            if pid != same_pid:
                same_post_expose = item.get('score_values', {}).get('rec', 0)
                similar_posts_expose.append(same_post_expose)
        if similar_posts_expose and max(similar_posts_expose) >= 10000 and step_boot >= 10:
            continue

        rec_pids.add(pid)
        pid_2_tid[pid] = tid
        pid_2_ptype[pid] = ptype

        # 搬运工排除pop\game\carton
        is_mover = 1 if mover_level > 0 and mover_part_id not in (58, 59, 145, 148, 51, 77, 81, 82, 83, 107, 110, 111, 149, 56, 87, 88) else 0

        post_base_info = {
            'pid': pid,
            'ptype': ptype,
            'ct': ct,
            'rec_t': rec_t,
            'tid': tid,
            'gid': gid,
            'vdur': vdur,
            'source': source,
            'step_boot': step_boot,
            'step_boot_t': step_boot_t,
            'rec': rec,
            'mid': mid,
            'is_mover': is_mover,
            'mover_support': mover_support,
            'part1_id': part1_id,
            'part2_id': part2_id,
        }

        if step_boot % 10 == 0:  # 一个保量阶段结束
            post_rt_info = get_post_rt_info(pid, offline_redis)
            step = int(step_boot / 10)
            res = step_cond(step_rules_conf, step, post_base_info, post_rt_info)
            if res:
                current_rec = max(int(post_rt_info.get('rec', 0)), post_base_info.get('rec', 0))
                left_expose = get_left_expose(pid, step, ptype, current_rec, step_rules_conf, col_recpool)
                if left_expose > 0:
                    unfinish[pid] = left_expose
                    gender_weight[pid] = get_gender_weight(post_rt_info, tid, ptype)

            # 搬运工扶持
            else:
                if is_mover:
                    if (step == 1 and 7800 < now-step_boot_t <= 8100) or (step == 2 and now-step_boot_t > 43200 and mover_support == 0):
                        current_rec = max(int(post_rt_info.get('rec', 0)), post_base_info.get('rec', 0))
                        left_expose = get_left_expose(pid, step, ptype, current_rec, step_rules_conf, col_recpool)
                        if left_expose > 0:
                            unfinish[pid] = left_expose
                            gender_weight[pid] = get_gender_weight(post_rt_info, tid, ptype)
                            mover_support_pids[pid] = step

    return unfinish, rec_pids, gender_weight, pid_2_tid, pid_2_ptype, mover_support_pids


def load_posts_from_redis(index_redis):
    finish, unfinish = set(), set()
    posts = index_redis.hgetall(KEY)
    if not posts:
        return finish, unfinish

    for pid, cnt in posts.items():
        pid = int(pid)
        cnt = int(cnt)
        if cnt <= 0:
            finish.add(pid)
        else:
            unfinish.add(pid)

    return finish, unfinish


def add_to_redis(data, index_redis, col_recpool, col_newpost_step_boot, gender_weight, mover_support_pids):
    if data:
        index_redis.hset(KEY, mapping=data)
        index_redis.hset(WEIGHT_KEY, mapping=gender_weight)
    for pid in data.keys():
        logger.info('pid:{0},mg step_boot inc 5'.format(pid))
        if pid in mover_support_pids.keys():
            col_recpool.update_one({'_id': pid}, {"$inc": {"step_boot": 5}, '$set': {'mover_support': mover_support_pids[pid]}})
            col_newpost_step_boot.update_one({'_id': pid}, {"$inc": {"step_boot": 5}, '$set': {'mover_support': mover_support_pids[pid]}}, upsert=True)
        else:
            col_recpool.update_one({'_id': pid}, {"$inc": {"step_boot": 5}})
            col_newpost_step_boot.update_one({'_id': pid}, {"$inc": {"step_boot": 5}}, upsert=True)

    total_cnt = index_redis.hlen(KEY)
    logger.info("add to redis: %s" % json.dumps(data))
    logger.info("total cnt add to redis: %d" % total_cnt)


if __name__ == "__main__":
    logger.info("build start")
    t = int(time.time())

    index_redis = redis_db.get_index_redis()
    offline_redis = redis_db.get_offline_redis()
    col_recpool = mongo_db.get_col_recommend_pool()
    col_newpost_step_boot = mongo_db.get_col_newpost_step_boot()

    rule_path = './step_rule.yaml'
    step_rules_conf = get_step_rules_conf(rule_path)
    tid_part_map = topic_helper.get_tid_part_dict()

    # 查看redis中给予帖子的曝光是否使用完
    finish1, unfinish1 = load_posts_from_redis(index_redis)
    unfinish2, all_pids, gender_weight, pid_2_tid, pid_2_ptype, mover_support_pids = load_posts_from_recpool(step_rules_conf, col_recpool, offline_redis, col_newpost_step_boot, tid_part_map)

    for pid in unfinish1:
        post_rt_info = get_post_rt_info(pid, offline_redis)
        post = col_recpool.find_one({'_id': pid})
        if not post:
            continue
        step_boot = post.get('step_boot', 0)
        ptype = post.get('ptype', 2)
        step = int(step_boot / 10)
        step_rules = step_rules_conf['video'] if ptype == 5 else step_rules_conf['img']

        left_expose = step_rules[step]['expose_to'] - max(int(post_rt_info.get('rec', 0)), post.get('score_values', {}).get('rec', 0))
        if left_expose <= 0:
            finish1.add(pid)
            continue
        tid = pid_2_tid.get(pid, 0)
        ptype = pid_2_ptype.get(pid, 0)
        if pid in all_pids:
            gender_weight[pid] = get_gender_weight(post_rt_info, tid, ptype)

    finish = finish1 | (unfinish1 - all_pids)
    for pid in finish:
        index_redis.hdel(KEY, pid)
        index_redis.hdel(WEIGHT_KEY, pid)
        post = col_recpool.find_one({'_id': pid})
        if not post:
            continue
        step_boot = post.get('step_boot', 0)
        logger.info('pid:{0},step_boot:{1},redis remove'.format(pid, step_boot))
        if step_boot % 10 == 5:
            logger.info('pid:{0},step_boot:{1},mg set step_boot:+5'.format(pid, step_boot))
            col_recpool.update_one({'_id': pid}, {"$inc": {"step_boot": 5}, "$set": {"step_boot_t": t}})
            col_newpost_step_boot.update_one({'_id': pid}, {"$inc": {"step_boot": 5}, "$set": {"step_boot_t": t}}, upsert=True)

    add_to_redis(unfinish2, index_redis, col_recpool, col_newpost_step_boot, gender_weight, mover_support_pids)

    logger.info("build finish")
