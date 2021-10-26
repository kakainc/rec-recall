# -*- coding: utf-8 -*-

import time

from dbs import mongo_db, redis_db
from utils import topic_helper


def get_post_rt_info(redis_offline, pid):
    key = 'fea_post_{0}'.format(pid)
    post_info = redis_offline.hgetall(key)
    return post_info


if __name__ == '__main__':
    now = int(time.time())
    t = now - 7 * 86400

    tid_part_dict = topic_helper.get_tid_part_dict()

    offline_redis = redis_db.get_offline_redis()
    col_recpool = mongo_db.get_col_recommend_pool()
    docs = col_recpool.find({'status': 2, 'ct': {'$gte': t}})

    part2_video_posts = dict()
    part2_img_posts = dict()

    part2_ids = set()
    for tid, part in tid_part_dict.items():
        part2_id = part['part2_id']
        part2_ids.add(part2_id)

    cnt = 0
    for doc in docs:
        community_tag_new = doc.get('community_tag', 0)

        if community_tag_new == 1040 or community_tag_new == 1050:

            pid = doc['_id']
            rt_info = get_post_rt_info(offline_redis, pid)
            rec = int(rt_info.get('rec', 0))
            review = int(rt_info.get('review', 0))

            if review >= 20 or rec >= 10000:
                continue

            ct = doc['ct']
            ptype = doc['ptype']
            tid = doc['tid']

            review_rate = score = float('%.4f' % (review * 1.0 / rec)) if rec > 0 else 0

            if tid in tid_part_dict:
                part2_id = tid_part_dict[tid]['part2_id']
            else:
                part2_id = 72

            cnt += 1
            if ptype == 5:
                part2_video_posts.setdefault(part2_id, [])
                part2_video_posts[part2_id].append((pid, review_rate))
            else:
                part2_img_posts.setdefault(part2_id, [])
                part2_img_posts[part2_id].append((pid, review_rate))

    index_redis = redis_db.get_index_redis()

    key_name_prefix = 'index_part2_community_review_video_'

    for part2_id in part2_ids:
        posts = part2_video_posts.get(part2_id, [])
        posts = sorted(posts, key=lambda x: -x[1])
        pid_score = ','.join([':'.join(map(str, [i[0], i[1]])) for i in posts])
        key_name = key_name_prefix + str(part2_id)
        index_redis.setex(key_name, 3 * 86400, pid_score)

    key_name_prefix = 'index_part2_community_review_img_'
    for part2_id in part2_ids:
        posts = part2_img_posts.get(part2_id, [])
        posts = sorted(posts, key=lambda x: -x[1])
        pid_score = ','.join([':'.join(map(str, [i[0], i[1]])) for i in posts])
        key_name = key_name_prefix + str(part2_id)
        index_redis.setex(key_name, 3 * 86400, pid_score)

    print('community_post_cnt:{0}'.format(cnt))
