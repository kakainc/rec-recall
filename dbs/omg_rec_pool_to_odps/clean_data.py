# -*- coding: utf-8 -*-

import argparse
import codecs
import time
from utils import const
from utils import mongo_db


MAX_DAYS = 1800
POST_TIME = 30 * 86400


def gen_recpool_info(fout):
    recommend_pool_col = mongo_db.get_col_recommend_pool()
    col_newpost_step_boot = mongo_db.get_col_newpost_step_boot()

    start_ts = int(time.time())
    earliest_t = start_ts - MAX_DAYS * 86400
    cursor = recommend_pool_col.find()

    with codecs.open(fout, 'w', 'utf8') as file_handler:
        for doc in cursor:
            mid = doc.get('mid', 0)
            pid = doc.get('_id', 0)
            if pid <= 0:
                continue

            gid = doc.get('gid', 0)
            if gid <= 0:
                continue

            gid_ct = doc.get('gid_ct', 0)

            tid = doc.get('tid', 0)
            if tid in const.TEST_TIDS:
                continue

            ptype = doc.get('ptype', 0)
            if ptype <= 1:
                continue

            status = doc.get('status', 0)
            if status < 2:
                continue

            choice_status = doc.get('choice_status', 0)
            choice_types_list = doc.get('choice_types', [])
            choice_types = ",".join(map(str, choice_types_list))

            subarea_status = doc.get('subarea_status', 0)
            subarea_types_list = doc.get('subarea_types', [])
            subarea_types = ",".join(map(str, subarea_types_list))

            fans = doc.get('author', {}).get('fans', 0)
            
            atts = doc.get('author', {}).get('atts', 0)

            score_values = doc.get('score_values', {})
            ct = doc.get('ct', 0)
            rec = score_values.get('rec', 0)
            download = score_values.get('download', 0)
            like = score_values.get('like', 0)
            review = score_values.get('review', 0)
            share = score_values.get('share', 0)
            qscore = score_values.get('qscore', 0)
            real_sume = score_values.get('real_sume', 0)
            ctr = score_values.get('ctr', 0)
            play_rate = score_values.get('play_rate', 0)
            dtr = score_values.get('dtr', 0)
            detail = score_values.get('detail', 0)
            vdur = doc.get('vdur', 0)
            score_mix = doc.get('score_mix', 0.0)
            age_tgi = doc.get('age_tgi', 0.0)
            like_bait = doc.get('like_bait', 0)
            resource_status = doc.get('resource_status', 0)
            source = doc.get('source', '')

            is_vulgar = 0
            is_sexy = 0
            censor_tags = doc.get('censor_tags', [])
            for tag in censor_tags:
                if tag == 2:
                    is_vulgar = 1
                if tag == 1:
                    is_sexy = 1

            # 一月前并且曝光数小于1000的帖子过滤
            if start_ts - ct > POST_TIME and rec < 1000:
                continue
            # 过滤：超过四个月时间，并且不是精选池帖子
            if start_ts - ct > 120 * 86400 and choice_status < 30:
                continue

            if start_ts - ct > POST_TIME and (download / (rec + 10.0) < 0.003) and (like / (rec + 10.0) < 0.003):
                continue

            author = doc.get('author', {})
            mover_level, mover_part_id = 0, 0
            if author and isinstance(author, dict):
                mover_level = author.get('mover_level', 0)
                mover_part_id = author.get('mover_part_id', 0)

            community_tag_new = int(doc.get('community_tag_new', 0))
            community_tag_high = int(doc.get('community_tag_high', 0))
            community_pre_tag = int(doc.get('community_pre_tag', 0))
            community_tag_godrev = int(doc.get('community_tag_godrev', 0))

            step_boot = int(doc.get('step_boot', 0))
            step_boot_t = int(doc.get('step_boot_t', 0))

            if step_boot == 0 and ct >= 1595174400:
                step_boot_info = col_newpost_step_boot.find_one({'_id': pid})
                if step_boot_info:
                    step_boot = step_boot_info.get('step_boot', 0)
                    step_boot_t = step_boot_info.get('step_boot_t', 0)

            subarea_time = int(doc.get('subarea_time', 0))
            original = int(doc.get('original', 0))
            assist_status = int(doc.get('assist_status', 0))
            official_template_id = int(doc.get('official_template_id', 0))

            community_hashtag_post = int(doc.get('community_hashtag_post', 0))

            file_handler.write('\t'.join(map(unicode, [pid, mid, tid, ptype, gid, ct, rec, download, like, score_mix, gid_ct, fans, atts, is_vulgar, is_sexy, choice_status, choice_types, vdur, real_sume, ctr, play_rate, dtr, qscore, age_tgi, mover_level, mover_part_id, review, community_tag_new, community_tag_high, community_pre_tag, share, step_boot, step_boot_t, community_tag_godrev, subarea_status, subarea_types, like_bait, resource_status, subarea_time, detail, source, original, assist_status, official_template_id, community_hashtag_post])) + '\n')
    end_ts = int(time.time())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--rec_pool', type=str, required=True)
    args = parser.parse_args()
    gen_recpool_info(args.rec_pool)
