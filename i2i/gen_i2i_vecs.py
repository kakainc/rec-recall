# -*- coding: utf-8 -*-
import argparse
import time
import gensim

from gensim.similarities.annoy import AnnoyIndexer
from concurrent.futures import ThreadPoolExecutor, as_completed, wait, FIRST_COMPLETED, ALL_COMPLETED

from dbs import mongo_db

POST_TIME = 30 * 86400

POST_PTYPE_TID = {}


def post_valid_ptype_tid(pid, col_recpool):
    post = col_recpool.find_one({'_id': pid})
    if not post:
        return False, None, None

    status = post.get('status', 0)
    if status <= 0:
        return False, None, None

    ptype = post.get('ptype', 0)
    tid = post.get('tid', 0)
    if ptype <= 0 or tid <= 0:
        return False, None, None

    ct = post.get('ct', 0)

    score_values = post.get('score_values', {})
    rec = score_values.get('rec', 0)

    # 一月前并且曝光数小于1000的帖子过滤
    t = int(time.time())
    if t - ct > POST_TIME and rec < 1000:
        return False, None, None

    return True, ptype, tid


# 0：未知； -1：不存在；
def get_post_ptype_tid(pid, col_recpool):
    ptype, tid = POST_PTYPE_TID.get(pid, (0, 0))
    if ptype == 0 or tid == 0:
        valid, ptype, tid = post_valid_ptype_tid(pid, col_recpool)
        if not valid:
            POST_PTYPE_TID[pid] = (-1, -1)
            return -1, -1
        else:
            POST_PTYPE_TID[pid] = (ptype, tid)
            return ptype, tid
    else:
        return ptype, tid


def load_model(model_path, mtype='word2vec', binary=True):
    if mtype == 'word2vec':
        model = gensim.models.Word2Vec.load(model_path)
    else:
        model = gensim.models.KeyedVectors.load_word2vec_format(model_path, binary=binary)
    return model


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('model_path')
    parser.add_argument('video_output_path')
    parser.add_argument('imgtxt_output_path')
    parser.add_argument('post_ptype_tid_file_path')
    parser.add_argument('--mtype', type=str, default='word2vec')
    parser.add_argument('--binary', type=bool, default=False)
    args = parser.parse_args()
    return args


def get_nearest_pids(pid, model, annoy_index, col_recpool):
    nearest_pids = []
    ptype, tid = get_post_ptype_tid(long(pid), col_recpool)
    if ptype <= 0:
        return pid, ptype, tid, nearest_pids

    nearest_list = model.wv.most_similar(pid, topn=200, indexer=annoy_index)
    cnt = 0

    if ptype == 5:
        for near_pid, v in nearest_list:
            if not near_pid.isdigit() or long(near_pid) == long(pid):
                continue
            if v < 0.5:
                break

            near_pid_ptype, near_pid_tid = get_post_ptype_tid(long(near_pid), col_recpool)
            if near_pid_ptype != 5:
                continue
            score = round(v, 2)
            nearest_pids.append((near_pid, score))
            cnt += 1
            if cnt >= 200:
                break
    else:
        for near_pid, v in nearest_list:
            if not near_pid.isdigit() or int(near_pid) == int(pid):
                continue
            if v < 0.5:
                break
            near_pid_ptype, near_pid_tid = get_post_ptype_tid(long(near_pid), col_recpool)
            if near_pid_ptype < 2 or near_pid_ptype > 4:
                continue
            score = round(v, 2)
            nearest_pids.append((near_pid, score))
            cnt += 1
            if cnt >= 200:
                break
    return pid, ptype, tid, nearest_pids


def save_post_ptype_tid(post_ptype_tid_file_path):
    with open(post_ptype_tid_file_path, 'w') as f:
        for pid, ptype_tid in POST_PTYPE_TID.items():
            ptype, tid = ptype_tid
            if ptype <= 0 or tid <= 0:
                continue
            f.write(','.join(map(str, [pid, ptype, tid])) + '\n')


if __name__ == '__main__':
    args = get_args()
    model_path = args.model_path
    video_output_path = args.video_output_path
    imgtxt_output_path = args.imgtxt_output_path
    mtype = args.mtype
    binary = args.binary
    post_ptype_tid_file_path = args.post_ptype_tid_file_path

    col_recpool = mongo_db.get_col_recommend_pool()

    model = load_model(model_path, mtype, binary)
    annoy_index = AnnoyIndexer(model, 150)
    print("finish build annoy_index")

    with open(video_output_path, 'w') as video_handler, open(imgtxt_output_path, 'w') as img_handler:
        all_task = []
        with ThreadPoolExecutor(10) as executor:
            for pid in model.wv.vocab.keys():
                if not pid.isdigit():
                    continue

                all_task.append(executor.submit(get_nearest_pids, pid, model, annoy_index, col_recpool))

            wait(all_task, return_when=ALL_COMPLETED)
            for future in as_completed(all_task):
                pid, ptype, tid, nearest_pids = future.result()
                if nearest_pids:
                    score_nearest_pids = sorted(nearest_pids, key=lambda x: x[1], reverse=True)
                    y = ','.join(map(lambda x: ':'.join(map(str, [x[0], x[1]])), score_nearest_pids))
                    if ptype == 5:
                        video_handler.write(' '.join(map(str, [pid, y])) + '\n')
                    else:
                        img_handler.write(' '.join(map(str, [pid, y])) + '\n')

    save_post_ptype_tid(post_ptype_tid_file_path)
