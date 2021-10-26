# -*- coding: utf-8 -*-

import argparse

from gensim.models import word2vec

# random.shuffle(data)

SIZE = 100
WINDOW = 10
MINCOUNT = 5
SAMPLE = 0.00001
SG = 1
NEGATIVE = 5
HS = 1
ITER = 10


def train(input_path, model_path, old_model_path,
          size=100, window=5, min_count=5, sample=1e-3, workers=3, sg=0, negative=5, hs=0, epoch=5, ns_exponent=0.75):
    sentence = word2vec.LineSentence(input_path)
    if old_model_path and old_model_path != 'NULL':
        model = word2vec.Word2Vec.load(old_model_path)  # 加载旧模型
        model.build_vocab(sentence, update=True)  # 更新词汇表
        model.train(sentences=sentence, total_examples=model.corpus_count, epochs=model.epochs)
    else:
        model = word2vec.Word2Vec(sentences=sentence, vector_size=size, window=window, workers=workers,
                                  min_count=min_count,
                                  sg=sg, sample=sample, negative=negative, hs=hs, epochs=epoch, ns_exponent=ns_exponent)
    model.save(model_path)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('sequence_path')
    parser.add_argument('model_path')
    parser.add_argument('old_model_path')
    parser.add_argument('sg', type=int, choices=[0, 1], help='Training algorithm: 1 for skip-gram; otherwise CBOW.')
    parser.add_argument('size', type=int, default=100)
    parser.add_argument('window', type=int, default=5)
    parser.add_argument('workers', type=int, default=12)
    parser.add_argument('min_count', type=int, default=5)
    parser.add_argument('negative', type=int, default=5)
    parser.add_argument('sample', type=float, default=1e-5)
    parser.add_argument('hs', type=int, default=0)
    parser.add_argument('iter', type=int, default=5)
    parser.add_argument('--ns_exponent', type=float, default=0.75)
    args = parser.parse_args()

    train(args.sequence_path, args.model_path, args.old_model_path, args.size, args.window, args.min_count, args.sample,
          args.workers,
          args.sg, args.negative, args.hs, args.iter, args.ns_exponent)
