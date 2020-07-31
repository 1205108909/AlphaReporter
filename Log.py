#!/usr/bin/env python
# encoding: utf-8

"""
@author: zhangmeng
@contact: arws@qq.com
@file: Log.py
@time: 2018/7/12 16:34
"""
import logging
import datetime
import os


def get_logger(name):
    logger = logging.getLogger(name)

    ch = logging.StreamHandler()
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(threadName)s- %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)

    if not os.path.exists('log'):
        os.mkdir('log')
    fh = logging.FileHandler(os.path.join('log', datetime.datetime.now().strftime('%Y%m%d') + '.log'))
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s %(threadName)s- %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    logger.addHandler(ch)
    logger.addHandler(fh)
    return logger


if __name__ == '__main__':
    pass
