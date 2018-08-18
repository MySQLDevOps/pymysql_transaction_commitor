#!/usr/bin/env python
# coding=utf-8
import random
import sys
import os
import argparse
import time
import numpy as np
import logging
import logging.handlers


def init_logger(log_name=None, level=logging.INFO, logger=logging.getLogger()):
    """初始化logger"""
    log_dir, name = os.path.split(os.path.abspath(sys.argv[0]))
    if log_name:
        name = log_name

    log_filename = os.path.dirname(log_dir) + '/log/' + name.replace(".py", "") + '.log'
    fmt = logging.Formatter('%(asctime)s %(levelname)s [%(process)d] %(filename)s %(message)s ')

    if not os.path.isdir(os.path.dirname(log_filename)):
        os.makedirs(os.path.dirname(log_filename))

    # 单文件最大10M
    file_handler = logging.handlers.RotatingFileHandler(log_filename, mode='a', maxBytes=10240000, backupCount=100, encoding="utf8")
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(fmt)
    logger.addHandler(stdout_handler)
    logger.setLevel(level)

    return logger


def random_uname():
    """随机产生用户名称"""
    return "".join(np.random.choice(list("abceefg"), 3)) + str(random.randint(0, 9000000000))


def random_gname():
    """随机产生群名称"""
    return "".join(np.random.choice(list("ABCDEFG"), 3)) + str(random.randint(0, 9000000000))


def random_birth_day():
    """随机产生生日"""
    start = time.mktime((1976, 1, 1, 0, 0, 0, 0, 0, 0))
    end = time.mktime((2017, 12, 31, 23, 59, 59, 0, 0, 0))
    t = random.randint(start, end)
    return time.strftime("%Y-%m-%d", time.localtime(t))


def random_addr_city():
    """随机城市id"""
    return random.randint(1, 340)


def random_addr_province():
    """随机省份id"""
    return random.randint(1, 34)


def parse_args():
    """parse args for PyMySQLConsistencyTesting"""

    parser = argparse.ArgumentParser(description='MySQL consistency testing', add_help=False)
    connect_setting = parser.add_argument_group('connect setting')
    connect_setting.add_argument('-h', '--host', dest='host', type=str,
                                 help='Host the MySQL database server located', default='127.0.0.1')
    connect_setting.add_argument('-u', '--user', dest='user', type=str,
                                 help='MySQL Username to log in as', default='root')
    connect_setting.add_argument('-p', '--password', dest='password', type=str,
                                 help='MySQL Password to use', default='')
    connect_setting.add_argument('-P', '--port', dest='port', type=int,
                                 help='MySQL port to use', default=3306)
    connect_setting.add_argument('-d', '--db', dest='db', type=str,
                                 help='MySQL database to use', default='')
    connect_setting.add_argument('--help', dest='help', action='store_true',
                                 help='help information', default=False)

    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.host:
        raise ValueError('MySQL host must be specified and not empty.')
    return args
