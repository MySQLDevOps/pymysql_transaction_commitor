import argparse
import logging
import os
import sys
import time
from multiprocessing import Pool
from pymysql_transaction_commitor.hongbao_manager import HongbaoManager
from pymysql_transaction_commitor.my_util import init_logger
from argparse import RawTextHelpFormatter

def parse_args():
    """parse args for mysql transaction commitor"""

    parser = argparse.ArgumentParser(description='''
    Hongbao transaction commitor.
    
    use examples:
    # create users and groups use 1 threads with debug logging level.
    python hongbao_commitor.py -e prepare -c 1 -l 10
    
    # running create hongbao use 50 threads send 5 hongbaos per user by 1000 users
    python hongbao_commitor.py -e run  -c 50 -U 1000 -b 5
    
    # drop database
    python hongbao_commitor.py -e cleanup
    ''', formatter_class=RawTextHelpFormatter, add_help=False)

    parser.add_argument("-e", "--method", type=str, dest='method',
                        help='The methods of: prepare,run,cleanup')
    parser.add_argument("-c", "--threads", type=int, default=1, dest='threads',
                        help='How many threads will be running.')
    parser.add_argument("-l", '--level', type=int, dest='log_level', default=logging.ERROR,
                        help='logging level: CRITICAL = 50, ERROR = 40, WARNING = 30, INFO = 20, DEBUG = 10 ')
    parser.add_argument('-h', '--help', dest='help', action='store_true', help='help information', default=False)
    prepare_setting = parser.add_argument_group('Prepare setting')
    prepare_setting.add_argument('-u', '--users', dest='users', type=int,
                                 help='How many users will be created.', default=1000)
    prepare_setting.add_argument('-f', '--friends', dest='friends', type=int,
                                 help='How many friends each user will be created.', default=50)
    prepare_setting.add_argument('-g', '--groups', dest='groups', type=int,
                                 help='How many groups each user will be created.', default=5)
    prepare_setting.add_argument('-m', '--members', dest='members', type=int,
                                 help='How many members each group will be created.', default=100)

    running_setting = parser.add_argument_group('Running setting')
    running_setting.add_argument('-U', '--sending_users', dest='sending_users', type=int,
                                 help='How many users will be created.', default=100)
    running_setting.add_argument('-b', '--hongbaos', dest='hongbaos', type=int,
                                 help='How many friends each user will be created.', default=5)

    return parser


def command_line_args(args):
    need_print_help = False if args else True
    parser = parse_args()
    args = parser.parse_args(args)
    if args.help or need_print_help:
        parser.print_help()
        sys.exit(1)
    if not args.method:
        raise ValueError('The method must be specified and not empty.')
    return args


def create_users_task(name, level, users=5, friends=20, groups=2, groups_members=30):
    start = time.time()
    log_name = os.path.basename(__file__).replace(".py", "") + "_" + str(name)
    logger = init_logger(log_name=log_name, level=level)
    logger.info('Run task %s (%s)...' % (name, os.getpid()))
    pm = HongbaoManager(logger=logger)
    pm.create_users(users=users, friends=friends, groups=groups, groups_members=groups_members)
    end = time.time()
    logger.info('Task %s runs %0.2f seconds.' % (name, (end - start)))


def create_hongbaos(name, level, sending_users=100, hongbaos=5):
    start = time.time()
    log_name = os.path.basename(__file__).replace(".py", "") + "_" + str(name)
    logger = init_logger(log_name=log_name, level=level)
    logger.info('Run task %s (%s)...' % (name, os.getpid()))
    pm = HongbaoManager(logger=logger)
    pm.create_hongbaos(sending_users=sending_users, hongbaos=hongbaos)
    end = time.time()
    logger.info('Task %s runs %0.2f seconds.' % (name, (end - start)))


if __name__ == '__main__':
    args = command_line_args(sys.argv[1:])
    method = args.method
    max_threads = args.threads
    log_level = args.log_level

    print('Parent process %s.' % os.getpid())
    p = Pool(max_threads)
    for i in range(max_threads):
        if method == "prepare":
            p.apply_async(create_users_task, args=(i, log_level, args.users, args.friends, args.groups, args.members))
        elif method == "run":
            p.apply_async(create_users_task, args=(i, log_level, args.sending_users, args.hongbaos,))

    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
