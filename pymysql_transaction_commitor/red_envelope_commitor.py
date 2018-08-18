import logging
import os
import time
from multiprocessing import Pool
from pymysql_transaction_commitor.red_envelope_manager import MyManager, MySQLHelper
from pymysql_transaction_commitor.red_envelope_util import init_logger


def create_users_task(name, level):
    start = time.time()
    log_name = os.path.basename(__file__).replace(".py", "") + "_" + str(name)
    logger = init_logger(log_name=log_name, level=level)
    logger.info('Run task %s (%s)...' % (name, os.getpid()))
    pm = MyManager(MySQLHelper(logger=logger))
    pm.create_users(users=5, friends=20, groups=2, groups_members=30)
    end = time.time()
    logger.info('Task %s runs %0.2f seconds.' % (name, (end - start)))


if __name__ == '__main__':
    max_threads = 2
    print('Parent process %s.' % os.getpid())
    p = Pool(max_threads)
    for i in range(max_threads):
        p.apply_async(create_users_task, args=(i, logging.INFO))

    print('Waiting for all subprocesses done...')
    p.close()
    p.join()
    print('All subprocesses done.')
