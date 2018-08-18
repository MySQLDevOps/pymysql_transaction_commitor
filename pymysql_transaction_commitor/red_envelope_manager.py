#!/usr/bin/env python
# coding=utf-8
import logging
import random
import traceback
import pymysql
import ConfigParser
import numpy as np
import py_trx_consistency_test.my_util as mu
from pymysql_transaction_commitor.red_envelope_util import init_logger


class MySQLHelper:
    """MySQL 简单封装"""
    def __init__(self, connection_settings=None, logger=None):
        self.connection = None
        self.general_log = False
        self.logger = logger if logger else init_logger()
        if not connection_settings:
            cf = ConfigParser.ConfigParser()
            cf.read("red_envelope.cnf")
            server = random.choice(cf.sections())
            connection_settings = {
                'host': cf.get(server, "host"),
                'port': cf.getint(server, "port"),
                'user': cf.get(server, "user"),
                'password': cf.get(server, "password"),
                'charset': cf.get(server, "charset"),
                'db': cf.get(server, "db")
            }
            self.logger.debug("MySQL服务器 [%s] %s:%d@%s" % (server, cf.get(server, "host"), cf.getint(server, "port"), cf.get(server, "user")))
        self.conn_setting = connection_settings

    def __del__(self):
        self.disconnect()

    def set_general_log(self, val):
        self.logger.debug("set general_log %s" % val)
        self.general_log = val

    def set_logger(self, val):
        self.logger = val

    def connect(self):
        if not self.connection:
            self.connection = pymysql.connect(
                host=self.conn_setting["host"],
                user=self.conn_setting["user"],
                password=self.conn_setting["password"],
                db=self.conn_setting["db"],
                charset=self.conn_setting["charset"],
                cursorclass=pymysql.cursors.DictCursor
            )
        if self.general_log:
            with self.connection.cursor() as cursor:
                    cursor.execute("set global general_log = 1")

        return self.connection

    def disconnect(self):
        if self.connection:
            if self.general_log:
                self.logger.debug("关闭general_log日志")
                with self.connection.cursor() as cursor:
                    cursor.execute("set global general_log = 0")
            self.connection.close()

    def insert_auto(self, sql, data, conn=None):
        if not conn:
            conn = self.connect()
        with conn as cursor:
            cursor.execute(sql, data)
            lastrowid = cursor.lastrowid

        return lastrowid

    def query2one(self, sql, para=None):
        with self.connect() as cursor:
            cursor.execute(sql, para)
            return cursor.fetchone()

    def query2list(self, sql, para=None):
        with self.connect() as cursor:
            cursor.execute(sql, para)
            return cursor.fetchall()

    def execute(self, sql_list=None):
        if sql_list is None:
            return False

        conn = self.connect()
        conn.autocommit(False)
        conn.begin()
        try:
            with conn.cursor() as cursor:
                for sql in sql_list:
                    cursor.execute(sql)
                conn.commit()
                opt_success = True
        except:
            self.logger.error(traceback.format_exc())
            conn.rollback()
            opt_success = False

        return opt_success

    def trx_begin(self):
        conn = self.connect()
        conn.autocommit(False)
        conn.begin()
        return conn

    def trx_end(self):
        conn = self.connect()
        conn.commit()
        return conn


class MyManager:
    """
    数据库操作Manager,具有事务性
    """
    def __init__(self, mysql):
        self.mysql = mysql
        self.logger = self.mysql.logger

    def create_user(self, balance=10000000):
        """
        创建用户和银行余额
        :return: user_id
        """
        sql_insert_user = "insert into `user` " \
                          "set uname = %s, birth_day = %s, addr_province = %s, addr_city = %s, friends = 0 "
        sql_insert_user_bank = "insert into `user_bank` set uid = %s, balance = %s"
        user = (mu.random_uname(), mu.random_birth_day(), mu.random_addr_province(), mu.random_addr_city())
        conn = self.mysql.trx_begin()
        insert_user_id = None
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_insert_user, user)
                insert_user_id = cursor.lastrowid
                cursor.execute(sql_insert_user_bank, (insert_user_id, balance))
                conn.commit()
        except:
            self.logger.error(traceback.format_exc())
            conn.rollback()

        return insert_user_id

    def create_user_friends(self, uid, count=10):
        """创建好友"""
        sql_query_user = "select uid,uname,friends from `user` where uid = %s"
        sql_query_user_count = "select min(uid) min_uid,max(uid) max_uid from `user`"
        sql_query_user_friends = """
            select uid from `user` a where uid in (%s) and uid not in (
                select ufid from user_friends b where b.uid = %s
            )
        """
        sql_insert_user_friends = "insert into user_friends set uid = %s, ufid = %s"
        sql_update_user = "update `user` set  friends = friends + %s where uid = %s"

        user = self.mysql.query2one(sql_query_user, (uid,))
        if not user:
            return False
        # 随机count个用户id
        uid_range = self.mysql.query2one(sql_query_user_count)
        random_user_friends_uids = []
        for _ in range(count):
            random_user_friends_uids.append(str(random.randint(uid_range["min_uid"], uid_range["max_uid"])))
        user_friends = self.mysql.query2list(sql_query_user_friends % (",".join(random_user_friends_uids), uid))

        # 彼此添加好友
        conn = self.mysql.trx_begin()
        try:
            with conn.cursor() as cursor:
                for friend in user_friends:
                    if uid == friend["uid"]:
                        continue
                    cursor.execute(sql_insert_user_friends, (uid, friend["uid"]))
                    cursor.execute(sql_insert_user_friends, (friend["uid"], uid))
                    cursor.execute(sql_update_user, (1, friend["uid"]))
                cursor.execute(sql_update_user, (len(user_friends), uid))
                conn.commit()
        except:
            self.logger.error(traceback.format_exc())
            conn.rollback()
        return user, self.mysql.query2one(sql_query_user, (uid,))

    def create_group(self, uid, members=50):
        """建群"""
        sql_query_group = "select gid,create_uid,gname,group_members from `group` where gid = %s"
        sql_query_user = "select uid,uname,friends from `user` where uid = %s"
        sql_query_user_friends_by_friends = """
            select ufid from `user_friends` where uid = %s
            union all
            select b.ufid from `user_friends` a, `user_friends` b where a.ufid = b.uid and a.uid = %s
        """
        sql_insert_group = "insert into `group` set create_uid = %s, gname = %s "
        sql_update_group = "update `group` set group_members = group_members + %s where gid = %s"
        sql_insert_group_member = "insert into `group_member` set gid = %s, uid = %s "

        user = self.mysql.query2one(sql_query_user, (uid,))
        if not user:
            return False
        # 获取用户好友,及好友的好友,并随机选择一部分后,再去掉重复的id
        user_friends_by_friends = self.mysql.query2list(sql_query_user_friends_by_friends, (uid, uid))
        # 总成员数不能大于 实际好友总数
        members = members if members <= len(user_friends_by_friends) else len(user_friends_by_friends)
        random_group_member = np.random.choice(user_friends_by_friends, members, replace=False)
        unique_group_member = {}.fromkeys([val["ufid"] for val in random_group_member]).keys()

        # 开始事务
        conn = self.mysql.trx_begin()
        insert_group_id = 0
        try:
            with conn.cursor() as cursor:
                cursor.execute(sql_insert_group, (uid, mu.random_gname()))
                insert_group_id = cursor.lastrowid
                for member_uid in unique_group_member:
                    cursor.execute(sql_insert_group_member, (insert_group_id, member_uid))
                cursor.execute(sql_update_group, (len(unique_group_member), insert_group_id))
                conn.commit()
        except:
            self.logger.error(traceback.format_exc())
            conn.rollback()

        return self.mysql.query2one(sql_query_group, (insert_group_id,))

    def create_red_envelope(self, uid, red_envelope_amount=10000, general_log=False):
        """发红包"""
        # 红包金额
        sql_query_user = "select uid,balance from `user` where uid = %s"
        sql_query_group = "select gid from `group_member` where uid = %s"
        sql_query_group_members = "select uid from `group_member` where gid = %s order by rand()"
        sql_query_red_envelope = "select * from `red_envelope` where reid = %s"
        sql_insert_red_envelope = "insert into red_envelope set uid = %s, gid = %s, amount = %s"
        sql_insert_red_envelope_detail = "insert into red_envelope_detail set reid = %s, uid = %s, amount = %s"
        sql_update_user = "update `user` set balance = balance + %s where uid = %s"
        sql_update_red_envelope = "update red_envelope " \
                                  "set best_luck_uid = %s, max_mount = %s, pickup_users = %s where reid = %s"

        # 检查帐户余额,没有就先冲值
        user = self.mysql.query2one(sql_query_user, (uid,))
        if not user:
            return False
        if user["balance"] < red_envelope_amount:
            self.user_add_balance(uid, red_envelope_amount)

        # 随机选择一个群gid
        group_list = self.mysql.query2list(sql_query_group, (uid,))
        gid = random.choice(group_list)["gid"]

        group_members = self.mysql.query2list(sql_query_group_members, (gid,))

        insert_reid, best_luck_uid, max_mount, pickup_users = (0, 0, 0, 0)
        # 开始事务
        conn = self.mysql.trx_begin()
        try:
            with conn.cursor() as cursor:
                # 发红包
                cursor.execute(sql_update_user, (-1 * red_envelope_amount, uid))
                cursor.execute(sql_insert_red_envelope, (uid, gid, red_envelope_amount))
                insert_reid = cursor.lastrowid
                amount = red_envelope_amount

                # 领红包
                for member in group_members:
                    member_uid = member["uid"]
                    # 累计人数
                    pickup_users += 1
                    # 随机分配金额,单位为分
                    pickup_amount = random.randint(1, amount)
                    amount -= pickup_amount
                    # 最后一个人分得红包所剩全部金额
                    if pickup_users == len(group_members):
                        pickup_amount = amount
                    # 记录最佳手气uid
                    if pickup_amount > max_mount:
                        max_mount = pickup_amount
                        best_luck_uid = member_uid
                    # 收红包的人加余额
                    cursor.execute(sql_update_user, (pickup_amount, member_uid))
                    # 记录红包记录
                    cursor.execute(sql_insert_red_envelope_detail, (insert_reid, member_uid, pickup_amount))
                    if amount == 0:
                        break
                # 更新总计信息
                cursor.execute(sql_update_red_envelope, (best_luck_uid, max_mount, pickup_users, insert_reid))
                conn.commit()
        except:
            self.logger.error(traceback.format_exc())
            conn.rollback()

        return self.mysql.query2one(sql_query_red_envelope, (insert_reid,))

    def user_add_balance(self, uid, amount):
        """用户冲值"""
        sql_update_user_bank = "update `user_bank` set balance=balance - %s where uid = %s"
        sql_update_user = "update `user` set  balance=balance + %s where uid = %s"
        return self.mysql.execute([
            sql_update_user_bank % (amount, uid),
            sql_update_user % (amount, uid)
        ])

    def user_bank_add_balance(self, uid, amount):
        """用户提现"""
        sql_update_user_bank = "update `user_bank` set balance=balance + %s where uid = %s"
        sql_update_user = "update `user` set  balance=balance - %s where uid = %s"
        return self.mysql.execute([
            sql_update_user_bank % (amount, uid),
            sql_update_user % (amount, uid)
        ])

    def create_users(self, users=100, friends=20, groups=5, groups_members=60):
        """创建用户,好友,群"""
        self.logger.info("开始创建用户:%d" % users)
        for _ in range(users):
            self.create_user()

        self.logger.info("开始创建好友:%d" % friends)
        for uid in range(1, users+1):
            pm.create_user_friends(uid, friends)

        self.logger.info("开始创建群:%d,成员:%d" % (groups, groups_members) )
        for uid in range(1, users+1):
            for group in range(0, groups):
                self.logger.debug("开始创建用户[%d]第[%d]个群" % (uid,group,))
                pm.create_group(uid, groups_members)

    def create_red_envelopes(self, users, envelopes):
        """发红包"""
        self.logger.info("开始发红包")
        range_user_id = self.mysql.query2one("select min(uid) begin_uid, max(uid) end_uid from `user`")
        for _ in range(0, users):
            self.logger.debug("随机用户id" % (range_user_id["begin_uid"], range_user_id["end_uid"]))
            uid = random.randint(range_user_id["begin_uid"], range_user_id["end_uid"])
            for _ in range(0, envelopes):
                self.create_red_envelope(uid)


if __name__ == '__main__':
    logger = init_logger(level=logging.DEBUG)
    pm = MyManager(MySQLHelper(logger=logger))
    pm.mysql.set_general_log(True)
    pm.create_users(users=50, friends=20, groups=2, groups_members=30)
    # pm.create_user()
