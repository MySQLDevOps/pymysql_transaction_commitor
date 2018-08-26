#!/usr/bin/env python
# coding=utf-8
import logging
import random
import traceback
import numpy as np
import py_trx_consistency_test.my_util as mu
from pymysql_transaction_commitor.my_dao import MyDAO
from pymysql_transaction_commitor.my_util import init_logger


class HongbaoManager(MyDAO):
    """
    红包Manager
    """
    def __init__(self, *args, **kw):
        MyDAO.__init__(self, *args, **kw)

    def create_user(self, balance=10000000):
        """
        创建用户和银行余额
        :return: user_id
        """
        sql_insert_user = "insert into `user` " \
                          "set uname = %s, birth_day = %s, addr_province = %s, addr_city = %s, friends = 0 "
        sql_insert_user_bank = "insert into `user_bank` set uid = %s, balance = %s"
        user = (mu.random_uname(), mu.random_birth_day(), mu.random_addr_province(), mu.random_addr_city())
        conn = self.trx_begin()
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

        user = self.query2one(sql_query_user, (uid,))
        if not user:
            return False
        # 随机count个用户id
        uid_range = self.query2one(sql_query_user_count)
        random_user_friends_uids = []
        for _ in range(count):
            random_user_friends_uids.append(str(random.randint(uid_range["min_uid"], uid_range["max_uid"])))
        user_friends = self.query2list(sql_query_user_friends % (",".join(random_user_friends_uids), uid))

        # 彼此添加好友
        conn = self.trx_begin()
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
        return user, self.query2one(sql_query_user, (uid,))

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

        user = self.query2one(sql_query_user, (uid,))
        if not user:
            return False
        # 获取用户好友,及好友的好友,并随机选择一部分后,再去掉重复的id
        user_friends_by_friends = self.query2list(sql_query_user_friends_by_friends, (uid, uid))
        # 总成员数不能大于 实际好友总数
        members = members if members <= len(user_friends_by_friends) else len(user_friends_by_friends)
        random_group_member = np.random.choice(user_friends_by_friends, members, replace=False)
        unique_group_member = {}.fromkeys([val["ufid"] for val in random_group_member]).keys()

        # 开始事务
        conn = self.trx_begin()
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

        return self.query2one(sql_query_group, (insert_group_id,))

    def create_hongbao(self, uid, hongbao_amount=10000):
        """发红包"""
        sql_query_user = "select uid,balance from `user` where uid = %s"
        sql_query_group = "select gid from `group_member` where uid = %s"
        sql_query_group_members = "select uid from `group_member` where gid = %s order by rand()"
        sql_query_hongbao = "select * from `hongbao` where reid = %s"
        sql_insert_hongbao = "insert into hongbao set uid = %s, gid = %s, amount = %s"
        sql_insert_hongbao_detail = "insert into hongbao_detail set reid = %s, uid = %s, amount = %s"
        sql_update_user = "update `user` set balance = balance + %s where uid = %s"
        sql_update_hongbao = "update hongbao " \
                             "set best_luck_uid = %s, max_mount = %s, pickup_users = %s where reid = %s"

        # 检查帐户余额,没有就先冲值
        user = self.query2one(sql_query_user, (uid,))
        if not user:
            return False
        if user["balance"] < hongbao_amount:
            self.user_add_balance(uid, hongbao_amount)

        # 随机选择一个群gid
        group_list = self.query2list(sql_query_group, (uid,))
        gid = random.choice(group_list)["gid"]

        group_members = self.query2list(sql_query_group_members, (gid,))

        insert_reid, best_luck_uid, max_mount, pickup_users = (0, 0, 0, 0)
        # 开始事务
        conn = self.trx_begin()
        try:
            with conn.cursor() as cursor:
                # 发红包
                cursor.execute(sql_update_user, (-1 * hongbao_amount, uid))
                cursor.execute(sql_insert_hongbao, (uid, gid, hongbao_amount))
                insert_reid = cursor.lastrowid
                amount = hongbao_amount

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
                    cursor.execute(sql_insert_hongbao_detail, (insert_reid, member_uid, pickup_amount))
                    if amount == 0:
                        break
                # 更新总计信息
                cursor.execute(sql_update_hongbao, (best_luck_uid, max_mount, pickup_users, insert_reid))
                conn.commit()
        except:
            self.logger.error(traceback.format_exc())
            conn.rollback()
            return False

        return self.query2one(sql_query_hongbao, (insert_reid,))

    def user_add_balance(self, uid, amount):
        """用户冲值"""
        sql_update_user_bank = "update `user_bank` set balance=balance - %s where uid = %s"
        sql_update_user = "update `user` set  balance=balance + %s where uid = %s"
        return self.execute([
            sql_update_user_bank % (amount, uid),
            sql_update_user % (amount, uid)
        ])

    def user_bank_add_balance(self, uid, amount):
        """用户提现"""
        sql_update_user_bank = "update `user_bank` set balance=balance + %s where uid = %s"
        sql_update_user = "update `user` set  balance=balance - %s where uid = %s"
        return self.execute([
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

    def create_hongbaos(self, sending_users, hongbaos):
        """发红包"""
        self.logger.info("开始发红包")
        successed, failed = 0, 0
        range_user_id = self.query2one("select min(uid) begin_uid, max(uid) end_uid from `user`")
        for _ in range(0, sending_users):
            self.logger.debug("随机用户id" % (range_user_id["begin_uid"], range_user_id["end_uid"]))
            uid = random.randint(range_user_id["begin_uid"], range_user_id["end_uid"])
            for _ in range(0, hongbaos):
                if self.create_hongbao(uid):
                    successed += 1
                else:
                    failed += 1
        return successed, failed


if __name__ == '__main__':
    logger = init_logger(level=logging.INFO)
    pm = HongbaoManager(logger=logger)
    # pm.set_general_log(True)
    pm.create_users(users=50, friends=20, groups=2, groups_members=30)
    # pm.create_user()