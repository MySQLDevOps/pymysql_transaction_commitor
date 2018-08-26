#!/usr/bin/env python
# coding=utf-8

import random
import traceback
import pymysql
import ConfigParser
from pymysql_transaction_commitor.my_util import init_logger


class MyDAO:
    """MySQL 简单封装"""
    def __init__(self, connection_settings=None, logger=None):
        self.connection = None
        self.general_log = False
        self.logger = logger if logger else init_logger()
        if not connection_settings:
            cf = ConfigParser.ConfigParser()
            cf.read("hongbao.cnf")
            server = random.choice(cf.sections())
            connection_settings = {
                'host': cf.get(server, "host"),
                'port': cf.getint(server, "port"),
                'user': cf.get(server, "user"),
                'password': cf.get(server, "password"),
                'charset': cf.get(server, "charset"),
                'db': cf.get(server, "db")
            }
            self.logger.debug("MySQL服务器 [%s] %s:%d user:%s" % (server, cf.get(server, "host"), cf.getint(server, "port"), cf.get(server, "user")))
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
