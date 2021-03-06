pymysql_transaction_commitor
============================
pymysql_transaction_commitor 简称PTC工具,使用pymysql持续生成MySQL事务性数据,用于MySQL数据一致性相关测试.

包含以下功能:

* 模拟发红包的资金流数据生成,红包随机发,保持总额不变
* 模拟电子商务订单数据流生成,类似于TPCC简化版,增强可读性,可扩展性

用途
===

* MySQL高可用failover的数据一致性校验
* MySQL多主写的数据更新丢失问题
* MySQL热备份的数据一致性校验


安装
===

```
shell> git clone https://github.com/alvinzane/pymysql_transaction_commitor.git
shell> yum install python-pip
shell> pip install -r requirements.txt
```

使用说明
====
```
# 数据库配置文件
shell> cat hongbao.cnf
[server1]
host = 192.168.20.101
port = 3306
user = youruser
password = p@ssw0rd
charset = utf8
db = db_hongbao

# 导入scheme
shell>mysql -p < schema/db_hongbao_tables.sql

# 数据一致性检查
shell>mysql -p < schema/db_hongbao_check.sql
+--------------------+---------------------+-----------+--------------+--------------+----------+---------------+----------+
| init_total_balance | final_total_balance | hb_amount | hb_dt_amount | user_friends | uf_count | group_members | gm_count |
+--------------------+---------------------+-----------+--------------+--------------+----------+---------------+----------+
|      1265100000000 |        126510000000 |   3010000 |    3010000   |       478854 |   478854 |         86339 |    86339 |
+--------------------+---------------------+-----------+--------------+--------------+----------+---------------+----------+

说明:
init_total_balance:所有用户初始金额
final_total_balance:让红包飞一会儿后的用户总金额,必须和上面一致的

hb_amount:已发红包总额
hb_dt_amount:已收红包总额

其它略,请自行查行参考SQL语句.
```

## 分支操作
```
1）进入github的项目的主页面，点击右上角的Fork按钮即可建立自己的分支。

2）从自己的分支复制到本地

git clone https://github.com/yourfork/pymysql_transaction_commitor

3）添加主库URL:

git remote add PTC https://github.com/MySQLDevOps/pymysql_transaction_commitor

4)获取主库的修改

git fetch PTC

5)合并主库代码到自己的本地分支上：

git merge PTC/master master

git pull origin master

6)上推到自己的github网站账号：

git push origin master

7)将自己的github网站分支将更新同步到主库：在自己的github网站分支按“pull request”按钮
```