-- /vagrant/pymysql_transaction_commitor/schema/check.sql

use db_hongbao;
select
(select 100000000 * (select count(*) from `user`)) as init_total_balance,
(select sum(balance) from `user`) + (select sum(balance) from user_bank) as final_total_balance,
(select sum(amount) from hongbao) as hb_amount,
(select sum(amount) from hongbao_detail) as hb_dt_amount,
(select sum(friends) friends from `user`) as user_friends,
(select count(*) friends from `user_friends`) as uf_count,
(select sum(group_members) group_members from `group`) as group_members,
(select count(*) friends from `group_member`) as gm_count;