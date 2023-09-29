drop table if exists mart.retention_compact; 
create table mart.retention_compact as
select * from mart.new_cust_compact full join mart.returned_cust_compact using(period) full join mart.refunded_cust_compact using(period) order by 1;

truncate table mart.f_customer_retention cascade;
insert into mart.f_customer_retention
select new_cust_count, returned_cust_count, cust_with_refunds, 'weekly', period, item_new, item_ret, item_ref, new_cust_revenue, returned_revenue, total_refunds 
from mart.new_cust full join mart.returned_cust using(period) full join mart.refunded_cust using(period) order by 1;