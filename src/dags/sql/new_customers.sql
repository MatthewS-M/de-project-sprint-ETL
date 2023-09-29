drop table if exists mart.new_cust;
create table mart.new_cust as
with new_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date), item_id,
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3,4 having count(*)=1
)
select period, item_id as item_new, count(customer_id) as new_cust_count, 
sum(revenue) as new_cust_revenue from new_cust nc2 group by 1,2;

drop table if exists mart.new_cust_compact;
create table mart.new_cust_compact as
with new_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date),
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3 having count(*)=1
)
select period, count(customer_id) as new_cust_count, 
sum(revenue) as new_cust_revenue from new_cust nc2 group by 1;