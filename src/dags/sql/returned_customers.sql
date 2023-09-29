drop table if exists mart.returned_cust;
create table mart.returned_cust as
with returned_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date), item_id,
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3,4 having count(*)>1
)
select period, item_id as item_ret, count(customer_id) as returned_cust_count, 
sum(revenue) as returned_revenue from returned_cust rc2 group by 1,2;

drop table if exists mart.returned_cust_compact;
create table mart.returned_cust_compact as
with returned_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date),
   count(*), sum(payment_amount) as revenue from staging.user_order_log_v2 uolv where status='shipped' group by 1,2,3 having count(*)>1
)
select period, count(customer_id) as returned_cust_count, 
sum(revenue) as returned_revenue from returned_cust rc2 group by 1;