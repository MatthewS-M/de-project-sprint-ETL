drop table if exists mart.refunded_cust;
create table mart.refunded_cust as
with refunded_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date), item_id,
   count(*) as refunds from staging.user_order_log_v2 uolv where status='refunded' group by 1,2,3,4
)
select period, item_id as item_ref, count(*) cust_with_refunds, sum(refunds) total_refunds from refunded_cust group by 1,2; -- refunded

drop table if exists mart.refunded_cust_compact;
create table mart.refunded_cust_compact as
with refunded_cust as (
select customer_id, date_time, 
    (select period from mart.d_calendar_weeks where uolv.date_time between start_date and end_date),
   count(*) as refunds from staging.user_order_log_v2 uolv where status='refunded' group by 1,2,3
)
select period, count(*) cust_with_refunds, sum(refunds) total_refunds from refunded_cust group by 1; -- refunded
