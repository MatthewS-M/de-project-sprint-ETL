insert into mart.d_calendar
select * from mart.d_calendar_temp;

drop table if exists mart.d_calendar_weeks;
create table mart.d_calendar_weeks as
SELECT
    MIN(fact_date) AS start_date,
    MAX(fact_date) AS end_date,
    ROW_NUMBER() OVER (ORDER BY MIN(fact_date)) AS period
FROM
    mart.d_calendar
GROUP BY
    DATE_TRUNC('week', fact_date)
ORDER BY
    start_date;

drop table if exists staging.user_order_log_v2;
create table staging.user_order_log_v2 as
select * from staging.user_order_log uol;

alter table staging.user_order_log_v2 add column status varchar(10);

update staging.user_order_log_v2 set status='shipped';

insert into staging.user_order_log_v2 
select * from staging.user_order_log_temp;