        drop table if exists mart.d_calendar_temp cascade;
        create table mart.d_calendar_temp as
            with all_dates as (
            select distinct(date_time::timestamp) as fact_date from (
                select date_id as date_time from staging.customer_research_temp cr
                    union all
                select date_time from staging.user_activity_log_temp ual
                    union all
                select date_time from staging.user_order_log_temp uol
            ) as dates
            )
        select nextval('date_id_seq') as date_id , fact_date, extract(day from fact_date) as day_num, extract(month from fact_date) as month_num,
        TO_CHAR(fact_date, 'mon') as month_name, extract(year from fact_date) as year_num from all_dates;
        drop sequence date_id_seq;

        drop table mart.d_item_temp cascade;
        create table mart.d_item_temp as
        select distinct on (item_id) nextval('d_item_id_seq'), item_id::int4, item_name from staging.user_order_log_temp;
        drop sequence d_item_id_seq;

        drop table mart.d_customer_temp cascade;
        create table mart.d_customer_temp as
        select distinct on (customer_id) nextval('d_customer_id_seq'), cast(customer_id as int4), first_name, last_name, max(city_id) from staging.user_order_log_temp uol group by 1,2,3,4;
        drop sequence d_customer_id_seq;