drop sequence if exists date_id_seq;
create sequence date_id_seq start 1;
truncate table mart.d_calendar cascade;
insert into mart.d_calendar 
        with all_dates as (
            select distinct(date_time::timestamp) as fact_date from (
                select date_id as date_time from staging.customer_research cr
                    union all 
                select date_time from staging.user_activity_log ual
                    union all 
                select date_time from staging.user_order_log uol
            ) as dates
        )
select nextval('date_id_seq') as date_id , fact_date, extract(day from fact_date) as day_num, extract(month from fact_date) as month_num, 
TO_CHAR(fact_date, 'mon') as month_name, extract(year from fact_date) as year_num from all_dates;

truncate table mart.f_sales cascade;

        delete from mart.d_customer;
        drop sequence if exists d_customer_id_seq;
        create sequence d_customer_id_seq start 1;
        insert into mart.d_customer 
        select distinct on (customer_id) nextval('d_customer_id_seq') as id, cast(customer_id as int4) as customer_id, first_name, last_name, max(city_id) as city_id from staging.user_order_log uol group by 1,2,3,4;

        delete from mart.d_item;
        drop sequence if exists d_item_id_seq;
        create sequence d_item_id_seq start 1;
        insert into mart.d_item
        select distinct on (item_id) nextval('d_item_id_seq'), item_id::int4, item_name from staging.user_order_log;

