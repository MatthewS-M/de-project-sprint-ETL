        truncate table mart.f_sales cascade;
        drop sequence if exists f_sales_id_seq;
        create sequence f_sales_id_seq start 1;
        insert into mart.f_sales 
        select nextval('f_sales_id_seq') as id, dc.date_id as date_id, uol.item_id as item_id, uol.customer_id as customer_id, uol.city_id as city_id, uol.quantity as quantity, uol.payment_amount as payment_amount
        from staging.user_order_log uol join mart.d_calendar dc on cast(dc.fact_date as timestamp)=cast(uol.date_time as timestamp);  

        drop table if exists mart.f_sales_v2;
        create table mart.f_sales_v2 as
        select * from mart.f_sales;

        alter table mart.f_sales_v2 add column status varchar(10);