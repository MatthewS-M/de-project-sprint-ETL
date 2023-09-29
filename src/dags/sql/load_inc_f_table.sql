        drop table if exists mart.f_sales_temp;
        create table mart.f_sales_temp as
        select nextval('f_sales_id_seq') as id, dc.date_id as date_id, uol.item_id as item_id, uol.customer_id as customer_id, uol.city_id as city_id, uol.quantity as quantity, uol.payment_amount as payment_amount, uol.status as status
        from staging.user_order_log_temp uol join mart.d_calendar_temp dc on cast(dc.fact_date as timestamp)=cast(uol.date_time as timestamp);
        drop sequence f_sales_id_seq;

        update mart.f_sales_v2 set status='shipped';

        insert into mart.f_sales_v2 
        select * from mart.f_sales_temp; 