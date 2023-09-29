drop table if exists mart.f_customer_retention;
create table mart.f_customer_retention(
    new_customers_count int4,
    returning_customers_count int4,
    refunded_customer_count int4,
    period_name varchar default 'weekly',
    period_id int4,
    item_id_new_cust int4,
    item_id_returned_cust int4,
    item_id_refunded_cust int4,
    new_customers_revenue bigint,
    returning_customers_revenue bigint,
    customers_refunded int4
);