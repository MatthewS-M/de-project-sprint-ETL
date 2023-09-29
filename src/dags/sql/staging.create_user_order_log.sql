drop table if exists staging.user_order_log;
CREATE TABLE if not exists staging.user_order_log(
        ID serial,
        uniq_id text,
        date_time          TIMESTAMP,
        city_id integer,
        city_name varchar(100),
        customer_id             BIGINT ,
        first_name varchar(100),
        last_name varchar(100),
        item_id integer,
        item_name varchar(100),
        quantity             BIGINT ,
        payment_amount numeric(14,2),
        PRIMARY KEY (ID)
);