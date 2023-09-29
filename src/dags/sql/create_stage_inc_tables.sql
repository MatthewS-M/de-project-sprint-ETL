    drop table if exists staging.user_activity_log_temp;
    CREATE TABLE if not exists staging.user_activity_log_temp(
        ID serial ,
        uniq_id varchar(255),
        date_time          TIMESTAMP ,
        action_id             BIGINT ,
        customer_id             BIGINT ,
        quantity             BIGINT ,
        PRIMARY KEY (ID)
    );

    drop table if exists staging.user_order_log_temp;
    CREATE TABLE if not exists staging.user_order_log_temp(
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
        status varchar(10),
        PRIMARY KEY (ID)
    );

    drop table if exists staging.customer_research_temp;
    CREATE TABLE if not exists staging.customer_research_temp(
        ID serial ,
        date_id          TIMESTAMP ,
        category_id             integer ,
        geo_id             integer ,
        sales_qty             integer ,
        sales_amt numeric(14,2),
        PRIMARY KEY (ID)
    );