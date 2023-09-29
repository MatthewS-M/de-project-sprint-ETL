drop table if exists staging.customer_research;
CREATE TABLE if not exists staging.customer_research(
        ID serial ,
        date_id          TIMESTAMP ,
        category_id             integer ,
        geo_id             integer ,
        sales_qty             integer ,
        sales_amt numeric(14,2),
        PRIMARY KEY (ID)
);