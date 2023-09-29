drop table if exists staging.user_activity_log;
CREATE TABLE if not exists staging.user_activity_log(
        ID serial ,
        uniq_id varchar(255),
        date_time          TIMESTAMP ,
        action_id             BIGINT ,
        customer_id             BIGINT ,
        quantity             BIGINT ,
        PRIMARY KEY (ID)
);