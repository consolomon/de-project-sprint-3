create table mart.f_customer_retention (
    new_customers_count bigint,
    returning_customers_count bigint,
    refunded_customer_count bigint,
    period_name varchar(8),
    period_id int,
    item_id int,
    new_customers_revenue numeric(10, 2),
    returning_customers_revenue numeric (10, 2),
    customers_refunded bigint,
    primary key (item_id, period_id),
    FOREIGN KEY (item_id) REFERENCES mart.d_item(item_id)
    );
 