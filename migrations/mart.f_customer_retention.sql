delete from mart.f_customer_retention
where mart.f_customer_retention.period_id in 
    (select dc.{{params.period_id}}
    from mart.d_calendar as dc
    where dc.date_actual >= '{{params.start_date}}'
        and dc.date_actual < '{{params.end_date}}');

insert into mart.f_customer_retention
(new_customers_count, returning_customers_count, refunded_customer_count, period_name, period_id, item_id, new_customers_revenue, returning_customers_revenue, customers_refunded)
with stats as (
select 
    s.item_id,
    s.customer_id,
    dc2.{{params.period_id}} as period_id,
    '{{params.period_name}}' as period_name,
    sum(case
    	    when s.status = 'shipped' then 1
    	    else 0
        end) as shipped,
    sum(case
    	    when s.status = 'refunded' then 1
    	    else 0
        end) as refunded,
    sum(case
    	    when s.status = 'shipped' then s.payment_amount
    	    else 0
        end
    ) as revenue
from mart.f_sales s
left join mart.d_calendar dc2 on s.date_id = dc2.date_id 
where s.date_id in 
(select date_id from mart.d_calendar dc 
where dc.date_actual >= '{{params.start_date}}'
and dc.date_actual < '{{params.end_date}}')
group by item_id, customer_id, period_id)
select
    sum(case
    	    when stats.shipped = 1 then 1
    	    else 0
        end) as new_customers_count,
    sum(case
    	    when stats.shipped > 1 then 1
    	    else 0
        end) as returning_customers_count,
    sum(case
    	    when stats.refunded > 0 then 1
    	    else 0
        end) as refunded_customer_count,
    stats.period_name,
    stats.period_id,
    stats.item_id,
    sum (case
    	     when stats.shipped = 1 then revenue
    	     else 0
         end
    ) as new_customers_revenue,
    sum (case
    	     when stats.shipped > 1 then revenue
    	     else 0
         end
    ) as returning_customers_revenue,
    sum(stats.refunded) as customers_refunded
from stats  
group by item_id, stats.period_name, stats.period_id;