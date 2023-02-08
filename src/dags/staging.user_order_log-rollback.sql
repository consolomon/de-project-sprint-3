/*  
{{ds}} - date of the dag execution
*/
delete from staging.user_order_log uol
where uol.date_time::Date = '{{ds}}'


