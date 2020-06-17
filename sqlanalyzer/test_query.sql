WITH date_table as (
    SELECT 
            user_id
            , product_start_date
            , dt
            , CASE WHEN DAY(CAST(product_start_date as DATE)) != DAY(CAST(dt_try as DATE))     
                       THEN DATE_ADD(DATE_TRUNC('month', CAST(dt as DATE)), -1)  
                   WHEN DAY(cast(product_start_date as DATE)) > day(cast(dt as DATE)) AND cast(dt as DATE) = DATE_ADD(ADD_MONTHS(DATE_TRUNC('month', CAST(dt as DATE)), 1), -1)
                       THEN DATE_ADD(DATE_ADD(DATE_TRUNC('month', cast(dt as DATE)), 1), -1)              
                   ELSE dt_try             
                   END as new_billing_date      
    FROM (
    SELECT 
            user_id
            , product_start_date
            , dt
            , CASE WHEN cast(dt as DATE) >=  DATE_ADD(DATE_TRUNC('month', cast(dt as DATE)), day(cast(product_start_date as DATE)) -1)
                       THEN DATE_ADD(DATE_TRUNC('month', CAST(dt as DATE)), DAY(CAST(product_start_date as DATE)) -1 )             
                   WHEN CAST(dt as DATE) < DATE_ADD(DATE_TRUNC('month', CAST(dt as DATE)), DAY(CAST(product_start_date as DATE)) -1)             
                       THEN DATE_ADD(ADD_MONTHS( DATE_TRUNC('month', cast(dt as DATE)), -1) , day(cast(product_start_date as DATE)) -1)             
                   END as dt_try
    from (
    SELECT
            id as user_id
            , cast(created as DATE) as product_start_date
            , '{run_date}' as dt 
    FROM mapbox_customer_data.accounts
    where cast(dt as DATE) = cast('{run_date}' as DATE) )
    )
    ),
    raw_data as (
    SELECT * 
    FROM analytics.user_daily_cn
    WHERE cast(dt as DATE) BETWEEN DATE_SUB(CAST('{run_date}' as DATE), 31) AND CAST('{run_date}' as DATE) 
    )
    SELECT 
            a.owner
            , b.product_start_date
            , b.new_billing_date
            , 'mobileactiveusers' as sku_id 
            , count(distinct a.userid) as sub_units
    FROM raw_data a
    INNER JOIN date_table b
            on a.owner = b.user_id
            and cast(a.dt as DATE) BETWEEN cast(b.new_billing_date as DATE) AND cast(b.dt as DATE)  
    GROUP BY 1,2,3 