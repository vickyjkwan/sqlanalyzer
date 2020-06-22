WITH mapbox_accounts0 as (
	select 
	        id 
	        , email as email_accounts
	        , accountlevel
	from mapbox_customer_data.accounts
	where dt = '{run_date}'
	AND COALESCE(LOWER(email), 'NULL') NOT LIKE ('%@mapbox.com') 
	AND COALESCE(accountlevel, 'NULL') NOT IN ('staff', 'mapbox-no-bill-1')
	AND id NOT IN ('streets-review-production')
	), 
	mbx_verticals as (
	SELECT 
	        mapbox_account_id
	        , vertical
	FROM (
	SELECT *
	        , max(dt) over (partition by 'dummy') as max_dt
	from analytics.mbx_account_verticals
	)
	where dt = max_dt
	), 
	mbx_sfdc_accounts as (
	SELECT a.*
	        , b.name as sfdc_account_name
	FROM sfdc.mapbox_accounts a
	LEFT JOIN sfdc.accounts b
	     on a.dt = '{run_date}'
	     and b.dt = '{run_date}'
	     and a.salesforce_account_id = b.id
	where a.dt = '{run_date}'
	),
	enterprise_exception AS
	(
		--List of special enterprise accounts aka cohort 1
		SELECT
		*
		FROM
		(
			VALUES
			('0013600000QmuNtAAJ')   --Snap nkmap
			,('00136000005AxApAAK')  --Facebook fbmaps
			,('00136000005AxMbAAK')  --TWC weather
			,('0013600001KDnawAAD')  --Yahoo yahoojapan
			,('0011R000020WJXyQAO')  --Tableau tableau-production
			,('00136000005AxCgAAK')  --IBM ibmrave
			,('00136000005AxElAAK')  --Mapquest mapquest
			,('0013600001KDUFDAA5')  --DPD dpd-saturn-five
			,('00136000005AxLLAA0')  --Strava strava
			,('00136000005Ax5QAAS')  --Alltrails alltrails
			,('00136000005AxLgAAK')  --T-Mobile
		) AS v (sfdc_acct_id)
	),
	apa_deals0 as (
	SELECT 
	    mbx_acct_id
	    , prd_start_date
	    , prd_end_date
	FROM (
	SELECT *
	        , max(dt) over (partition by 'dummy') as max_dt
	from analytics.apa_deals
	) 
	WHERE dt = max_dt
	),
	apa_deals as (
	SELECT DISTINCT
		rcal.vdate
		,apa.mbx_acct_id
	FROM
		apa_deals0 apa
	RIGHT JOIN
		analytics.ref_calendar rcal
		ON rcal.vdate BETWEEN apa.prd_start_date AND apa.prd_end_date
	),
	paygo_free AS
	(
		--MBX accounts that used a 100% repeating/forever
		SELECT DISTINCT
			DATE(DATE_TRUNC('month', inv.date)) AS date_month
			,mbxacct.id AS mbx_acct_id
		FROM
			payments.stripe_invoices inv
		INNER JOIN
			payments.stripe_discounts disc
			ON inv.discount_id = disc.id
		INNER JOIN
			payments.stripe_coupons coup
			ON disc.coupon_id = coup.id
			AND coup.duration IN ('forever', 'repeating')
			AND coup.percent_off = 100
		INNER JOIN
			mapbox_customer_data.accounts mbxacct
			ON inv.customer_id = mbxacct.customerid
			AND mbxacct.dt = '{run_date}'
		WHERE
			inv.date >= DATE('2019-05-01')
	),
	cohort_mapping as (
	SELECT id
	        , email_accounts
	        , sfdc_account_id
	        , vertical
	        , revenue_group
	        ,CASE 
			  WHEN revenue_group in ('enterprise exception') THEN 'Cohort 1'
			  WHEN revenue_group in ('enterprise longtail', 'spp') THEN 'Cohort 2'
			  ELSE 'Cohort 3'
			 END AS cohort  
	FROM (
	SELECT 
	        a.id
	        , a.email_accounts
	        , mbx_sfdc.salesforce_account_id as sfdc_account_id
	        , COALESCE(b.vertical, 'none') as vertical
		    , CASE
	                WHEN entexp.sfdc_acct_id IS NOT NULL THEN 'enterprise exception'
	                WHEN spp.mbx_acct_id IS NOT NULL THEN 'spp'
	                WHEN a.accountlevel = 'enterprise' THEN 'enterprise longtail'
	                WHEN pfree.mbx_acct_id IS NOT NULL THEN 'paygo_free'
	                ELSE 'paygo'
	                END AS revenue_group
	FROM mapbox_accounts0 a
	LEFT JOIN mbx_verticals b
	    on a.id = b.mapbox_account_id
	LEFT JOIN mbx_sfdc_accounts mbx_sfdc
	        on a.id = mbx_sfdc.mapbox_account_id
	LEFT JOIN enterprise_exception entexp
	        on mbx_sfdc.salesforce_account_id = entexp.sfdc_acct_id
	LEFT JOIN apa_deals spp
	        on a.id = spp.mbx_acct_id
	        and spp.vdate = DATE('{run_date}')
	LEFT JOIN paygo_free pfree
	        on a.id = pfree.mbx_acct_id 
		AND pfree.date_month BETWEEN ADD_MONTHS(DATE('{run_date}'), -1) AND DATE_SUB(DATE('{run_date}'), 1) 
	)
	),
	service_mapping as (
	SELECT 
	        distinct 
	        sku_id as service
	        , service_org
	from (
	SELECT 
	        distinct
	        sku_id 
	        , service_org
	        , dt
	        , max(dt) over (partition by 'dummy') as max_dt
	FROM analytics.service_endpoint_mapping
	where cast(dt as DATE) >= CURRENT_DATE - INTERVAL '4' DAY
	)
	where dt = max_dt
	),
	weekly_data0 as (
	select 
	        (date_trunc('WEEK',(cast(a.dt as DATE) + INTERVAL '1' DAY)) - INTERVAL '1' DAY) as dt_week
	        , a.log_source 
	        , a.platform
	        , a.service
	        , COALESCE(c.service_org, 'other') as service_org
	        , case when a.status_code in (301, 302, 307) OR a.status_code >= 400 then 'non_billable' else 'billable' end as status_code
	        , case when a.request_type is null then 'non_billable' else a.request_type end as request_type
	        , b.cohort
	        , 'all_cohort' as dummy       
	        , b.vertical
	        , a.account
	        , COALESCE(b.sfdc_account_id, a.account) as sfdc_account_id
	        , sum(a.num_requests) as num_requests
	from analytics.api_requests a
	INNER JOIN cohort_mapping b 
	    on a.account = b.id
	LEFT JOIN service_mapping c
		on a.service = c.service
	where cast(a.dt as DATE) > cast('{run_date}' as DATE) - INTERVAL '14' DAY
	and cast(a.dt as DATE) <= cast('{run_date}' as DATE)
	and a.account != 'unknown'
	group by 1,2,3,4,5,6,7,8,9,10,11,12
	union all
	select 
	        (date_trunc('WEEK',(cast(a.dt as DATE) + INTERVAL '1' DAY)) - INTERVAL '1' DAY) as dt_week
	        , a.log_source 
	        , a.platform
	        , a.service
	        , COALESCE(c.service_org, 'other') as service_org
	        , case when a.status_code in (301, 302, 307) OR a.status_code >= 400 then 'non_billable' else 'billable' end as status_code
	        , case when a.request_type is null then 'non_billable' else a.request_type end as request_type
	        , 'Cohort 2, 3' as cohort
	        , 'cohort_2_3' as dummy
	        , b.vertical
	        , a.account
	        , COALESCE(b.sfdc_account_id, a.account) as sfdc_account_id
	        , sum(a.num_requests) as num_requests
	from analytics.api_requests a
	INNER JOIN cohort_mapping b 
	    on a.account = b.id
	    and b.cohort in ('Cohort 2', 'Cohort 3')
	LEFT JOIN service_mapping c
		on a.service = c.service
	where cast(a.dt as DATE) > cast('{run_date}' as DATE) - INTERVAL '14' DAY
	and cast(a.dt as DATE) <= cast('{run_date}' as DATE)
	and a.account != 'unknown'
	and b.cohort in ('Cohort 2', 'Cohort 3')
	group by 1,2,3,4,5,6,7,8,9,10,11,12
	),	
	cubed_data0_pre as (
	SELECT 
	    dt_week
	    , log_source
	    , platform
	    , service
	    , service_org
	    , status_code
	    , request_type
	    , cohort
	    , vertical
	    , account
	    , sfdc_account_id
	    , num_requests
	FROM (
		select 
		        dt_week
		        , log_source
		        , platform
		        , service
		        , service_org
		        , status_code
		        , request_type
		        , cohort
		        , dummy
		        , vertical
		        , account
		        , sfdc_account_id
		        , sum(num_requests) as num_requests
		from weekly_data0
		GROUP BY dt_week, log_source, platform, service, service_org, status_code, request_type, account, cohort, dummy, vertical, sfdc_account_id
		WITH CUBE
	)
	WHERE dummy is not null
	AND NOT (cohort is null AND dummy != 'all_cohort')
	AND NOT (service is not null AND service_org is NULL)
	AND sfdc_account_id is not null
	AND dt_week is not null
	),
	cubed_data as (
		select 
		        a.dt_week
		        , COALESCE(a.log_source, '_all') as log_source
		        , COALESCE(a.platform, '_all') as platform
		        , COALESCE(a.service, '_all') as service
		        , COALESCE(a.service_org, '_all') as service_org
		        , COALESCE(a.status_code, '_all') as status_code
		        , COALESCE(a.request_type, '_all') as request_type
		        , COALESCE(a.cohort, '_all') as cohort
		        , COALESCE(a.vertical, '_all') as vertical
		        , COALESCE(a.account, '_all') as account
		        , a.sfdc_account_id as sfdc_account_id
		        , a.num_requests
		from cubed_data0_pre a
	),
	this_week as (
		select 
		        dt_week
		        , log_source
		        , platform
		        , service
		        , service_org
		        , status_code
		        , request_type
		        , cohort
		        , vertical
		        , account as account_a
		        , sfdc_account_id
		        , num_requests
		from cubed_data
		
	),
	last_week as (
		select 
		        cast(dt_week as DATE) + INTERVAL '7' DAY as dt_week
		        , log_source
		        , platform
		        , service
		        , service_org
		        , status_code
		        , request_type
		        , cohort
		        , vertical
		        , account as account_b
		        , sfdc_account_id
		        , num_requests
		from cubed_data
		
	),
	total_join as (
		        select 
		            coalesce(b.account_b, a.account_a) as account
		            , coalesce(b.sfdc_account_id, a.sfdc_account_id) as sfdc_account_id
		            , coalesce(b.cohort, a.cohort) as cohort
		            , coalesce(b.vertical, a.vertical) as vertical
		            , coalesce(cast(b.dt_week as DATE), cast(a.dt_week as DATE)) as dt_week
		            , coalesce(b.service, a.service) as service
		            , coalesce(b.service_org, a.service_org) as service_org
		            , coalesce(b.log_source, a.log_source) as log_source
		            , coalesce(b.platform, a.platform) as platform
		            , coalesce(b.status_code, a.status_code) as status_code
		            , coalesce(b.request_type, a.request_type) as request_type
		            , coalesce(a.num_requests,0) as this_week
		            , coalesce(b.num_requests,0) as last_week
		            , COALESCE(a.num_requests, 0) - coalesce(b.num_requests,0) as diff
		        from this_week a
		        left join last_week b
		                on a.account_a = b.account_b
		                and a.sfdc_account_id = b.sfdc_account_id
		                and a.cohort = b.cohort
		                and a.vertical = b.vertical
		                and a.service = b.service
		                and a.service_org = b.service_org
		                and a.log_source = b.log_source
		                and a.platform = b.platform
		                and a.request_type = b.request_type
		                and a.status_code = b.status_code
		                and cast(a.dt_week as DATE) = cast(b.dt_week as DATE)
	),
	winners_pre as (
		SELECT * 
		, row_number() OVER (PARTITION by dt_week, log_source, service, service_org, platform, request_type, status_code, cohort, vertical ORDER BY diff DESC) as rank
		FROM total_join
		WHERE account = '_all'
		AND cast(dt_week as DATE) + INTERVAL '6' DAY = cast('{run_date}' as DATE)
		AND diff > 0
		AND account != 'unknown'
		
		union all
		
		SELECT * 
		, row_number() OVER (PARTITION by dt_week, log_source, service, service_org, platform, request_type, status_code, cohort, vertical ORDER BY diff DESC) as rank
		FROM total_join
		WHERE account != '_all'	
		AND cast(dt_week as DATE) + INTERVAL '6' DAY = cast('{run_date}' as DATE)
		AND diff > 0
		AND account != 'unknown'
		
	),
	winners as (
		select 
		        'Winners' as category
		        , log_source
		        , service
		        , service_org
		        , account
		        , sfdc_account_id
		        , cohort
		        , vertical
		        , platform
		        , status_code
		        , request_type
		        , cast(dt_week as DATE) + INTERVAL '6' DAY as dt_week
		        , this_week
		        , last_week
		        , diff
		        , rank 
		from winners_pre
		where rank <= 50 
		        and diff > 0
		        and account != 'unknown'
		        
	),
	losers_pre as (
		SELECT * 
		, row_number() OVER (PARTITION by dt_week, log_source, service, service_org, platform, request_type, status_code, cohort, vertical ORDER BY diff ASC) as rank
		FROM total_join
		WHERE account = '_all'
		AND cast(dt_week as DATE) + INTERVAL '6' DAY = cast('{run_date}' as DATE)
		AND diff < 0
		AND account != 'unknown'
		
		union all
		
		SELECT * 
		, row_number() OVER (PARTITION by dt_week, log_source, service, service_org, platform, request_type, status_code, cohort, vertical ORDER BY diff ASC) as rank
		FROM total_join
		WHERE account != '_all'
		AND cast(dt_week as DATE) + INTERVAL '6' DAY = cast('{run_date}' as DATE)
		AND diff < 0
		AND account != 'unknown'
		
	),
	losers as (
		select 
		        'Losers' as category
		        , log_source
		        , service
		        , service_org
		        , account
		        , sfdc_account_id
		        , cohort
		        , vertical
		        , platform
		        , status_code
		        , request_type
		        , cast(dt_week as DATE) + INTERVAL '6' DAY as dt_week
		        , this_week
		        , last_week
		        , diff
		        , rank 
		from losers_pre
		where rank <= 50 
		        and diff < 0 
		        and account != 'unknown'
	), 
	final_winners_losers as (
		SELECT 
		    category
		    , log_source
		    , service
		    , service_org
		    , account
		    , sfdc_account_id
		    , cohort
		    , vertical
		    , platform
		    , status_code
		    , request_type
		    , this_week
		    , last_week
		    , diff
		    , rank
		from losers
		where cast(dt_week as DATE) = cast('{run_date}' as DATE)
		
		union all
		
		SELECT
		    category
		    , log_source
		    , service
		    , service_org
		    , account
		    , sfdc_account_id
		    , cohort
		    , vertical
		    , platform
		    , status_code
		    , request_type
		    , this_week
		    , last_week
		    , diff
		    , rank
		from winners
		where cast(dt_week as DATE) = cast('{run_date}' as DATE)
		
	)
	SELECT * 
	FROM final_winners_losers