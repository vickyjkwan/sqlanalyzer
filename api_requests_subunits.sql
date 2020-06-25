WITH com0 as (
	SELECT * 
	, CASE WHEN service = 'directionsmatrix' THEN
	       CASE WHEN (sources = 'all' or sources is null or trim(sources) = '') AND (destinations = 'all' or destinations is null or trim(destinations) = '') then query_cardin*query_cardin
	            WHEN (sources = 'all' or sources is null  or trim(sources) = '') and destinations != 'all' and destinations is not null and trim(destinations) != '' then query_cardin*destinations_cardin
	            WHEN sources != 'all' and sources is not null and trim(sources) != '' and (destinations = 'all' or destinations is null or trim(destinations) = '') then sources_cardin*query_cardin
	            WHEN sources != 'all' and sources is not null and trim(sources) != '' and destinations != 'all' and destinations is not null and trim(destinations) != '' then sources_cardin*destinations_cardin
	            ELSE NULL end 
	       WHEN service in ('geocode', 'permanentgeocode') THEN query_cardin
	            ELSE 1 END as total_subunits
	FROM (
	SELECT * 
	, replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';') as sources
	, cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) as sources_cardin
	, CASE WHEN cs_uri_query like '%destinations&%' then '1' ELSE
	replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';') end as destinations
	, CASE WHEN cs_uri_query like '%destinations&%' then 1 ELSE
	cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) end as destinations_cardin
	--, split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';') as query_mod
	, cardinality(split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';')) as query_cardin                 
	FROM {cloudfront_logs_dataset} 
	WHERE dt = '{run_date}'
	AND cast(hr as BIGINT) in {hr_block} 
	--AND service in ('geocode', 'directionsmatrix', 'permanentgeocode')
	)
	),



	cn0 as (
	        
	SELECT * 
	, CASE WHEN service = 'directionsmatrix' THEN
	   CASE WHEN (sources = 'all' or sources is null or trim(sources) = '') AND (destinations = 'all' or destinations is null or trim(destinations) = '') then query_cardin*query_cardin
	        WHEN (sources = 'all' or sources is null  or trim(sources) = '') and destinations != 'all' and destinations is not null and trim(destinations) != '' then query_cardin*destinations_cardin
	        WHEN sources != 'all' and sources is not null and trim(sources) != '' and (destinations = 'all' or destinations is null or trim(destinations) = '') then sources_cardin*query_cardin
	        WHEN sources != 'all' and sources is not null and trim(sources) != '' and destinations != 'all' and destinations is not null and trim(destinations) != '' then sources_cardin*destinations_cardin
	        ELSE NULL end 
	   WHEN service in ('geocode', 'permanentgeocode') THEN query_cardin
	        ELSE 1 END as total_subunits
	FROM (
	SELECT * 
	, replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';') as sources
	, cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) as sources_cardin
	, CASE WHEN cs_uri_query like '%destinations&%' then '1' ELSE
	replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';') end as destinations
	, CASE WHEN cs_uri_query like '%destinations&%' then 1 ELSE
	cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) end as destinations_cardin
	--, split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';') as query_mod
	, cardinality(split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';')) as query_cardin 
	FROM {cloudfront_logs_china_dataset}
	WHERE dt = '{run_date}'
	AND cast(hr as BIGINT) in {hr_block} 
	--AND service in ('geocode', 'directionsmatrix', 'permanentgeocode')
	)
	),



	proxy0 as (
	SELECT * 
	, CASE WHEN service = 'directionsmatrix' THEN
	       CASE WHEN (sources = 'all' or sources is null or trim(sources) = '') AND (destinations = 'all' or destinations is null or trim(destinations) = '') then query_cardin*query_cardin
	            WHEN (sources = 'all' or sources is null  or trim(sources) = '') and destinations != 'all' and destinations is not null and trim(destinations) != '' then query_cardin*destinations_cardin
	            WHEN sources != 'all' and sources is not null and trim(sources) != '' and (destinations = 'all' or destinations is null or trim(destinations) = '') then sources_cardin*query_cardin
	            WHEN sources != 'all' and sources is not null and trim(sources) != '' and destinations != 'all' and destinations is not null and trim(destinations) != '' then sources_cardin*destinations_cardin
	            ELSE NULL end 
	       WHEN service in ('geocode', 'permanentgeocode') THEN query_cardin
	            ELSE 1 END as total_subunits
	FROM (
	SELECT * 
	, replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';') as sources
	, cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) as sources_cardin
	, CASE WHEN cs_uri_query like '%destinations&%' then '1' ELSE
	replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';') end as destinations
	, CASE WHEN cs_uri_query like '%destinations&%' then 1 ELSE
	cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) end as destinations_cardin
	--, split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';') as query_mod
	, cardinality(split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';')) as query_cardin 
	FROM {cloudfront_logs_china_to_global_proxy_dataset}
	WHERE dt = '{run_date}'
	AND cast(hr as BIGINT) in {hr_block} 
	--AND service in ('geocode', 'directionsmatrix', 'permanentgeocode')
	)
	),




	prod_load0 as (
		    
	SELECT * 
	, CASE WHEN service = 'directionsmatrix' THEN
	       CASE WHEN (sources = 'all' or sources is null or trim(sources) = '') AND (destinations = 'all' or destinations is null or trim(destinations) = '') then query_cardin*query_cardin
	            WHEN (sources = 'all' or sources is null  or trim(sources) = '') and destinations != 'all' and destinations is not null and trim(destinations) != '' then query_cardin*destinations_cardin
	            WHEN sources != 'all' and sources is not null and trim(sources) != '' and (destinations = 'all' or destinations is null or trim(destinations) = '') then sources_cardin*query_cardin
	            WHEN sources != 'all' and sources is not null and trim(sources) != '' and destinations != 'all' and destinations is not null and trim(destinations) != '' then sources_cardin*destinations_cardin
	            ELSE NULL end 
	       WHEN service in ('geocode', 'permanentgeocode') THEN query_cardin
	            ELSE 1 END as total_subunits
	FROM (
	SELECT * 
	, replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';') as sources
	, cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(sources=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) as sources_cardin
	, CASE WHEN cs_uri_query like '%destinations&%' then '1' ELSE
	replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';') end as destinations
	, CASE WHEN cs_uri_query like '%destinations&%' then 1 ELSE
	cardinality(split(replace(regexp_extract(lower(cs_uri_query), '(destinations=)(.+?)((?=&)|$)', 2), '%253b', ';'), ';')) end as destinations_cardin
	--, split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';') as query_mod
	, cardinality(split(replace(replace(lower(query), '%253b', ';'), '%252c', ','), ';')) as query_cardin 
	FROM production_loading_dock.cloudflare_logs
	WHERE dt = '{run_date}'
	AND cast(hr as BIGINT) in {hr_block} 
	--AND service in ('geocode', 'directionsmatrix', 'permanentgeocode')
	)
	),
	    
    com AS
    (
    SELECT
    -- a.dt
    a.hr
    , 'com' AS log_source
    , CASE 
        WHEN LOWER(a.useragent) LIKE '%mapbox%' AND LOWER(a.useragent) RLIKE '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)' -- add iphone?
        THEN REGEXP_EXTRACT(LOWER(a.useragent), '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)', 1)
        ELSE 'web' END AS platform 
    , a.service
    , a.country
    , CAST(a.status as BIGINT) AS status_code
    , CASE WHEN b.sku_id IS NOT NULL THEN CONCAT('exempt_', b.sku_id)
            WHEN c.sku_id IS NOT NULL THEN 'billable'
            WHEN b.sku_id IS NULL and c.sku_id IS NULL AND a.sku_id IS NOT NULL AND a.sku_id != a.service then CONCAT('exempt_', a.sku_id) 
            ELSE NULL
            END AS request_type
    , REGEXP_EXTRACT(a.cs_uri_query, 'pluginName=([^&^\\\\/]+)', 1) as plugin
    , a.account
    , COUNT(a.x_edge_request_id) AS num_requests
    , SUM(a.total_subunits) as subunits
    FROM
    com0 a -- need to change to logs.cloudfront_logs for runs older than 30 days
    LEFT JOIN
    sku.exempted_requests b
    ON
    a.account = b.account
        AND a.x_edge_request_id = b.x_edge_request_id
        AND a.dt = '{run_date}'
        AND b.dt = '{run_date}'
        AND cast(a.hr as BIGINT) in {hr_block} 
        AND cast(b.hr as BIGINT) in {hr_block} 
        AND cast(a.hr as BIGINT) = cast(b.hr as BIGINT)
        AND a.dt = b.dt
        AND a.account IS NOT NULL
    LEFT JOIN
    sku.api c
    ON
    a.account = c.account
        --and c.sku_id in ('geocode', 'directionsmatrix', 'permanentgeocode')
        AND a.service = c.sku_id
        AND a.x_edge_request_id = c.x_edge_request_id
        AND a.dt = '{run_date}'
        AND c.dt = '{run_date}'
        AND cast(a.hr as BIGINT) in {hr_block} 
        AND cast(c.hr as BIGINT) in {hr_block}  
        AND a.dt = c.dt
        AND cast(a.hr as BIGINT) = cast(c.hr as BIGINT)
        AND a.account IS NOT NULL
    WHERE
    a.dt = '{run_date}'
        AND a.hr in {hr_block} 
        AND a.account IS NOT NULL
    GROUP BY
    1,2,3,4,5,6,7,8,9
    ),


    cn AS
    (
    SELECT
    -- a.dt
    a.hr
    , 'cn' AS log_source
    , CASE 
        WHEN LOWER(a.useragent) LIKE '%mapbox%' AND LOWER(a.useragent) RLIKE '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)' -- add iphone?
        THEN REGEXP_EXTRACT(LOWER(a.useragent), '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)', 1)
        ELSE 'web' END AS platform
    , a.service
    , a.country
    , CAST(a.status as BIGINT) AS status_code
    -- , CASE WHEN a.sku_id IS NOT NULL AND a.sku_id != a.service THEN CONCAT('exempt_', a.sku_id) ELSE NULL END AS request_type
    , NULL AS request_type -- china is not payg billable so requests do not exist on the billing tables yet
    , REGEXP_EXTRACT(a.cs_uri_query, 'pluginName=([^&^\\\\/]+)', 1) as plugin
    , a.account
    , COUNT(a.x_edge_request_id) AS num_requests
    , SUM(a.total_subunits) as subunits
    FROM 
    cn0 a -- need to change to logs.cloudfront_logs_china / production_loading_dock.cloudflare_received for older days
    WHERE 
    a.dt = '{run_date}'
        AND cast(a.hr as BIGINT) in {hr_block}
        AND a.account IS NOT NULL
    GROUP BY
    1,2,3,4,5,6,7,8,9
    ),

    
    proxy AS
    (
    SELECT
    a.hr
    , 'proxy' AS log_source
    , CASE 
        WHEN LOWER(a.useragent) LIKE '%mapbox%' AND LOWER(a.useragent) RLIKE '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)' -- add iphone?
        THEN REGEXP_EXTRACT(LOWER(a.useragent), '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)', 1)
        ELSE 'web' END AS platform 
    , a.service
    , a.country
    , CAST(a.status as BIGINT) AS status_code
    -- , CASE WHEN a.sku_id IS NOT NULL AND a.sku_id != a.service THEN CONCAT('exempt_', a.sku_id) ELSE NULL END AS request_type
    , NULL AS request_type
    , REGEXP_EXTRACT(a.cs_uri_query, 'pluginName=([^&^\\\\/]+)', 1) as plugin
    , a.account
    , COUNT(a.x_edge_request_id) AS num_requests
    , SUM(a.total_subunits) as subunits
    FROM
    proxy0 a
    WHERE
    a.dt = '{run_date}'
        AND cast(a.hr as BIGINT) in {hr_block}
        AND a.account IS NOT NULL
    GROUP BY
    1,2,3,4,5,6,7,8,9
    
    union all
    
    SELECT
    a.hr
    , CASE WHEN host = 'api-global.mapbox.cn' then 'proxy' 
            WHEN host = 'api.mapbox.cn' THEN 'cn' ELSE null END AS log_source
    , CASE 
        WHEN LOWER(a.useragent) LIKE '%mapbox%' AND LOWER(a.useragent) RLIKE '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)' -- add iphone?
        THEN REGEXP_EXTRACT(LOWER(a.useragent), '((android|ios|mapboxgl\\\\/1\\\\.0)(react)?)', 1)
        ELSE 'web' END AS platform 
    , a.service
    , a.country
    , CAST(a.status as BIGINT) AS status_code
    -- , CASE WHEN a.sku_id IS NOT NULL AND a.sku_id != a.service THEN CONCAT('exempt_', a.sku_id) ELSE NULL END AS request_type
    , NULL AS request_type
    , REGEXP_EXTRACT(a.cs_uri_query, 'pluginName=([^&^\\\\/]+)', 1) as plugin
    , a.account
    , COUNT(a.x_edge_request_id) AS num_requests
    , SUM(a.total_subunits) as subunits
    FROM
    prod_load0 a
    WHERE
    a.dt = '{run_date}'
        AND cast(a.hr as BIGINT) in {hr_block}
        AND a.account IS NOT NULL
        AND a.host in ('api-global.mapbox.cn', 'api.mapbox.cn')
    GROUP BY
    1,2,3,4,5,6,7,8,9
    )
    
    SELECT
    log_source
    , platform
    , service
    , country
    , status_code
    , request_type
    , case when plugin = '' then null else plugin end as plugin
    , account
    , hr
    , num_requests
    , subunits
    FROM
    com
    
    UNION ALL
    
    SELECT
    log_source
    , platform
    , service
    , country
    , status_code
    , request_type
    , case when plugin = '' then null else plugin end as plugin
    , account
    , hr
    , num_requests
    , subunits
    FROM
    cn
    
    UNION ALL
    
    SELECT
    log_source
    , platform
    , service
    , country
    , status_code
    , request_type
    , case when plugin = '' then null else plugin end as plugin
    , account
    , hr
    , num_requests
    , subunits
    FROM
    proxy