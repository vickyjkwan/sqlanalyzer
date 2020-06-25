WITH
    analytics_service_endpoint_mapping AS
    (
      SELECT
      b.*
      FROM
      (
        SELECT
        MAX(dt) AS dt
        FROM
        analytics.service_endpoint_mapping
      ) a
      INNER JOIN
      analytics.service_endpoint_mapping b
      ON
      a.dt = b.dt
    ),


    web_dev AS
    (
      SELECT
      s.dt,
      COALESCE(em.platform, 'unknown') AS platform,
      COALESCE(service_org, 'other') AS service,
      account
      FROM
      sku.daily_by_account s
      INNER JOIN
      mapbox_customer_data.accounts a
      ON
      s.account = a.id AND a.dt = '{run_date}'
      LEFT JOIN
      analytics_service_endpoint_mapping em
      ON
      s.sku_id = em.sku_id AND em.in_sku IS NOT NULL AND em.parent_sku IS NULL
      WHERE
      s.dt BETWEEN DATE_SUB('{run_date}', 29) AND '{run_date}'
      -- CAST(s.dt AS DATE) BETWEEN CURRENT_DATE - INTERVAL '30' DAY AND CURRENT_DATE - INTERVAL '1' DAY
      GROUP BY
      1,2,3,4
    ),


    mobile_dev AS
    (
      SELECT
      dt,
      'mobile' AS platform,
      CASE WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'
        WHEN LOWER(useragent) RLIKE '(mapboxeventsunity|mapbox-scenekit)' THEN 'unity'
        WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'
        WHEN LOWER(useragent) RLIKE 'vision' THEN 'vision'
        WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'
        ELSE 'other' END AS service,
      owner AS account
      FROM
      sdk_events.appuserturnstile
      WHERE
      dt BETWEEN DATE_SUB('{run_date}', 29) AND '{run_date}'
      AND
      (
        sdkidentifier IS NULL
        OR
        sdkidentifier <> 'mapbox-gl-js'
      )
      AND
      owner IS NOT NULL
      GROUP BY
      dt,
      'mobile',
      CASE WHEN LOWER(useragent) RLIKE '(mapboxeventsnavigation|navigation-)' THEN 'navigation'
        WHEN LOWER(useragent) RLIKE '(mapboxeventsunity|mapbox-scenekit)' THEN 'unity'
        WHEN LOWER(useragent) RLIKE 'mapboxtelemetry' THEN 'telemetry'
        WHEN LOWER(useragent) RLIKE 'vision' THEN 'vision'
        WHEN LOWER(useragent) RLIKE '(mapboxevents|mapbox-maps-)' THEN 'maps'
        ELSE 'other' END,
      owner
    ),


    studio_dev AS
    (
      SELECT
      dt,
      'web' AS platform,
      'studio' AS service,
      user_id AS account
      FROM
      mapbox_customer_data.segment_tracks 
      WHERE
      dt BETWEEN DATE_SUB('{run_date}', 29) AND '{run_date}'
      AND 
      event_text IN ('Finished an Upload', 'Published a style', 'Created a style', 'Added add a map campaign to their account','Created cartogram style', 'Style editor updated stylesheet layer')
      GROUP BY 
      dt,
      'web',
      'studio',
      user_id
    ),


    web_mobile_studio AS
    (
      SELECT
      dt,
      platform,
      service,
      account
      FROM
      web_dev
      UNION ALL
      SELECT
      dt,
      platform,
      service,
      account
      FROM
      mobile_dev
      UNION ALL
      SELECT
      dt,
      platform,
      service,
      account
      FROM
      studio_dev
    ),


    mau_cube AS
    (
      SELECT
      COALESCE(a.platform, 'other') AS platform,
      COALESCE(a.service, 'unknown') AS service,
      COUNT(DISTINCT a.account) AS num_devs,
      COUNT(DISTINCT CASE WHEN b.account IS NOT NULL THEN NULL ELSE a.account END) AS num_devs_xstudio_only
      FROM
      web_mobile_studio a 
      LEFT JOIN
      (
        SELECT
        account,
        CONCAT_WS(',', COLLECT_SET(LOWER(service))) AS service
        FROM
        web_mobile_studio
        GROUP BY
        account
      ) b 
      ON
      a.account = b.account AND b.service = 'studio'
      GROUP BY
      COALESCE(a.platform, 'other'),
      COALESCE(a.service, 'unknown')
      WITH CUBE
    ),


    wau_cube AS
    (
      SELECT
      COALESCE(a.platform, 'other') AS platform,
      COALESCE(a.service, 'unknown') AS service,
      COUNT(DISTINCT a.account) AS num_devs,
      COUNT(DISTINCT CASE WHEN b.account IS NOT NULL THEN NULL ELSE a.account END) AS num_devs_xstudio_only
      FROM
      web_mobile_studio a 
      LEFT JOIN
      (
        SELECT
        account,
        CONCAT_WS(',', COLLECT_SET(LOWER(service))) AS service
        FROM
        web_mobile_studio
        WHERE
        dt BETWEEN DATE_SUB('{run_date}', 6) AND '{run_date}'
        GROUP BY
        account
      ) b 
      ON
      a.account = b.account AND b.service = 'studio'
      WHERE
      a.dt BETWEEN DATE_SUB('{run_date}', 6) AND '{run_date}'
      GROUP BY
      COALESCE(a.platform, 'other'),
      COALESCE(a.service, 'unknown')
      WITH CUBE
    ),


    dau_cube AS
    (
      SELECT
      COALESCE(a.platform, 'other') AS platform,
      COALESCE(a.service, 'unknown') AS service,
      COUNT(DISTINCT a.account) AS num_devs,
      COUNT(DISTINCT CASE WHEN b.account IS NOT NULL THEN NULL ELSE a.account END) AS num_devs_xstudio_only
      FROM
      web_mobile_studio a 
      LEFT JOIN
      (
        SELECT
        account,
        CONCAT_WS(',', COLLECT_SET(LOWER(service))) AS service
        FROM
        web_mobile_studio
        WHERE
        dt = '{run_date}'
        GROUP BY
        account
      ) b 
      ON
      a.account = b.account AND b.service = 'studio'
      WHERE
      a.dt = '{run_date}'
      GROUP BY
      COALESCE(a.platform, 'other'),
      COALESCE(a.service, 'unknown')
      WITH CUBE
    )


    SELECT
    '30d' AS aggregation,
    COALESCE(platform, '_all') AS platform,
    COALESCE(service, '_all') AS service,
    num_devs,
    num_devs_xstudio_only
    FROM
    mau_cube
    UNION ALL
    SELECT
    '7d' AS aggregation,
    COALESCE(platform, '_all') AS platform,
    COALESCE(service, '_all') AS service,
    num_devs,
    num_devs_xstudio_only
    FROM
    wau_cube
    UNION ALL
    SELECT
    '1d' AS aggregation,
    COALESCE(platform, '_all') AS platform,
    COALESCE(service, '_all') AS service,
    num_devs,
    num_devs_xstudio_only
    FROM
    dau_cube
    