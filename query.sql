SELECT *
   FROM
     (SELECT a.*,
             b.*,
             c.*,
             d.*
      FROM
        (SELECT DISTINCT anonymous_id,
                         user_id
         FROM mapbox_customer_data.segment_identifies
         WHERE dt >= '2018-07-01'
           AND anonymous_id IS NOT NULL
           AND user_id IS NOT NULL ) a
      LEFT JOIN
        (SELECT id,
                email,
                created
         FROM mapbox_customer_data.accounts
         WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id
      LEFT JOIN
        (SELECT anonymous_id AS anon_id_ad,
                context_campaign_name,
                min(TIMESTAMP) AS min_exposure
         FROM mapbox_customer_data.segment_pages
         WHERE dt >= '2018-07-01'
           AND context_campaign_name IS NOT NULL
         GROUP BY 1,
                  2) c ON a.anonymous_id = c.anon_id_ad
      LEFT JOIN
        (SELECT DISTINCT anonymous_id AS anon_id_event,
                         original_timestamp,
                         event,
                         context_traits_email
         FROM mapbox_customer_data.segment_tracks
         WHERE dt >= '2018-07-01'
           AND event LIKE 'submitted_%form'
           AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event
    LEFT JOIN
        (SELECT sfdc_accounts.platform, sfdc_accounts.mobile_os, sfdc_accounts.service_metadata,
sfdc_cases.account, sfdc_cases.num_requests, sfdc_cases.owner, sfdc_accounts.user_id
FROM sfdc.accounts sfdc_accounts
LEFT JOIN 
(SELECT MAX(dt) FROM 
    (SELECT dt 
    FROM sfdc.oppty 
    LEFT JOIN (SELECT MAX(dt) FROM (SELECT DISTINCT dt FROM sfdc.owner AS sfdc_owner) AS dt_owner ON sfdc_oppty.dt = sfdc_cases.dt)
    LEFT JOIN (SELECT dt FROM sfdc.cases) sfdc_cases ON sfdc_oppty.dt = sfdc_cases.dt) )
AS sfdc_cases_oppty ON sfdc_cases_oppty.dt = sfdc_accounts.dt
LEFT JOIN sfdc.cases AS sfdc_cases ON sfdc_cases.id = sfdc_accounts.case_id
WHERE sfdc_cases_oppty.dt > '2020-04-03' AND sfdc_cases_oppty.dt < '2020-05-04' ORDER BY 1 GROUP BY 3 LIMIT 20
        ) e ON e.user_id = a.user_id
        )
   WHERE context_campaign_name IS NOT NULL 
