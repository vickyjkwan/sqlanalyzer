SELECT u.name,
       b.customer_tier_c,
       b.name,
       m.account,
       b.x18_digit_account_id_c,
       s.id,
       m.platform,
       m.mobile_os,
       m.num_requests,
       Row_number() OVER(PARTITION BY s.id) row_
FROM wbr.map_requests_by_account m
INNER JOIN
  (SELECT DISTINCT id
   FROM mapbox_customer_data.styles
   WHERE cast(dt AS DATE) >= CURRENT_DATE - INTERVAL '14' DAY
     AND sources LIKE '%mapbox-streets-v7%' ) s ON m.service_metadata_version = s.id
LEFT JOIN
  (SELECT customer_tier_c,
          csm_c,
          name,
          mapbox_username_c,
          x18_digit_account_id_c
   FROM sfdc.accounts
   WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON m.account = b.mapbox_username_c
LEFT JOIN
  (SELECT name,
          id
   FROM sfdc.users
   WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) u ON b.csm_c = u.id
WHERE cast(m.dt AS DATE) >= CURRENT_DATE - INTERVAL '14' DAY
  AND m.service_metadata = 'custom'
  AND m.service = 'styles'
  AND b.customer_tier_c IN ('Tier 0',
                            'Tier 1',
                            'Tier 2',
                            'Tier 3',
                            'Tier 4')
                            