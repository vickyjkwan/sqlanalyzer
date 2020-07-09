SELECT a.*,
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