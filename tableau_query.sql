SELECT *
FROM
  (SELECT *
   FROM
     (WITH reg_users AS
        (SELECT *
         FROM
           (SELECT a.* ,
                   b.* ,
                   c.* ,
                   d.*
            FROM
              (SELECT DISTINCT anonymous_id ,
                               user_id
               FROM mapbox_customer_data.segment_identifies
               WHERE dt >= '2018-07-01'
                 AND anonymous_id IS NOT NULL
                 AND user_id IS NOT NULL ) a
            LEFT JOIN
              (SELECT id ,
                      email ,
                      created
               FROM mapbox_customer_data.accounts
               WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY ) b ON a.user_id = b.id
            LEFT JOIN
              (SELECT anonymous_id AS anon_id_ad ,
                      context_campaign_name ,
                      min(TIMESTAMP) AS min_exposure
               FROM mapbox_customer_data.segment_pages
               WHERE dt >= '2018-07-01'
                 AND context_campaign_name IS NOT NULL
               GROUP BY 1,
                        2) c ON a.anonymous_id = c.anon_id_ad
            LEFT JOIN
              (SELECT DISTINCT anonymous_id AS anon_id_event ,
                               original_timestamp ,
                               event ,
                               context_traits_email
               FROM mapbox_customer_data.segment_tracks
               WHERE dt >= '2018-07-01'
                 AND event LIKE 'submitted_%form'
                 AND context_traits_email IS NOT NULL ) d ON a.anonymous_id = d.anon_id_event)
         WHERE context_campaign_name IS NOT NULL ),


           non_reg_users AS
        (SELECT context_campaign_name ,
                min_exposure ,
                event ,
                original_timestamp AS event_timestamp ,
                context_traits_email AS event_email
         FROM
           (SELECT a.* ,
                   b.*
            FROM
              (SELECT anonymous_id AS anon_id_ad ,
                      context_campaign_name ,
                      min(original_timestamp) AS min_exposure
               FROM
                 (SELECT context_campaign_name ,
                         anonymous_id ,
                         original_timestamp
                  FROM mapbox_customer_data.segment_pages
                  WHERE dt >= '2018-07-01'
                    AND context_campaign_name IS NOT NULL )
               GROUP BY 1,
                        2) a
            LEFT JOIN
              (SELECT DISTINCT anonymous_id AS anon_id_event ,
                               original_timestamp ,
                               event ,
                               context_traits_email
               FROM mapbox_customer_data.segment_tracks
               WHERE dt >= '2018-07-01'
                 AND event LIKE 'submitted_%form'
                 AND context_traits_email IS NOT NULL ) b ON a.anon_id_ad = b.anon_id_event)
         WHERE anon_id_event IS NOT NULL
           AND to_unixtime(min_exposure) <= to_unixtime(original_timestamp)
           AND cast(min_exposure AS DATE) >= cast(original_timestamp AS DATE) - INTERVAL '28' DAY ),


           mql_flag AS
        (SELECT email ,
                created_date ,
                last_mql_date_c ,
                mql_flag
         FROM
           (SELECT email ,
                   min(created_date) created_date ,
                   max(last_mql_date_c) last_mql_date_c ,
                   CASE
                       WHEN max(last_mql_date_c) IS NOT NULL THEN 1
                       ELSE 0
                   END AS mql_flag ,
                   sum(CASE
                           WHEN is_deleted = TRUE THEN 1
                           ELSE 0
                       END) AS is_deleted
            FROM sales.salesforce_leads
            WHERE cast(dt AS DATE) = CURRENT_DATE - INTERVAL '1' DAY
            GROUP BY 1)
         WHERE mql_flag = 1
           AND is_deleted = 0 ),


           cleaned_list AS
        (SELECT DISTINCT *
         FROM
           (SELECT context_campaign_name ,
                   min_exposure ,
                   'created_an_account' AS event ,
                   created AS event_timestamp ,
                   email AS event_email
            FROM reg_users
            WHERE to_unixtime(min_exposure) <= to_unixtime(created)
              AND cast(min_exposure AS DATE) >= cast(created AS DATE) - INTERVAL '28' DAY
            UNION ALL SELECT context_campaign_name ,
                             min_exposure ,
                             event ,
                             original_timestamp AS event_timestamp ,
                             context_traits_email AS event_email
            FROM reg_users
            WHERE to_unixtime(min_exposure) <= to_unixtime(original_timestamp)
              AND cast(min_exposure AS DATE) >= cast(original_timestamp AS DATE) - INTERVAL '28' DAY
            UNION ALL SELECT *
            FROM non_reg_users)) 
            
            
            SELECT a.* ,
                   b.*
      FROM cleaned_list a
      LEFT JOIN mql_flag b ON a.event_email = b.email) custom_sql_query
   LIMIT 0) T
LIMIT 0
