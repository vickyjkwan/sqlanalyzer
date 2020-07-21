-- audit: not pass

WITH opportunity_product_rev AS
  (SELECT opportunity_id,
          op.product_2_id, p.name AS product_name,
                           COALESCE(CAST(quantity AS DOUBLE) * CAST(list_price AS DOUBLE), 0) AS total_price,
                           netsuite_conn_netsuite_item_key_id_c AS netsuite_conn_net_suite_item_key_id_c,
                           COUNT(netsuite_conn_netsuite_item_key_id_c) OVER (PARTITION BY opportunity_id) AS num_netsuite_items
   FROM sfdc.opportunity_product op
   LEFT JOIN sfdc.products p ON op.product_2_id = p.id
   AND p.dt = '{run_date}'
   WHERE op.dt = '{run_date}'
     AND opportunity_id IS NOT NULL
     AND product_name_c IS NOT NULL
     AND op.is_deleted = FALSE ),

product_mapping AS
  (SELECT opportunity_id,
          op.product_name,
          COALESCE(mapped_product, 'unmapped') AS mapped_product,
          CASE
              WHEN op.opportunity_id = '0063600000B1icTAAR'
                   AND op.product_2_id = '01t36000001SzdWAAS' THEN 0.0
              WHEN op.opportunity_id = '00636000004JKS0AAO'
                   AND op.product_2_id = '01t36000000iDrwAAE' THEN 42.0
              WHEN op.opportunity_id = '00636000004JKS0AAO'
                   AND op.product_2_id = '01t36000001SzdWAAS' THEN 58.0
              WHEN op.opportunity_id = '00636000006IgpvAAC'
                   AND op.product_2_id = '01t36000000iDrwAAE' THEN 9.0
              WHEN op.opportunity_id = '00636000006IgpvAAC'
                   AND op.product_2_id = '01t36000001SzdWAAS' THEN 91.0
              WHEN op.opportunity_id = '00636000008xsoWAAQ'
                   AND op.product_2_id = '01t360000007IMZAA2' THEN 3.0
              WHEN op.opportunity_id = '00636000008xsoWAAQ'
                   AND op.product_2_id = '01t36000000iDrwAAE' THEN 79.5
              WHEN op.opportunity_id = '00636000008xsoWAAQ'
                   AND op.product_2_id = '01t36000001SzdWAAS' THEN 17.5
              WHEN op.opportunity_id = '00636000008xsoWAAQ'
                   AND op.product_2_id = '01t36000003Mn0JAAS' THEN 0.0
              WHEN op.opportunity_id = '0063600000IH8B9AAL'
                   AND op.product_2_id = '01t360000005tjBAAQ' THEN 0.2
              WHEN op.opportunity_id = '0063600000IH8B9AAL'
                   AND op.product_2_id = '01t360000007IMZAA2' THEN 9.6
              WHEN op.opportunity_id = '0063600000IH8B9AAL'
                   AND op.product_2_id = '01t36000001SzdWAAS' THEN 61.4
              WHEN op.opportunity_id = '0063600000IH8B9AAL'
                   AND op.product_2_id = '01t36000004WEfoAAG' THEN 28.3
              WHEN op.opportunity_id = '0063600000IH8B9AAL'
                   AND op.product_2_id = '01t36000004OtprAAC' THEN 0.5
              WHEN op.opportunity_id = '0061R00000ogYPlQAM'
                   AND op.product_2_id = '01t36000005vWnJAAU' THEN 13.33
              WHEN op.opportunity_id = '0061R00000ogYPlQAM'
                   AND op.product_2_id = '01t36000005vg85AAA' THEN 66.67
              WHEN op.opportunity_id = '0061R00000ogYPlQAM'
                   AND op.product_2_id = '01t36000005vX1wAAE' THEN 20.0
              ELSE total_price
          END AS list_price_value
   FROM opportunity_product_rev op
   LEFT JOIN
     (SELECT name AS product_name,
             COALESCE(MAX(service_organization_c), 'unmapped') AS mapped_product,
             COALESCE(MAX(service_organization_c), 'unmapped') AS mid_product,
             COALESCE(MAX(sku_id_c), 'unknown') AS endpoint
      FROM sfdc.products
      WHERE dt = '{run_date}'
      GROUP BY name) sm ON op.product_name = sm.product_name
   WHERE total_price > 0
     AND (netsuite_conn_net_suite_item_key_id_c IS NOT NULL
          OR num_netsuite_items = 0 ) ),

pricing_by_method AS
  (SELECT opportunity_id,
          mapped_product,
          product_name,
          SUM(list_price_value) AS list_price_value
   FROM product_mapping
   GROUP BY opportunity_id,
            mapped_product,
            product_name),

opportunity_to_name AS
  (SELECT id AS account_id,
          CONCAT_WS(',', COLLECT_SET(LOWER(name))) AS account_name
   FROM sfdc.accounts
   WHERE dt = '{run_date}'
   GROUP BY id),

opportunity_arr_tmp_org AS
  (SELECT so.account_id,
          COALESCE(account_name, so.account_id) AS account_name,
          id AS opportunity_id,
          prior_opportunity_c AS prior_opportunity_id,
          DATE_FORMAT(service_start_date_c, 'yyyy-MM-dd') AS service_start_day,
          DATE_FORMAT(CASE
                          WHEN stage_name NOT IN ('Won', '7 - ICR', 'Won - Pending')
                               AND service_start_date_c IS NOT NULL THEN service_start_date_c
                          ELSE effective_date_c
                      END, 'yyyy-MM-dd') AS effective_day, CASE
                                                               WHEN service_end_date_c IS NULL THEN ADD_MONTHS(DATE_FORMAT(service_start_date_c, 'yyyy-MM-dd'), 12)
                                                               ELSE DATE_FORMAT(service_end_date_c, 'yyyy-MM-dd')
                                                           END AS service_end_day,
                                                           stage_name,
                                                           TYPE,
                                                           COALESCE(CAST(arr_c AS DOUBLE), 0) AS arr
   FROM sfdc.opportunities so
   LEFT JOIN opportunity_to_name otn ON so.account_id = otn.account_id
   WHERE dt = '{run_date}'
     AND service_start_date_c IS NOT NULL
     AND so.account_id IS NOT NULL
     AND is_deleted = FALSE
     AND (stage_name IN ('Won',
                         '7 - ICR',
                         'Won - Pending')
          OR (TYPE = 'Renewal Business'
              AND stage_name NOT IN ('Lost',
                                     'Dead',
                                     'Closed - No Decision')))
     AND non_enterprise_c = FALSE
     AND (service_end_date_c IS NULL
          OR DATE_FORMAT(service_end_date_c, 'yyyy-MM') > DATE_FORMAT(service_start_date_c, 'yyyy-MM'))
     AND COALESCE(CAST(arr_c AS DOUBLE), 0) > 0 ),

opportunity_arr_tmp AS
  (SELECT a.account_id,
          a.account_name,
          a.opportunity_id,
          a.prior_opportunity_id,
          a.service_start_day,
          a.effective_day,
          a.service_end_day,
          a.stage_name,
          a.type,
          CASE
              WHEN a.stage_name IN ('Won',
                                    '7 - ICR',
                                    'Won - Pending') THEN a.arr
              ELSE b.arr
          END AS arr
   FROM opportunity_arr_tmp_org a
   LEFT JOIN opportunity_arr_tmp_org b ON COALESCE(a.prior_opportunity_id, '') = b.opportunity_id
   WHERE a.stage_name IN ('Won',
                          '7 - ICR',
                          'Won - Pending')
     OR (a.stage_name NOT IN ('Won',
                              '7 - ICR',
                              'Won - Pending')
         AND b.service_end_day >= DATE_SUB('{run_date}', 45) ) ),

opportunity_arr AS
  (SELECT oa.account_id,
          oa.account_name,
          oa.opportunity_id,
          oa.prior_opportunity_id,
          oa.service_start_day,
          CASE
              WHEN oa.opportunity_id IN ('0063600000VQA62AAH',
                                         '00636000004NSbIAAW',
                                         '0063600000VwjuXAAR') THEN oa.service_start_day
              ELSE oa.effective_day
          END AS effective_day,
          CASE
              WHEN DATE_FORMAT(oaa.service_start_day, 'yyyy-MM-01') <= DATE_FORMAT(oa.service_end_day, 'yyyy-MM-01') THEN ADD_MONTHS(DATE_FORMAT(oaa.service_start_day, 'yyyy-MM-01'), -1)
              ELSE oa.service_end_day
          END AS service_end_day,
          CASE
              WHEN DATE_FORMAT(oaa.service_start_day, 'yyyy-MM-01') <= DATE_FORMAT(oa.service_end_day, 'yyyy-MM-01') THEN TRUE
              ELSE FALSE
          END AS is_superceded,
          oa.stage_name,
          oa.type,
          oa.arr
   FROM opportunity_arr_tmp oa
   INNER JOIN opportunity_arr_tmp oaa ON oa.opportunity_id = oaa.prior_opportunity_id
   AND oaa.type = 'Renewal Business'
   AND oaa.prior_opportunity_id IS NOT NULL
   UNION ALL SELECT a.account_id,
                    a.account_name,
                    a.opportunity_id,
                    a.prior_opportunity_id,
                    a.service_start_day,
                    CASE
                        WHEN a.opportunity_id IN ('0063600000VQA62AAH',
                                                  '00636000004NSbIAAW',
                                                  '0063600000VwjuXAAR') THEN a.service_start_day
                        ELSE a.effective_day
                    END AS effective_day,
                    a.service_end_day,
                    FALSE AS is_superceded,
                             a.stage_name,
                             a.type,
                             a.arr
   FROM opportunity_arr_tmp a
   LEFT JOIN opportunity_arr_tmp b ON a.opportunity_id = b.prior_opportunity_id
   AND b.type = 'Renewal Business'
   AND b.prior_opportunity_id IS NOT NULL
   WHERE b.opportunity_id IS NULL ),

pricing_by_method_nw AS
  (SELECT pm.opportunity_id,
          mapped_product,
          product_name,
          list_price_value
   FROM pricing_by_method pm
   INNER JOIN opportunity_arr oa ON pm.opportunity_id = oa.opportunity_id
   WHERE oa.stage_name IN ('Won',
                           '7 - ICR',
                           'Won - Pending')
   UNION ALL SELECT a.opportunity_id, mapped_product, product_name,
                                                      list_price_value
   FROM
     (SELECT opportunity_id,
             prior_opportunity_id
      FROM opportunity_arr
      WHERE stage_name NOT IN ('Won',
                               '7 - ICR',
                               'Won - Pending')
        AND prior_opportunity_id IS NOT NULL ) a
   INNER JOIN pricing_by_method pmm ON a.prior_opportunity_id = pmm.opportunity_id),

account_product AS
  (SELECT account_id,
          COALESCE(mapped_product, 'maps') AS mapped_product,
          COALESCE(product_name, 'unknown') AS product_name, MIN(service_start_day) AS service_start_day,
                                                             MAX(service_end_day) AS service_end_day
   FROM opportunity_arr oa
   LEFT JOIN pricing_by_method_nw pm ON oa.opportunity_id = pm.opportunity_id
   GROUP BY account_id,
            mapped_product,
            product_name),

arr_by_month_dummy AS
  (SELECT am.account_id,
          COALESCE(account_name, am.account_id) AS account_name,
          mapped_product,
          product_name,
          year_month AS service_month
   FROM
     (SELECT year_month,
             'dummy' AS dummy
      FROM wbr.year_month_dummy_final) ym
   INNER JOIN
     (SELECT account_id,
             mapped_product,
             product_name,
             MAX('dummy') AS dummy,
             MIN(DATE_FORMAT(service_start_day, 'yyyy-MM-01')) AS min_month, MAX(DATE_FORMAT(ADD_MONTHS(service_end_day, 1), 'yyyy-MM-01'))AS max_month
      FROM account_product
      GROUP BY account_id,
               mapped_product,
               product_name) am ON ym.dummy = am.dummy
   LEFT JOIN opportunity_to_name otn ON am.account_id = otn.account_id
   WHERE year_month BETWEEN min_month AND max_month ),

opportunity_product AS
  (SELECT oa.*,
          COALESCE(product_name, 'unknown') AS product_name, COALESCE(CASE
                                                                          WHEN mapped_product IN ('bundled') THEN 'maps'
                                                                          ELSE mapped_product
                                                                      END, 'maps') AS mapped_product, COALESCE(list_price_value, 0) AS list_price_value,
                                                                                                      CASE
                                                                                                          WHEN product_name IS NOT NULL
                                                                                                               AND mapped_product NOT IN ('bundled') THEN COALESCE(list_price_value, 0)
                                                                                                          ELSE 0
                                                                                                      END AS product_value, COUNT(product_name) OVER (PARTITION BY oa.account_id,
                                                                                                                                                                   oa.opportunity_id) AS num_items, SUM(COALESCE(list_price_value, 0)) OVER (PARTITION BY oa.account_id,
                                                                                                                                                                                                                                                          oa.opportunity_id) AS total_value,
                                                                                                                                                                                                                                            SUM(CASE
                                                                                                                                                                                                                                                    WHEN product_name IS NOT NULL
                                                                                                                                                                                                                                                         AND mapped_product NOT IN ('bundled') THEN COALESCE(list_price_value, 0)
                                                                                                                                                                                                                                                    ELSE 0
                                                                                                                                                                                                                                                END) OVER (PARTITION BY oa.account_id,
                                                                                                                                                                                                                                                                        oa.opportunity_id) AS total_product_value
   FROM opportunity_arr oa
   LEFT JOIN pricing_by_method_nw pm ON oa.opportunity_id = pm.opportunity_id),

opp_product_share AS
  (SELECT DATE_FORMAT(service_start_day, 'yyyy-MM-01') AS service_start_month,
          DATE_FORMAT(service_end_day_r, 'yyyy-MM-01') AS service_end_month,
          a.*,
          arr * SHARE AS arr_p
   FROM
     (SELECT *,
             CASE
                 WHEN mapped_product = 'maps'
                      AND product_name = 'unknown'
                      AND num_items <= 1 THEN 1
                 WHEN (total_value > 0
                       AND total_product_value = 0) THEN list_price_value / total_value
                 WHEN total_product_value > 0 THEN product_value / total_product_value
                 ELSE 0
             END AS SHARE,
             service_end_day AS service_end_day_r
      FROM opportunity_product) a), 
      
arr_product_exp AS
  (SELECT md.account_id,
          md.account_name,
          md.product_name,
          CASE
              WHEN md.mapped_product IN ('bundled') THEN 'maps'
              ELSE md.mapped_product
          END AS mapped_product, service_month,
                                 COLLECT_SET(opportunity_id) AS opportunity_id_s, COLLECT_SET(CASE
                                                                                                  WHEN stage_name NOT IN ('Won', '7 - ICR', 'Won - Pending') THEN opportunity_id
                                                                                                  ELSE NULL
                                                                                              END) AS opportunity_id_nw, SUM(arr_p) AS arr_p
   FROM arr_by_month_dummy md
   LEFT JOIN opp_product_share ps ON md.account_id = ps.account_id
   AND md.product_name = ps.product_name
   AND md.service_month BETWEEN ps.service_start_month AND ps.service_end_month
   GROUP BY md.account_id,
            md.account_name,
            md.product_name,
            md.mapped_product,
            service_month),
                                    arr_product_prev AS
  (SELECT account_id,
          account_name,
          mapped_product,
          product_name,
          service_month,
          COALESCE(ROUND(arr_p, 2), 0) AS arr_p,
          COALESCE(LAG(ROUND(arr_p, 2)) OVER (PARTITION BY account_id, product_name
                                              ORDER BY service_month ASC), 0) AS prev_arr_p,
          opportunity_id_s AS opportunity_id,
          CONCAT_WS(',', opportunity_id_s) AS opportunity_id_p,
          LAG(CONCAT_WS(',', opportunity_id_s)) OVER (PARTITION BY account_id,
                                                                   product_name
                                                      ORDER BY service_month ASC) AS prev_opportunity_id_p,
                                                     CONCAT_WS(',', opportunity_id_nw) AS opportunity_id_nw
   FROM arr_product_exp),
                                    arr_product_status AS
  (SELECT account_id,
          account_name,
          mapped_product,
          product_name,
          service_month,
          CASE
              WHEN arr_p > 0
                   AND prev_arr_p = 0 THEN 'new'
              WHEN arr_p = 0
                   AND prev_arr_p > 0 THEN 'churn'
              WHEN arr_p > 0
                   AND arr_p < prev_arr_p THEN 'contraction'
              WHEN arr_p > 0
                   AND arr_p > prev_arr_p THEN 'expansion'
              WHEN arr_p > 0
                   AND arr_p = prev_arr_p
                   AND opportunity_id_p <> prev_opportunity_id_p THEN 'renewal'
              WHEN arr_p > 0
                   AND arr_p = prev_arr_p
                   AND opportunity_id_p = prev_opportunity_id_p THEN 'active'
              WHEN arr_p = 0
                   AND prev_arr_p = 0 THEN 'not_active'
              ELSE 'unknown'
          END AS product_status,
          arr_p,
          prev_arr_p,
          opportunity_id,
          opportunity_id_p,
          prev_opportunity_id_p,
          opportunity_id_nw
   FROM arr_product_prev),
                                    arr_product_status_mid AS
  (SELECT COALESCE(mid_product, 'unmapped') AS mid_product,
          ps.*
   FROM arr_product_status ps
   LEFT JOIN wbr.product_service_mapping sm ON ps.product_name = sm.product_name),
                                    

arr_account AS
  (SELECT account_id,
          service_month,
          arr_a,
          prev_arr_a,
          CASE
              WHEN arr_a > 0
                   AND prev_arr_a = 0 THEN 'new'
              WHEN arr_a = 0
                   AND prev_arr_a > 0 THEN 'churn'
              WHEN arr_a > 0
                   AND arr_a < prev_arr_a THEN 'contraction'
              WHEN arr_a > 0
                   AND arr_a > prev_arr_a THEN 'expansion'
              WHEN arr_a > 0
                   AND arr_a = prev_arr_a
                   AND opportunity_id_a <> prev_opportunity_id_a THEN 'renewal'
              WHEN arr_a > 0
                   AND arr_a = prev_arr_a
                   AND opportunity_id_a = prev_opportunity_id_a THEN 'active'
              WHEN arr_a = 0
                   AND prev_arr_a = 0 THEN 'not_active'
              ELSE 'unknown'
          END AS account_status
   FROM
     (SELECT account_id,
             service_month,
             ROUND(arr_a, 2) AS arr_a,
             ROUND(COALESCE(LAG(arr_a) OVER (PARTITION BY account_id
                                             ORDER BY service_month ASC), 0), 2) AS prev_arr_a,
             opportunity_id_a,
             prev_opportunity_id_a
      FROM
          (SELECT account_id,
                service_month,
                SUM(arr_a) OVER (PARTITION BY account_id
                                 ORDER BY service_month ASC) AS arr_a, CONCAT_WS(',', opportunity_id_a) AS opportunity_id_a,
                                                                       LAG(CONCAT_WS(',', opportunity_id_a)) OVER (PARTITION BY account_id
                                                                                                                   ORDER BY service_month ASC) AS prev_opportunity_id_a
         FROM
           (SELECT aa.account_id,
                   aa.service_month,
                   arr_a,
                   opportunity_id_a
            FROM
              (SELECT account_id,
                      service_month,
                      SUM(COALESCE(arr_p, 0) - COALESCE(prev_arr_p, 0)) AS arr_a
               FROM arr_product_status_mid
               GROUP BY account_id,
                        service_month) aa
            LEFT JOIN
              (SELECT account_id,
                      service_month,
                      COLLECT_SET(opp) AS opportunity_id_a
               FROM arr_product_status_mid LATERAL VIEW EXPLODE (opportunity_id) t AS opp
               GROUP BY account_id,
                        service_month) bb ON aa.account_id = bb.account_id
            AND aa.service_month = bb.service_month) 
            a) 
            
          b)          
     c),
                                    
arr_mapped_product AS
  (SELECT account_id,
          mapped_product,
          service_month,
          arr_m,
          prev_arr_m,
          CASE
              WHEN arr_m > 0
                   AND prev_arr_m = 0 THEN 'new'
              WHEN arr_m = 0
                   AND prev_arr_m > 0 THEN 'churn'
              WHEN arr_m > 0
                   AND arr_m < prev_arr_m THEN 'contraction'
              WHEN arr_m > 0
                   AND arr_m > prev_arr_m THEN 'expansion'
              WHEN arr_m > 0
                   AND arr_m = prev_arr_m
                   AND opportunity_id_m <> prev_opportunity_id_m THEN 'renewal'
              WHEN arr_m > 0
                   AND arr_m = prev_arr_m
                   AND opportunity_id_m = prev_opportunity_id_m THEN 'active'
              WHEN arr_m = 0
                   AND prev_arr_m = 0 THEN 'not_active'
              ELSE 'unknown'
          END AS mapped_status
   FROM
     (SELECT account_id,
             mapped_product,
             service_month,
             ROUND(arr_m, 2) AS arr_m,
             ROUND(COALESCE(LAG(arr_m) OVER (PARTITION BY account_id, mapped_product
                                             ORDER BY service_month ASC), 0), 2) AS prev_arr_m,
             opportunity_id_m,
             prev_opportunity_id_m
      FROM
        (SELECT account_id,
                mapped_product,
                service_month,
                SUM(arr_m) OVER (PARTITION BY account_id,
                                              mapped_product
                                 ORDER BY service_month ASC) AS arr_m,
                                CONCAT_WS(',', opportunity_id_m) AS opportunity_id_m,
                                LAG(CONCAT_WS(',', opportunity_id_m)) OVER (PARTITION BY account_id,
                                                                                         mapped_product
                                                                            ORDER BY service_month ASC) AS prev_opportunity_id_m
         FROM
           (SELECT aa.account_id,
                   aa.service_month,
                   aa.mapped_product,
                   arr_m,
                   opportunity_id_m
            FROM
              (SELECT account_id,
                      mapped_product,
                      service_month,
                      SUM(COALESCE(arr_p, 0) - COALESCE(prev_arr_p, 0)) AS arr_m
               FROM arr_product_status_mid
               GROUP BY account_id,
                        mapped_product,
                        service_month) aa
            LEFT JOIN
              (SELECT account_id,
                      mapped_product,
                      service_month,
                      COLLECT_SET(opp) AS opportunity_id_m
               FROM arr_product_status_mid LATERAL VIEW EXPLODE (opportunity_id) t AS opp
               GROUP BY account_id,
                        mapped_product,
                        service_month) bb ON aa.account_id = bb.account_id
            AND aa.service_month = bb.service_month
            AND aa.mapped_product = bb.mapped_product) a) b) c),
                                    
arr_mid_product AS
  (SELECT account_id,
          mid_product,
          service_month,
          arr_mid,
          prev_arr_mid,
          CASE
              WHEN arr_mid > 0
                   AND prev_arr_mid = 0 THEN 'new'
              WHEN arr_mid = 0
                   AND prev_arr_mid > 0 THEN 'churn'
              WHEN arr_mid > 0
                   AND arr_mid < prev_arr_mid THEN 'contraction'
              WHEN arr_mid > 0
                   AND arr_mid > prev_arr_mid THEN 'expansion'
              WHEN arr_mid > 0
                   AND arr_mid = prev_arr_mid
                   AND opportunity_id_mid <> prev_opportunity_id_mid THEN 'renewal'
              WHEN arr_mid > 0
                   AND arr_mid = prev_arr_mid
                   AND opportunity_id_mid = prev_opportunity_id_mid THEN 'active'
              WHEN arr_mid = 0
                   AND prev_arr_mid = 0 THEN 'not_active'
              ELSE 'unknown'
          END AS mid_status
   FROM
     (SELECT account_id,
             mid_product,
             service_month,
             ROUND(arr_mid, 2) AS arr_mid,
             ROUND(COALESCE(LAG(arr_mid) OVER (PARTITION BY account_id, mid_product
                                               ORDER BY service_month ASC), 0), 2) AS prev_arr_mid,
             opportunity_id_mid,
             prev_opportunity_id_mid
      FROM
        (SELECT account_id,
                mid_product,
                service_month,
                SUM(arr_mid) OVER (PARTITION BY account_id,
                                                mid_product
                                   ORDER BY service_month ASC) AS arr_mid,
                                  CONCAT_WS(',', opportunity_id_mid) AS opportunity_id_mid,
                                  LAG(CONCAT_WS(',', opportunity_id_mid)) OVER (PARTITION BY account_id,
                                                                                             mid_product
                                                                                ORDER BY service_month ASC) AS prev_opportunity_id_mid
         FROM
           (SELECT aa.account_id,
                   aa.service_month,
                   aa.mid_product,
                   arr_mid,
                   opportunity_id_mid
            FROM
              (SELECT account_id,
                      mid_product,
                      service_month,
                      SUM(COALESCE(arr_p, 0) - COALESCE(prev_arr_p, 0)) AS arr_mid
               FROM arr_product_status_mid
               GROUP BY account_id,
                        mid_product,
                        service_month) aa
            LEFT JOIN
              (SELECT account_id,
                      mid_product,
                      service_month,
                      COLLECT_SET(opp) AS opportunity_id_mid
               FROM arr_product_status_mid LATERAL VIEW EXPLODE (opportunity_id) t AS opp
               GROUP BY account_id,
                        mid_product,
                        service_month) bb ON aa.account_id = bb.account_id
            AND aa.service_month = bb.service_month
            AND aa.mid_product = bb.mid_product) a) b) c),
                                    
arr_full_status AS
  (SELECT ps.service_month,
          ps.account_id,
          account_name,
          ps.mapped_product,
          ps.mid_product,
          product_name,
          account_status,
          mapped_status,
          mid_status,
          product_status,
          arr_p - prev_arr_p AS arr_p,
          arr_p AS cum_arr_p,
          prev_arr_p AS prev_cum_arr_p, opportunity_id_p,
                                        prev_opportunity_id_p,
                                        opportunity_id_nw
   FROM arr_product_status_mid ps
   INNER JOIN arr_account aa ON ps.account_id = aa.account_id
   AND ps.service_month = aa.service_month
   INNER JOIN arr_mapped_product mp ON ps.account_id = mp.account_id
   AND ps.service_month = mp.service_month
   AND ps.mapped_product = mp.mapped_product
   INNER JOIN arr_mid_product md ON ps.account_id = md.account_id
   AND ps.service_month = md.service_month
   AND ps.mid_product = md.mid_product)

SELECT service_month,
       account_id,
       account_name,
       mapped_product,
       mid_product,
       product_name,
       account_status,
       mapped_status,
       mid_status,
       product_status,
       arr_p,
       cum_arr_p,
       prev_cum_arr_p,
       opportunity_id_p,
       prev_opportunity_id_p,
       opportunity_id_nw
FROM arr_full_status
