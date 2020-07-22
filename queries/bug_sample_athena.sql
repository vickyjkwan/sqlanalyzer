SELECT *,        CASE            WHEN revenue_category = 'non_metered' THEN amortized_revenue            WHEN revenue_group = 'enterprise exception' THEN amortized_revenue            ELSE rack_rate_revenue_w_disc        END AS usage_revenue,        '2020-06-30' AS last_run_date FROM no alias  
SELECT * FROM enterprise_agg UNION ALL SELECT * FROM paygo_agg 
SELECT vdate,
          COALESCE(mbx_acct_id, 'unmapped') AS mbx_acct_id,
          sfdc_acct_id,
          sfdc_acct_name,
          opp_id,
          revenue_category,
          CASE
              WHEN ent_exception = TRUE THEN 'enterprise exception'
              ELSE 'enterprise longtail'
          END AS revenue_group,
          CASE
              WHEN service_group IN ('maps',
                                     'search',
                                     'navigation',
                                     'support',
                                     'data_services',
                                     'atlas') THEN service_group
              ELSE 'other'
          END AS service_group,
          service,
          sku_type AS metered_service_type,
          NULL AS units,
          SUM(prd_value_daily) AS amortized_revenue,
          SUM(0) AS rack_rate_revenue,
          SUM(0) AS rack_rate_revenue_w_disc
   FROM enterprise_daily
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11 

 


SELECT DATE(vdate) AS vdate,
          mbx_acct_id,
          sfdc_acct_id,
          sfdc_acct_name,
          NULL AS opp_id,
          'metered' AS revenue_category,
          CASE
              WHEN ent_exception = TRUE THEN 'enterprise exception'
              WHEN is_apa = TRUE THEN 'spp'
              WHEN mbx_acct_lvl = 'enterprise' THEN 'enterprise longtail'
              WHEN paygo_free = TRUE THEN 'paygo_free'
              ELSE 'paygo'
          END AS revenue_group,
          service_group,
          sku_id AS service,
          sku_type,
          subunits AS units,
          SUM(0) AS amortized_revenue,
          SUM(daily_revenue) AS rack_rate_revenue,
          SUM(CASE
                  WHEN is_apa = TRUE THEN daily_revenue_w_discount
                  WHEN mbx_acct_lvl = 'enterprise' THEN daily_revenue * .65
                  ELSE daily_revenue_w_discount
              END) AS rack_rate_revenue_w_disc
   FROM paygo_daily
   GROUP BY 1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11 
SELECT COUNT(*),
       SUM(usage_revenue)
FROM combined
LIMIT 13000 
SELECT *
   FROM (
         VALUES ('01t1R000005tfSqQAI',
                 'APA - Prepayment') ,('01t36000005v7iSAAQ',
                                       'Application-Old') ,('01t36000003KQCxAAO',
                                                            'Assets-Old') ,('01t1R000006RdvEQAS',
                                                                            'Bundle Product') ,('01t36000000ixRcAAI',
                                                                                                'Commercial Application - CPM') ,('01t36000005vX5jAAE',
                                                                                                                                  'Commercial Application - End Users') ,('01t36000000hyyOAAQ',
                                                                                                                                                                          'Commercial Application - End Users-Old') ,('01t36000005vX5tAAE',
                                                                                                                                                                                                                      'Commercial Application - High Value Users') ,('01t36000005v7cUAAQ',
                                                                                                                                                                                                                                                                     'Commercial Application - High Value Users-Old') ,('01t36000005vX5oAAE',
                                                                                                                                                                                                                                                                                                                        'Commercial Application - Low Value Users') ,('01t36000005vFB8AAM',
                                                                                                                                                                                                                                                                                                                                                                      'Commercial Application - Low Value Users-Old') ,('01t36000005vX5yAAE',
                                                                                                                                                                                                                                                                                                                                                                                                                        'Commercial Application - Unlimited') ,('01t3600000309BAAAY',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                'Commercial Application - Unlimited-Old') ,('01t36000005vi7hAAA',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            'Data Storage') ,('01t36000000hyydAAA',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              'Data Storage-Old') ,('01t36000005vX63AAE',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    'End User - Commercial Application - Overages') ,('01t36000003KQBVAA4',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      'End Users - Commercial Application - Overages-Old') ,('01t36000005v7ihAAA',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             'Mobile Products-Old') ,('01t1R000006bpRyQAI',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      'Tileset Storage') ,('01t36000005vg8oAAA',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           'Tracked Assets') ,('01t1R000006RcFCQA0',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               'Tracked Assets - Ratable') ,('01t36000005vg8yAAA',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             'Tracked Assets - Unlimited') ,('01t36000003L3aQAAS',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             'Tracked Assets - Unlimited-Old') ,('01t360000027vUaAAI',
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 'Tracked Assets-Old')) AS v (prd_2_id, prd_name) 
 
SELECT *
   FROM (
         VALUES ('0013600000QmuNtAAJ') ,('00136000005AxApAAK') ,('00136000005AxMbAAK') ,('0013600001KDnawAAD') ,('0011R000020WJXyQAO') ,('00136000005AxCgAAK') ,('00136000005AxElAAK') ,('0013600001KDUFDAA5') ,('00136000005AxLLAA0') ,('00136000005Ax5QAAS') ,('00136000005AxLgAAK')) AS v (sfdc_acct_id) 


SELECT rcal.vdate,
          edeals.*
   FROM analytics.ref_calendar rcal
   LEFT JOIN enterprise_deals edeals ON rcal.vdate BETWEEN edeals.prd_start_date AND edeals.prd_end_date
   WHERE rcal.vdate BETWEEN DATE('2019-06-01') AND DATE('2020-06-30') 

-- bug --
SELECT rrr.*,
          CASE
              WHEN entexc.sfdc_acct_id IS NOT NULL THEN TRUE
              ELSE FALSE
          END AS ent_exception,
          CASE
              WHEN pfree.mbx_acct_id IS NOT NULL THEN TRUE
              ELSE FALSE
          END AS paygo_free,
          meta.sku_type
   FROM analytics.rack_rate_revenue rrr
   LEFT JOIN enterprise_exception entexc ON rrr.sfdc_acct_id = entexc.sfdc_acct_id
   AND rrr.mbx_acct_lvl = 'enterprise'
   LEFT JOIN paygo_free pfree ON rrr.mbx_acct_id = pfree.mbx_acct_id
   AND pfree.date_month BETWEEN DATE_ADD('month', -1, DATE(vdate)) AND DATE_ADD('day', -1, DATE(vdate))
   LEFT JOIN analytics.sku_metadata meta ON rrr.sku_id = meta.sku_id
   WHERE rrr.dt = '2020-06-30'  

SELECT DISTINCT DATE(DATE_TRUNC('month', inv.date)) AS date_month,
                   mbxacct.id AS mbx_acct_id
   FROM payments.stripe_invoices inv
   INNER JOIN payments.stripe_discounts disc ON inv.discount_id = disc.id
   INNER JOIN payments.stripe_coupons coup ON disc.coupon_id = coup.id
   AND coup.duration IN ('forever',
                         'repeating')
   AND coup.percent_off = 100
   INNER JOIN mapbox_customer_data.accounts mbxacct ON inv.customer_id = mbxacct.customerid
   AND mbxacct.dt = '2020-06-30'
   WHERE inv.date >= DATE('2019-05-01') 


SELECT opp.id AS opp_id,
          opp.name AS opp_name,
          opp.account_id AS sfdc_acct_id,
          acct.name AS sfdc_acct_name,
          LOWER(opp.mapbox_username_c) AS mbx_acct_id,
          oprd.id AS prd_id,
          oprd.product_name_c AS prd_name,
          oprd.product_2_id AS prd_2_id,
          CASE
              WHEN pmo.prd_name IS NOT NULL THEN 'metered'
              WHEN prd.sku_id_c = 'non_metered_product' THEN 'non_metered'
              ELSE 'metered'
          END AS revenue_category,
          prd.service_organization_c AS service_group,
          meta.sku_name,
          meta.sku_type,
          prd.name,
          CASE
              WHEN pmo.prd_name IS NOT NULL THEN prd.name
              WHEN prd.sku_id_c = 'non_metered_product' THEN prd.name
              WHEN meta.sku_name IS NOT NULL THEN meta.sku_name
              ELSE prd.sku_id_c
          END AS service,
          oprd.service_date AS prd_start_date,
          oprd.end_date_c AS prd_end_date,
          oprd.product_value_c AS prd_value,
          oprd.product_value_c/(DATE_DIFF('day', oprd.service_date, oprd.end_date_c) + 1) AS prd_value_daily,
          CASE
              WHEN entexc.sfdc_acct_id IS NOT NULL THEN TRUE
              ELSE FALSE
          END AS ent_exception
   FROM sfdc.opportunities opp
   LEFT JOIN sfdc.accounts acct ON opp.account_id = acct.id
   AND acct.dt = '2020-06-30'
   LEFT JOIN sfdc.opportunity_product oprd ON opp.id = oprd.opportunity_id
   AND oprd.dt = '2020-06-30'
   LEFT JOIN sfdc.products prd ON oprd.product_2_id = prd.id
   AND prd.dt = '2020-06-30'
   LEFT JOIN product_mapping_overrides pmo ON oprd.product_2_id = pmo.prd_2_id
   LEFT JOIN enterprise_exception entexc ON opp.account_id = entexc.sfdc_acct_id
   LEFT JOIN analytics.sku_metadata meta ON prd.sku_id_c = meta.sku_id
   WHERE opp.dt = '2020-06-30'
     AND opp.stage_name = 'Won'
     AND opp.non_enterprise_c = FALSE
     AND oprd.id IS NOT NULL
     AND oprd.product_2_id != '01t1R000005tfSqQAI' 
