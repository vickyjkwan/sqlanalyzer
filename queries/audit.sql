WITH weekly_usage_data AS (
SELECT DATE_TRUNC('week', DATE(stats.dt)) AS week_date,
       stats.account AS mbx_acct_id,
       mbxmap.salesforce_account_id AS sfdc_acct_id,
       stats.sku_id AS service,
       SUM(stats.subunits) AS subunits
FROM sku.daily_by_account stats
LEFT JOIN sfdc.mapbox_accounts mbxmap ON stats.account = mbxmap.mapbox_account_id
AND mbxmap.dt =
  (SELECT MAX(dt)
   FROM sfdc.mapbox_accounts)
WHERE DATE(stats.dt) BETWEEN DATE_ADD('week', -24, DATE_TRUNC('week', DATE_ADD('day', -1, CURRENT_DATE))) AND DATE_ADD('day', -1, DATE_TRUNC('week', DATE_ADD('day', -1, CURRENT_DATE))) ,
                                                                                                              weeks_in_period AS
    (SELECT DISTINCT DATE_TRUNC('week', vdate) AS week_date
     FROM dwh_utils.ref_calendar
     WHERE vdate BETWEEN DATE_ADD('week', -24, DATE_TRUNC('week', DATE_ADD('day', -1, CURRENT_DATE))) AND DATE_ADD('day', -1, DATE_TRUNC('week', DATE_ADD('day', -1, CURRENT_DATE))) ) ,
                                                                                                              filled_weekly_usage_data AS
    (SELECT asl.mbx_acct_id,
            asl.sfdc_acct_id,
            asl.service,
            wip.week_date,
            COALESCE(wus.subunits, 0) AS subunits
     FROM
       (SELECT DISTINCT mbx_acct_id,
                        sfdc_acct_id,
                        service
        FROM weekly_usage_data) asl
     CROSS JOIN weeks_in_period wip
     LEFT JOIN weekly_usage_data wus ON asl.mbx_acct_id = wus.mbx_acct_id
     AND asl.service = wus.service
     AND wip.week_date = wus.week_date) ,
                                                                                                              weekly_usage_data_enhanced AS (
  SELECT fwud.sfdc_acct_id,
         acct.name AS sfdc_acct_name,
         acct.customer_tier_c AS sfdc_acct_tier,
         acct.account_health_c AS sfdc_acct_health,
         acct.TYPE AS sfdc_acct_type,
         acct.segmentation_c AS SEGMENT,
         tam.manager_c AS tam_mgr_name,
         tam.name AS tam_name,
         fwud.mbx_acct_id,
         fwud.week_date,
         sem.service_group,
         fwud.service,
         fwud.subunits AS requests,
         DATE_DIFF('week', DATE_TRUNC('week', DATE_ADD('day', -1, DATE_TRUNC('week', DATE_ADD('day', -1, CURRENT_DATE)))), fwud.week_date) AS week_offset
  FROM filled_weekly_usage_data fwud
  LEFT JOIN sfdc.accounts acct ON fwud.sfdc_acct_id = acct.id
  AND acct.dt =
    (SELECT MAX(dt)
     FROM sfdc.accounts)
  LEFT JOIN sfdc.users tam ON acct.csm_c = tam.id
  AND tam.dt =
    (SELECT MAX(dt)
     FROM sfdc.users)
  LEFT JOIN
    (SELECT DISTINCT service,
                     service_group
     FROM wbr.service_endpoint_mapping_sfdc) sem ON fwud.service = sem.service WHERE tam.id IS NOT NULL
  SELECT *
  FROM weekly_usage_data_enhanced