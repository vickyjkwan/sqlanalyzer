from sqlanalyzer import column_parser, unbundle, query_analyzer
import sqlparse
import re, json, time, sys
import pandas as pd


def extract_subquery_fields(query, db_fields):
    formatter = column_parser.Parser(query)
    formatted = formatter.format_query(query)
    fields = formatter.match_queried_fields(formatted, db_fields)
    return fields


def unnest_query_list(query_list):
    preprocess_list = []
    
    for q in query_list:
        for _, query in q.items():
            
            if isinstance(query, str):
                preprocess_list.append(query)
            else:
                
                for sub_q in query:
                    sub_list = []
                    for _, sub_query in sub_q.items():
                        
                        if isinstance(sub_query, str):
                            sub_list.append(sub_query)
                            
                        else:
                            for sub_sub_q in sub_query:
                                for _, sub_sub_query in sub_sub_q.items():
                                    
                                    if isinstance(sub_sub_query, str):
                                        sub_list.append(sub_sub_query)
                                        
                    preprocess_list.extend(sub_list)
                    
    return preprocess_list


if __name__ == '__main__':
    
    t0 = time.perf_counter()
    raw_query = open('queries/{}.sql'.format(sys.argv[1])).read()

    # formatting and analyze the query structure
    formatter = column_parser.Parser(raw_query)
    formatted = formatter.format_query(raw_query)
    analyzer = query_analyzer.Analyzer(formatted)
    query_dict = analyzer.parse_query(formatted)

    # preprocess query breakdowns
    preprocess_list = unnest_query_list(query_dict)

    # getting metastore
    db_fields_1 = pd.DataFrame({'db_table': 'sfdc.opportunity_product', 
            'all_columns': ['actual_quantity_c',
                    'annual_list_price_value_c',
                    'annual_product_value_c',
                    'annual_recurring_revenue_c',
                    'contract_is_12_months_or_more_c',
                    'created_by_id',
                    'created_date',
                    'description',
                    'discount_c',
                    'end_date_c',
                    'final_year_of_contract_c',
                    'id',
                    'invoice_schedule_c',
                    'is_deleted',
                    'last_modified_by_id',
                    'last_modified_date',
                    'line_family_c',
                    'list_price',
                    'list_price_value_c',
                    'monthly_recurring_revenue_c',
                    'name',
                    'netsuite_conn_netsuite_item_id_import_c',
                    'netsuite_conn_netsuite_item_key_id_c',
                    'netsuite_conn_pushed_from_netsuite_c',
                    'netsuite_conn_start_date_c',
                    'opp_end_date_lineitem_end_date_c',
                    'opportunity_id',
                    'opportunity_product_line_types_c',
                    'opportunity_service_days_c',
                    'overage_price_c',
                    'pricebook_entry_id',
                    'product_2_id',
                    'product_code',
                    'product_family_c',
                    'product_name_c',
                    'product_value_c',
                    'quantity',
                    'roll_up_summary_years_c',
                    'service_date',
                    'service_days_c',
                    'service_year_c',
                    'service_year_to_text_c',
                    'system_modstamp',
                    'time_fetched_from_salesforce',
                    'total_price',
                    'unit_price',
                    'update_everything_c',
                    'x18_digit_opportunity_id_c',
                    'dt']})

    db_fields_2 = pd.DataFrame({'db_table': 'sfdc.products', 
            'all_columns': ['availability_c',
                    'billing_type_c',
                    'cpm_product_c',
                    'created_date',
                    'exempt_api_calls_c',
                    'family',
                    'id',
                    'implementing_sdks_c',
                    'is_active',
                    'is_deleted',
                    'launch_date_c',
                    'name',
                    'netsuite_conn_celigo_update_c',
                    'netsuite_conn_item_category_c',
                    'netsuite_conn_netsuite_id_c',
                    'netsuite_conn_sub_type_c',
                    'pql_usage_tier_c',
                    'product_code',
                    'product_id_c',
                    'service_organization_c',
                    'sku_id_c',
                    'volume_discount_c',
                    'dt']})

    db_fields_3 = pd.DataFrame({'db_table': 'sfdc.accounts', 
            'all_columns': ['account_health_c',
                    'account_health_flag_c',
                    'account_health_last_touch_c',
                    'account_notes_c',
                    'account_owner_c',
                    'account_owner_id_c',
                    'account_segment_c',
                    'account_source',
                    'account_start_date_c',
                    'account_tier_c',
                    'add_company_tags_single_c',
                    'annual_revenue',
                    'billing_city',
                    'billing_country',
                    'billing_postal_code',
                    'billing_state',
                    'billing_street',
                    'churned_date_c',
                    'created_by_id',
                    'created_date',
                    'crunchbase_funding_c',
                    'csm_c',
                    'customer_tier_c',
                    'domain_c',
                    'dscorgpkg_lead_source_c',
                    'dscorgpkg_naics_codes_c',
                    'dscorgpkg_sic_codes_c',
                    'finance_arr_c',
                    'github_issue_ticket_c',
                    'health_update_c',
                    'id',
                    'industry',
                    'industry_group_c',
                    'industry_sector_c',
                    'initial_deal_arr_c',
                    'initial_deal_date_c',
                    'is_deleted',
                    'last_activity_date',
                    'last_modified_date',
                    'lfbn_account_domain_c',
                    'lost_opportunities_c',
                    'lost_renewals_c',
                    'mapbox_username_c',
                    'naics_code_c',
                    'name',
                    'netsuite_conn_channel_tier_c',
                    'next_renewal_date_c',
                    'number_of_employees',
                    'number_of_mapbox_users_c',
                    'open_opportunities_c',
                    'open_renewals_c',
                    'owner_id',
                    'owner_role_c',
                    'parent_id',
                    'partner_status_c',
                    'partner_type_c',
                    'primary_contact_c',
                    'primary_use_case_c',
                    'rating',
                    'record_type_id',
                    'region_c',
                    'renewal_manager_c',
                    'sb_pf_company_c',
                    'sdr_c',
                    'segmentation_c',
                    'shipping_city',
                    'shipping_country',
                    'shipping_postal_code',
                    'shipping_state',
                    'shipping_street',
                    'sic',
                    'solution_engineer_c',
                    'sub_industry_c',
                    'sub_region_c',
                    'support_engineer_c',
                    'type',
                    'vertical_c',
                    'vertical_formula_c',
                    'won_opportunities_c',
                    'x18_digit_account_id_c',
                    'zendesk_result_c',
                    'zendesk_zendesk_organization_c',
                    'zendesk_zendesk_organization_id_c',
                    'zisf_zoominfo_industry_c',
                    'dt']})

    db_fields_4 = pd.DataFrame({'db_table': 'sfdc.opportunities', 
            'all_columns': ['account_id',
                    'add_company_tag_c',
                    'add_use_cases_c',
                    'admin_churn_fc_override_c',
                    'agenda_c',
                    'amount',
                    'arr_c',
                    'authority_c',
                    'authority_detail_c',
                    'autorenewal_c',
                    'average_contract_value_acv_c',
                    'billing_entity_c',
                    'budget_in_usd_c',
                    'business_goals_notes_c',
                    'campaign_id',
                    'churn_acv_c',
                    'churn_arr_c',
                    'close_date',
                    'commit_flag_c',
                    'compelling_event_c',
                    'confirm_enterprise_requirements_c',
                    'contract_signed_c',
                    'contracted_expansion_c',
                    'contraction_acv_c',
                    'country_c',
                    'created_by_id',
                    'created_by_role_c',
                    'created_date',
                    'csm_c',
                    'customer_presentation_date_c',
                    'customer_value_prop_c',
                    'department_c',
                    'economic_buyer_identified_c',
                    'effective_date_c',
                    'effective_date_mgr_c',
                    'effective_date_vp_c',
                    'estimated_annual_revenue_c',
                    'exit_arr_c',
                    'expected_close_date_c',
                    'expected_launch_date_c',
                    'final_confirmation_on_triptik_c',
                    'final_documents_sent_c',
                    'forecast_category',
                    'forecast_category_name',
                    'forecasted_churn_reportable_c',
                    'gclid_c',
                    'gclid_date_c',
                    'github_ticket_c',
                    'id',
                    'inbound_message_c',
                    'interested_in_c',
                    'is_closed',
                    'is_deleted',
                    'is_split',
                    'is_won',
                    'last_activity_date',
                    'last_modified_date',
                    'last_referenced_date',
                    'last_trip_tik_update_c',
                    'latest_hand_off_date_c',
                    'lead_source',
                    'lead_source_detail_c',
                    'lost_because_c',
                    'lost_because_competitor_list_c',
                    'lost_because_detail_c',
                    'lost_because_notes_c',
                    'lost_date_c',
                    'mapbox_service_owner_c',
                    'mapbox_username_c',
                    'name',
                    'need_detail_c',
                    'need_notes_sdr_c',
                    'need_sdr_c',
                    'net_new_arr_c',
                    'net_new_arr_forecast_c',
                    'net_new_arr_forecast_mgr_c',
                    'net_new_arr_forecast_vp_c',
                    'netsuite_conn_bill_to_tier_c',
                    'netsuite_conn_current_sales_order_id_c',
                    'netsuite_conn_netsuite_sales_order_number_c',
                    'netsuite_conn_ship_to_tier_c',
                    'new_acv2019_c',
                    'next_step',
                    'next_step_c',
                    'next_step_date_c',
                    'next_steps_new_c',
                    'non_enterprise_c',
                    'notes_c',
                    'objectives_c',
                    'opp_renewal_risk_c',
                    'opp_renewal_risk_flag_c',
                    'opportunity_count_c',
                    'opportunity_owner_id_c',
                    'opportunity_product_lines_c',
                    'opportunity_segment_c',
                    'original_renewal_date_c',
                    'other_use_case_c',
                    'owner_id',
                    'owner_role_c',
                    'partner_reseller_c',
                    'poc_kick_off_date_c',
                    'pricebook_2_id',
                    'primary_competitor_c',
                    'primary_use_case_c',
                    'primary_use_case_sdr_c',
                    'prior_amount_c',
                    'prior_close_date_c',
                    'prior_opportunity_c',
                    'prior_opportunity_service_end_date_c',
                    'prior_stage_c',
                    'probability',
                    'product_acv_c',
                    'qualified_by_c',
                    'record_type_id',
                    'renewal_acv_c',
                    'renewal_arr_c',
                    'renewal_arr_override_c',
                    'renewal_deadline_c',
                    'renewal_health_c',
                    'renewal_manager_c',
                    'renewal_new_agreement_c',
                    'requires_legal_c',
                    'sal_date_c',
                    'sales_engineer_c',
                    'sales_forecast_mgr_c',
                    'sales_forecast_vp_c',
                    'sales_manager_forecast_last_updated_on_c',
                    'sales_manager_forecast_updated_manually_c',
                    'sales_rep_forecast_last_updated_on_c',
                    'sales_to_cs_hand_off_c',
                    'se_github_ticket_c',
                    'service_days_c',
                    'service_end_date_c',
                    'service_start_date_c',
                    'service_years_c',
                    'shipping_entity_c',
                    'stage_0_date_c',
                    'stage_1_date_c',
                    'stage_2_date_c',
                    'stage_3_date_c',
                    'stage_4_date_c',
                    'stage_5_date_c',
                    'stage_6_date_c',
                    'stage_7_date_c',
                    'stage_change_date_c',
                    'stage_duration_c',
                    'stage_name',
                    'stakeholder_identified_c',
                    'sub_vertical_c',
                    'tcp_confirmed_with_buyer_c',
                    'tcp_customer_tech_signoff_c',
                    'tcp_end_date_c',
                    'tcp_entered_evaluation_status_c',
                    'tcp_entered_review_status_c',
                    'tcp_lost_because_c',
                    'tcp_products_used_c',
                    'tcp_risks_c',
                    'tcp_solution_architecture_url_c',
                    'tcp_solution_fit_score_c',
                    'tcp_solution_notes_c',
                    'tcp_start_date_c',
                    'tcp_status_c',
                    'tcp_tech_owner_c',
                    'technical_goals_notes_c',
                    'territory_2_id',
                    'total_contract_value_tcv_c',
                    'trip_tik_created_c',
                    'trip_tik_url_c',
                    'type',
                    'vertical_c',
                    'vertical_formula_c',
                    'vp_forecast_updated_manually_c',
                    'weighted_arr_c',
                    'won_date_c',
                    'x18_digit_opportunity_id_c',
                    'years_c',
                    'churn_code_c',
                    'churn_sub_code_c',
                    'dt']})


    db_fields_5 = pd.DataFrame({'db_table': 'wbr.year_month_dummy_final', 
            'all_columns': ['year_month']})
        
    db_fields_6 = pd.DataFrame({'db_table': 'wbr.product_service_mapping', 
            'all_columns': ['product_name', 'mapped_product', 'mid_product', 'endpoint']})

    db_fields = db_fields_1.append(db_fields_2, ignore_index=True)
    db_fields = db_fields.append(db_fields_3, ignore_index=True)
    db_fields = db_fields.append(db_fields_4, ignore_index=True)
    db_fields = db_fields.append(db_fields_5, ignore_index=True)
    db_fields = db_fields.append(db_fields_6, ignore_index=True)        
    
    # retrieve columns
    col_list = []
    for query in preprocess_list:
        col_list.extend(extract_subquery_fields(query, db_fields))
    
    print(col_list)

    t1 = time.perf_counter()
    print(t1-t0)
