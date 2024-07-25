with edw_vw_pop6_analysis_active_data as(
    select * from {{ ref('aspedw_integration__edw_vw_pop6_analysis_active_data') }}
),
transformed as(
    SELECT edw_vw_pop6_analysis_active_data.audit_form
        ,edw_vw_pop6_analysis_active_data.audit_form_id
        ,edw_vw_pop6_analysis_active_data.brand_l4
        ,edw_vw_pop6_analysis_active_data.business_unit_name
        ,edw_vw_pop6_analysis_active_data.cancellation_note
        ,edw_vw_pop6_analysis_active_data.cancellation_reason
        ,edw_vw_pop6_analysis_active_data.channel
        ,edw_vw_pop6_analysis_active_data.company
        ,edw_vw_pop6_analysis_active_data.customer
        ,edw_vw_pop6_analysis_active_data.data_type
        ,edw_vw_pop6_analysis_active_data.display_code
        ,edw_vw_pop6_analysis_active_data.display_comments
        ,edw_vw_pop6_analysis_active_data.display_name
        ,edw_vw_pop6_analysis_active_data.display_order
        ,edw_vw_pop6_analysis_active_data.display_type
        ,edw_vw_pop6_analysis_active_data.field_code
        ,edw_vw_pop6_analysis_active_data.field_id
        ,edw_vw_pop6_analysis_active_data.field_label
        ,edw_vw_pop6_analysis_active_data.franchise_l3
        ,edw_vw_pop6_analysis_active_data.msl_rank
        ,edw_vw_pop6_analysis_active_data.platform_l6
        ,edw_vw_pop6_analysis_active_data.pop_name
        ,edw_vw_pop6_analysis_active_data.product_attribute
        ,edw_vw_pop6_analysis_active_data.ps_category
        ,edw_vw_pop6_analysis_active_data.ps_segment
        ,edw_vw_pop6_analysis_active_data.regional_franchise_l2
        ,edw_vw_pop6_analysis_active_data.response
        ,edw_vw_pop6_analysis_active_data.retail_environment_ps
        ,edw_vw_pop6_analysis_active_data.sales_group_name
        ,edw_vw_pop6_analysis_active_data.sku
        ,edw_vw_pop6_analysis_active_data.sku_code
        ,edw_vw_pop6_analysis_active_data.sku_english
        ,edw_vw_pop6_analysis_active_data.sub_category_l5
        ,edw_vw_pop6_analysis_active_data.visit_date
        ,edw_vw_pop6_analysis_active_data."y/n_flag"
        ,edw_vw_pop6_analysis_active_data.mkt_share
    FROM edw_vw_pop6_analysis_active_data
    WHERE ((edw_vw_pop6_analysis_active_data.cntry_cd)::TEXT = 'JP'::TEXT)
)
select * from transformed