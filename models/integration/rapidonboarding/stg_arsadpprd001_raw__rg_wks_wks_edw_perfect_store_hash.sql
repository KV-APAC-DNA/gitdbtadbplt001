with

source as (

    select *
    from {{ source('arsadpprd001_raw', 'rg_wks_wks_edw_perfect_store_hash') }}

),

renamed as (

    select
        "hashkey" as hashkey,
        "hash_row" as hash_row,
        "dataset" as dataset,
        "customerid" as customerid,
        "salespersonid" as salespersonid,
        "visitid" as visitid,
        "questiontext" as questiontext,
        "productid" as productid,
        "kpi" as kpi,
        "scheduleddate" as scheduleddate,
        "latestdate" as latestdate,
        "fisc_yr" as fisc_yr,
        "fisc_per" as fisc_per,
        "merchandiser_name" as merchandiser_name,
        "customername" as customername,
        "country" as country,
        "state" as state,
        "parent_customer" as parent_customer,
        "retail_environment" as retail_environment,
        "channel" as channel,
        "retailer" as retailer,
        "business_unit" as business_unit,
        "eannumber" as eannumber,
        "matl_num" as matl_num,
        "prod_hier_l1" as prod_hier_l1,
        "prod_hier_l2" as prod_hier_l2,
        "prod_hier_l3" as prod_hier_l3,
        "prod_hier_l4" as prod_hier_l4,
        "prod_hier_l5" as prod_hier_l5,
        "prod_hier_l6" as prod_hier_l6,
        "prod_hier_l7" as prod_hier_l7,
        "prod_hier_l8" as prod_hier_l8,
        "prod_hier_l9" as prod_hier_l9,
        "ques_type" as ques_type,
        "y/n_flag" as y_n_flag,
        "priority_store_flag" as priority_store_flag,
        "add_info" as add_info,
        "response" as response,
        "response_score" as response_score,
        "kpi_chnl_wt" as kpi_chnl_wt,
        "mkt_share" as mkt_share,
        "salience_val" as salience_val,
        "channel_weightage" as channel_weightage,
        "actual_value" as actual_value,
        "ref_value" as ref_value,
        "kpi_actual_wt_val" as kpi_actual_wt_val,
        "kpi_ref_val" as kpi_ref_val,
        "kpi_ref_wt_val" as kpi_ref_wt_val,
        "photo_url" as photo_url,
        "store_grade" as store_grade

    from source

)

select * from renamed
