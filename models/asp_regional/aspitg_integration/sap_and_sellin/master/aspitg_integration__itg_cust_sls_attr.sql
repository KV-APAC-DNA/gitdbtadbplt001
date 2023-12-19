{{
    config(
        alias="itg_cust_sls_attr",
        sql_header="ALTER SESSION SET TIMEZONE = 'Asia/Singapore';",
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["division","distr_chan","salesorg","cust_sales"],
        merge_exclude_columns = ["crt_dttm"],
        tags=["daily"]
    )
}}

with 

source as (

    select * from {{ ref('aspwks_integration__wks_itg_cust_sls_attr') }}
),

final as (
    select
    division,
    distr_chan,
    salesorg,
    cust_sales,
    accnt_asgn,
    cust_cla,
    cust_group,
    cust_grp1,
    cust_grp2,
    cust_grp3,
    cust_grp4,
    cust_grp5,
    c_ctr_area,
    incoterms,
    incoterms2,
    plant,
    pmnttrms,
    sales_dist,
    sales_grp,
    sales_off,
    cur_sls_emp,
    lcl_cust_grp_1,
    lcl_cust_grp_2,
    lcl_cust_grp_3,
    lcl_cust_grp_4,
    lcl_cust_grp_5,
    lcl_cust_grp_6,
    lcl_cust_grp_7,
    lcl_cust_grp_8,
    customer,
    prc_proc,
    prc_grp,
    prc_lst_type,
    shpg_con,
    par_del,
    max_num_pa,
    prnt_cust_key,
    bnr_key,
    bnr_frmt_key,
    go_to_mdl_key,
    chnl_key,
    sub_chnl_key,
    segmt_key,
    cust_set_1,
    cust_set_2,
    cust_set_3,
    cust_set_4,
    cust_set_5,
    current_timestamp()::timestamp_ntz(9) as crt_dttm,
    current_timestamp()::timestamp_ntz(9) as updt_dttm
  FROM source
)

select * from final