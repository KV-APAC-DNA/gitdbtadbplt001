{{
    config(
        materialized='table',
        alias='wks_itg_cust_sls_attr'
    )
}}

with 

sdl_cust_sls_attr as (

    select * from {{ ref('stg_ASPSDL_RAW__sdl_cust_sls_attr') }}
),

final as (
    SELECT
    SRC.division,
    SRC.distr_chan,
    SRC.salesorg,
    SRC.cust_sales,
    SRC.accnt_asgn,
    SRC.cust_cla,
    SRC.cust_group,
    SRC.cust_grp1,
    SRC.cust_grp2,
    SRC.cust_grp3,
    SRC.cust_grp4,
    SRC.cust_grp5,
    SRC.c_ctr_area,
    SRC.incoterms,
    SRC.incoterms2,
    SRC.plant,
    SRC.pmnttrms,
    SRC.sales_dist,
    SRC.sales_grp,
    SRC.sales_off,
    SRC.cur_sls_emp,
    SRC.lcl_cust_grp_1,
    SRC.lcl_cust_grp_2,
    SRC.lcl_cust_grp_3,
    SRC.lcl_cust_grp_4,
    SRC.lcl_cust_grp_5,
    SRC.lcl_cust_grp_6,
    SRC.lcl_cust_grp_7,
    SRC.lcl_cust_grp_8,
    SRC.customer,
    SRC.prc_proc,
    SRC.prc_grp,
    SRC.prc_lst_type,
    SRC.shpg_con,
    SRC.par_del,
    SRC.max_num_pa,
    SRC.prnt_cust_key,
    SRC.bnr_key,
    SRC.bnr_frmt_key,
    SRC.go_to_mdl_key,
    SRC.chnl_key,
    SRC.sub_chnl_key,
    SRC.segmt_key,
    SRC.cust_set_1,
    SRC.cust_set_2,
    SRC.cust_set_3,
    SRC.cust_set_4,
    SRC.cust_set_5,
    -- CURRENT_TIMESTAMP() AS TGT_CRT_DTTM,
    UPDT_DTTM
  FROM sdl_cust_sls_attr AS SRC
)

select * from final