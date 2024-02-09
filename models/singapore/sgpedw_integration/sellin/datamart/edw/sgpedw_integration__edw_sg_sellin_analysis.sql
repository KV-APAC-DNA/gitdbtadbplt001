with source as
(
    select * from {{ ref('sgpedw_integration__edw_vw_sg_sellin_analysis') }}
),
final as
(
    select 
        data_source_cd::varchar(5) as data_source_cd,
        data_source::varchar(18) as data_source,
        sap_cmp_id::varchar(6) as sap_cmp_id,
        sap_cntry_nm::varchar(40) as sap_cntry_nm,
        "year"::number(18,0) as year,
        qrtr::varchar(8) as qrtr,
        mnth_id::varchar(11) as mnth_id,
        mnth_no::number(18,0) as mnth_no,
        gl_acct_no::number(18,0) as gl_acct_no,
        gl_description::varchar(500) as gl_description,
        measure_bucket::varchar(500) as measure_bucket,
        cust_l1::varchar(7) as cust_l1,
        sg_banner::varchar(255) as sg_banner,
        sap_bnr_frmt_desc::varchar(50) as sap_bnr_frmt_desc,
        sap_cust_nm::varchar(100) as sap_cust_nm,
        sap_prnt_cust_desc::varchar(50) as sap_prnt_cust_desc,
        retail_env::varchar(50) as retail_env,
        sg_brand::varchar(255) as sg_brand,
        gph_reg_frnchse_grp::varchar(50) as gph_reg_frnchse_grp,
        gph_prod_frnchse::varchar(30) as gph_prod_frnchse,
        gph_prod_brnd::varchar(30) as gph_prod_brnd,
        gph_prod_sub_brnd::varchar(100) as gph_prod_sub_brnd,
        sap_matl_num::varchar(40) as sap_matl_num,
        sap_mat_desc::varchar(100) as sap_mat_desc,
        currency::varchar(5) as currency,
        base_value::float as base_value
        from source
)
select * from final