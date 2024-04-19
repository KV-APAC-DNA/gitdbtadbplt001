{{
    config
    (
        post_hook = "create or replace table 
                                        {% if target.name=='prod' %}
                                            pcfedw_integration.edw_promax_ciw_snapshot_temp
                                        {% else %}
                                            {{schema}}.pcfedw_integration__edw_promax_ciw_snapshot
                                        {% endif %}
                clone {{this}};"
    )
}}
with source as 
(
    select * from {{ ref('pcfwks_integration__wks_promax_ciw_snapshot') }}
),
final as 
(
    select
        snapshot_date::timestamp_ntz(9) as snapshot_date,
        snapshot_month::varchar(10) as snapshot_month,
        snapshot_year::number(18,0) as snapshot_year,
        jj_period::number(18,0) as jj_period,
        jj_wk::varchar(1) as jj_wk,
        jj_mnth::number(18,0) as jj_mnth,
        jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
        jj_mnth_long::varchar(10) as jj_mnth_long,
        jj_qrtr::number(18,0) as jj_qrtr,
        jj_year::number(18,0) as jj_year,
        cust_no::varchar(10) as cust_no,
        cmp_desc::varchar(20) as cmp_desc,
        channel_desc::varchar(20) as channel_desc,
        cust_nm::varchar(100) as cust_nm,
        sales_office_desc::varchar(30) as sales_office_desc,
        sales_grp_desc::varchar(30) as sales_grp_desc,
        matl_id::varchar(40) as matl_id,
        matl_desc::varchar(100) as matl_desc,
        parent_matl_id::varchar(18) as parent_matl_id,
        parent_matl_desc::varchar(100) as parent_matl_desc,
        fran_desc::varchar(100) as fran_desc,
        grp_fran_desc::varchar(100) as grp_fran_desc,
        matl_type_desc::varchar(40) as matl_type_desc,
        prod_fran_desc::varchar(100) as prod_fran_desc,
        prod_mjr_desc::varchar(100) as prod_mjr_desc,
        prod_mnr_desc::varchar(100) as prod_mnr_desc,
        base_curr_cd::varchar(10) as base_curr_cd,
        to_ccy::varchar(5) as to_ccy,
        px_qty::number(38,0) as px_qty,
        px_gts::number(38,7) as px_gts,
        px_eff_val::float as px_eff_val,
        px_jgf_si_val::float as px_jgf_si_val,
        px_pmt_terms_val::float as px_pmt_terms_val,
        px_datains_val::float as px_datains_val,
        px_exp_adj_val::float as px_exp_adj_val,
        px_jgf_sd_val::float as px_jgf_sd_val,
        px_ciw_tot::float as px_ciw_tot,
        px_nts::number(38,7) as px_nts
    from source
)
select * from final