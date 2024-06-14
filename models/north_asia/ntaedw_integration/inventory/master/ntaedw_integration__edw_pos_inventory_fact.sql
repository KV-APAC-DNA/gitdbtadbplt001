{{
    config(
        materialized="incremental",
        incremental_strategy = "append",
        unique_key=["src_sys_cd"],
        pre_hook="delete from {{this}} where src_sys_cd in (select distinct src_sys_cd from {{ ref('ntawks_integration__wks_edw_pos_inventory_fact') }}) and {{this}}.hist_flg = 'N';"
    )
}}

with source as(
    select * from {{ ref('ntawks_integration__wks_edw_pos_inventory_fact') }}
),
final as(
    SELECT 
       to_date(invnt_dt) as invnt_dt,
       vend_cd::varchar(40) as vend_cd,
       vend_nm::varchar(100) as vend_nm,
       vend_prod_cd::varchar(40) as vend_prod_cd,
       vend_prod_nm::varchar(100) as vend_prod_nm,
       ean_num::varchar(40) as ean_num,
       str_cd::varchar(40) as str_cd,
       str_nm::varchar(100) as str_nm,
       invnt_qty::number(18,0) as invnt_qty,
       invnt_amt::number(16,5) as invnt_amt,
       unit_prc_amt::number(16,5) as unit_prc_amt,
       per_box_qty::number(16,5) as per_box_qty,
       cust_invnt_qty::number(16,5) as cust_invnt_qty,
       box_invnt_qty::number(16,5) as box_invnt_qty,
       wk_hold_sls::number(16,5) as wk_hold_sls,
       wk_hold::number(16,5) as wk_hold,
       fst_recv_dt::varchar(10) as fst_recv_dt,
       dsct_dt::varchar(10) as dsct_dt,
       dc::varchar(40) as dc,
       stk_cls::varchar(40) as stk_cls,
       sold_to_party::varchar(100) as sold_to_party,
       sls_grp::varchar(100) as sls_grp,
       mysls_brnd_nm::varchar(500) as mysls_brnd_nm,
       mysls_catg::varchar(100) as mysls_catg,
       matl_num::varchar(40) as matl_num,
       matl_desc::varchar(100) as matl_desc,
       prom_invnt_amt::number(16,5) as prom_invnt_amt,
       prom_prc_amt::number(16,5) as prom_prc_amt,
       hist_flg::varchar(40) as hist_flg,
       crncy_cd::varchar(10) as crncy_cd,
       src_sys_cd::varchar(30) as src_sys_cd,
       ctry_cd::varchar(10) as ctry_cd,
       current_timestamp()::timestamp_ntz(9) as crt_dttm,
       current_timestamp()::timestamp_ntz(9) as upd_dttm
    FROM source
)
select * from final

