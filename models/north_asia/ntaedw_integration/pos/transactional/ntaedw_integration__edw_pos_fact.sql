{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= ["{% if is_incremental() %}
                    delete
                    from {{this}}
                    where src_sys_cd in (select distinct src_sys_cd from {{ ref('ntawks_integration__wks_edw_pos_fact') }})
                    and   hist_flg ilike  '%n%';
                    {% endif %}"]
        )
}}


with wks_edw_pos_fact as (
    select * from {{ ref('ntawks_integration__wks_edw_pos_fact') }}
),
final as (
select
pos_dt::date as pos_dt,
vend_cd::varchar(40) as vend_cd,
vend_nm::varchar(100) as vend_nm,
prod_nm::varchar(100) as prod_nm,
vend_prod_cd::varchar(40) as vend_prod_cd,
vend_prod_nm::varchar(100) as vend_prod_nm,
brnd_nm::varchar(40) as brnd_nm,
ean_num::varchar(100) as ean_num,
str_cd::varchar(40) as str_cd,
str_nm::varchar(100) as str_nm,
sls_qty::number(18,0) as sls_qty,
sls_amt::number(16,5) as sls_amt,
unit_prc_amt::number(16,5) as unit_prc_amt,
sls_excl_vat_amt::number(16,5) as sls_excl_vat_amt,
stk_rtrn_amt::number(16,5) as stk_rtrn_amt,
stk_recv_amt::number(16,5) as stk_recv_amt,
avg_sell_qty::number(16,5) as avg_sell_qty,
cum_ship_qty::number(18,0) as cum_ship_qty,
cum_rtrn_qty::number(18,0) as cum_rtrn_qty,
web_ordr_takn_qty::number(18,0) as web_ordr_takn_qty,
web_ordr_acpt_qty::number(18,0) as web_ordr_acpt_qty,
dc_invnt_qty::number(18,0) as dc_invnt_qty,
invnt_qty::number(18,0) as invnt_qty,
invnt_amt::number(16,5) as invnt_amt,
invnt_dt::date as invnt_dt,
serial_num::varchar(40) as serial_num,
prod_delv_type::varchar(40) as prod_delv_type,
prod_type::varchar(40) as prod_type,
dept_cd::varchar(40) as dept_cd,
dept_nm::varchar(100) as dept_nm,
spec_1_desc::varchar(100) as spec_1_desc,
spec_2_desc::varchar(100) as spec_2_desc,
cat_big::varchar(100) as cat_big,
cat_mid::varchar(40) as cat_mid,
cat_small::varchar(40) as cat_small,
dc_prod_cd::varchar(40) as dc_prod_cd,
cust_dtls::varchar(100) as cust_dtls,
dist_cd::varchar(40) as dist_cd,
crncy_cd::varchar(10) as crncy_cd,
src_txn_sts::varchar(40) as src_txn_sts,
src_seq_num::number(18,0) as src_seq_num,
src_sys_cd::varchar(30) as src_sys_cd,
ctry_cd::varchar(10) as ctry_cd,
sold_to_party::varchar(100) as sold_to_party,
sls_grp::varchar(100) as sls_grp,
mysls_brnd_nm::varchar(500) as mysls_brnd_nm,
mysls_catg::varchar(100) as mysls_catg,
matl_num::varchar(40) as matl_num,
matl_desc::varchar(100) as matl_desc,
hist_flg::varchar(40) as hist_flg,
crt_dttm::timestamp_ntz(9) as crt_dttm,
updt_dttm::timestamp_ntz(9) as updt_dttm,
prom_sls_amt::number(16,5) as prom_sls_amt,
prom_prc_amt::number(16,5) as prom_prc_amt,
null::varchar(25) as channel,
null::varchar(25) as store_type,
null::varchar(18) as sls_grp_cd
from wks_edw_pos_fact
)
select * from final