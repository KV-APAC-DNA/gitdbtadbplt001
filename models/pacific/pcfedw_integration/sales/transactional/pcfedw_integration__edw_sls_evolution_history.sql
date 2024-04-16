{{
    config(
        materialized="incremental",
        incremental_strategy = "append"
    )
}}



with
edw_time_dim as (
      select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_TIME_DIM
),
edw_sales_reporting as (
      select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_SALES_REPORTING
),
etd as (
 (select cast(to_char(add_months (to_date(t1.jj_mnth_id::varchar,'YYYYMM'),- 1),'YYYYMM') as integer) as prev_jj_period,

             t1.jj_mnth_id as curr_jj_period

      from edw_time_dim t1

      where t1.cal_date::date = convert_timezone ('Australia/Sydney',current_timestamp())::date) 
) ,
final as (
select
country::varchar(20) as country,
convert_timezone('Australia/Sydney',current_timestamp())::date as snapshot_date,
ltrim(cust_no,'0')::varchar(10) as cust_no,
ltrim(matl_id,'0')::varchar(40) as matl_id,
grp_fran_desc::varchar(100) as grp_fran_desc,
prod_fran_desc::varchar(100) as prod_fran_desc,
prod_mjr_desc::varchar(100) as prod_mjr_desc,
prod_mnr_desc::varchar(100) as prod_mnr_desc,
matl_desc::varchar(100) as matl_desc,
brnd_desc::varchar(100) as brnd_desc,
gcph_franchise::varchar(100) as gcph_franchise,
gcph_brand::varchar(100) as gcph_brand,
gcph_subbrand::varchar(100) as gcph_subbrand,
gcph_variant::varchar(100) as gcph_variant,
gcph_needstate::varchar(100) as gcph_needstate,
gcph_category::varchar(100) as gcph_category,
gcph_subcategory::varchar(100) as gcph_subcategory,
gcph_segment::varchar(100) as gcph_segment,
gcph_subsegment::varchar(100) as gcph_subsegment,
master_code::varchar(18) as master_code,
channel_desc::varchar(20) as channel_desc,
sales_office_desc::varchar(30) as sales_office_desc,
cust_nm::varchar(100) as cust_nm,
sales_grp_desc::varchar(30) as sales_grp_desc,
key_measure::varchar(40) as key_measure,
ciw_ctgry::varchar(40) as ciw_ctgry,
ciw_accnt_grp::varchar(40) as ciw_accnt_grp,
sap_accnt::varchar(40) as sap_accnt,
local_curr_cd::varchar(10) as local_curr_cd,
curr_jj_period::number(18,0) as curr_jj_period,
prev_jj_period::number(18,0) as prev_jj_period,
jj_mnth::number(18,0) as jj_mnth,
jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
jj_year::number(18,0) as jj_year,
jj_period::number(18,0) as jj_period,
jj_qrtr::number(18,0) as jj_qrtr,
to_ccy::varchar(5) as to_ccy,
exch_rate::number(15,5) as exch_rate,
gts::number(38,7) as gts,
futr_gts::number(38,9) as futr_gts

from edw_sales_reporting esr,

    etd

where jj_period = etd.curr_jj_period

and   upper(pac_source_type) = 'SAPBW'

and   upper(pac_subsource_type) in ('SAPBW_ACTUAL','SAPBW_FUTURES')
{% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        and snapshot_date > (select max(snapshot_date) from {{ this }}) 
    {% endif %}
)

select * from final