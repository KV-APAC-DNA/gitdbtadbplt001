{{
    config(
        pre_hook="{{build_pcfedw_integration__edw_sls_evolution()}}"
    )
}}



with
edw_time_dim as (
      select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
edw_sales_reporting as (
      select * from {{ ref('pcfedw_integration__edw_sales_reporting') }}
),
union_1 as (
    select 'ANZ' as identifier,

             country,

             snapshot_date,

             curr_jj_period,

             prev_jj_period,

             jj_mnth,

             jj_mnth_shrt,

             jj_year,

             ese.jj_period,

             jj_qrtr,

             cust_no,

             matl_id,

             grp_fran_desc,

             prod_fran_desc,

             prod_mjr_desc,

             prod_mnr_desc,

             matl_desc,

             brnd_desc,

             gcph_franchise,

             gcph_brand,

             gcph_subbrand,

             gcph_variant,

             gcph_needstate,

             gcph_category,

             gcph_subcategory,

             gcph_segment,

             gcph_subsegment,

             master_code,

             channel_desc,

             sales_office_desc,

             cust_nm,

             sales_grp_desc,

             key_measure,

             ciw_ctgry,

             ciw_accnt_grp,

             sap_accnt,

             local_curr_cd,

             to_ccy,

             exch_rate,

             gts,

             futr_gts

      from {{this}} ese,

           (select cast(to_char(add_months (to_date(t1.jj_mnth_id::varchar,'YYYYMM'),- 1),'YYYYMM') as integer) as jj_period

            from edw_time_dim t1

            where t1.cal_date::date = convert_timezone ('Australia/Sydney',current_timestamp())::date

            union

            select t1.jj_mnth_id as jj_period

            from edw_time_dim t1

            where t1.cal_date::date = convert_timezone ('Australia/Sydney',current_timestamp())::date) etd

      where ese.jj_period = etd.jj_period
)
,
union_2 as ( select 'ANZ' as identifier,

             country,

             convert_timezone ('Australia/Sydney',current_timestamp())::date as snapshot_date,

             etd.curr_jj_period,

             etd.prev_jj_period,

             jj_mnth,

             jj_mnth_shrt,

             jj_year,

             jj_period,

             jj_qrtr,

             ltrim(cust_no,'0') as cust_no,

             ltrim(matl_id,'0') as matl_id,

             grp_fran_desc,

             prod_fran_desc,

             prod_mjr_desc,

             prod_mnr_desc,

             matl_desc,

             brnd_desc,

             gcph_franchise,

             gcph_brand,

             gcph_subbrand,

             gcph_variant,

             gcph_needstate,

             gcph_category,

             gcph_subcategory,

             gcph_segment,

             gcph_subsegment,

             master_code,

             channel_desc,

             sales_office_desc,

             cust_nm,

             sales_grp_desc,

             key_measure,

             ciw_ctgry,

             ciw_accnt_grp,

             sap_accnt,

             local_curr_cd,

             to_ccy,

             exch_rate,

             gts,

             futr_gts

      from edw_sales_reporting esr,

           (select cast(to_char(add_months (to_date(t1.jj_mnth_id::varchar,'YYYYMM'),- 1),'YYYYMM') as integer) as prev_jj_period,

                   t1.jj_mnth_id as curr_jj_period

            from edw_time_dim t1

            where t1.cal_date::date = convert_timezone ('Australia/Sydney',current_timestamp())::date) etd

      where jj_period = etd.curr_jj_period

      and   upper(pac_source_type) = 'SAPBW'

      and   upper(pac_subsource_type) in ('SAPBW_ACTUAL','SAPBW_FUTURES')
      ),
      
wks as (

select country,

       snapshot_date,

       max(curr_jj_period) over (partition by upper(identifier) order by upper(identifier) asc nulls last rows between unbounded preceding and unbounded following) as curr_jj_period,

       max(prev_jj_period) over (partition by upper(identifier) order by upper(identifier) asc nulls last rows between unbounded preceding and unbounded following) as prev_jj_period,

       jj_mnth,

       jj_mnth_shrt,

       jj_year,

       jj_period,

       jj_qrtr,

       cust_no,

       matl_id,

       grp_fran_desc,

       prod_fran_desc,

       prod_mjr_desc,

       prod_mnr_desc,

       matl_desc,

       brnd_desc,

       gcph_franchise,

       gcph_brand,

       gcph_subbrand,

       gcph_variant,

       gcph_needstate,

       gcph_category,

       gcph_subcategory,

       gcph_segment,

       gcph_subsegment,

       master_code,

       channel_desc,

       sales_office_desc,

       cust_nm,

       sales_grp_desc,

       key_measure,

       ciw_ctgry,

       ciw_accnt_grp,

       sap_accnt,

       local_curr_cd,

       to_ccy,

       exch_rate,

       gts,

       futr_gts
from(
select * from union_1

      union all

select * from union_2)
),
final as (
select
country::varchar(20) as country,
snapshot_date::date as snapshot_date,
cust_no::varchar(10) as cust_no,
matl_id::varchar(40) as matl_id,
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
from wks
)
select * from final 