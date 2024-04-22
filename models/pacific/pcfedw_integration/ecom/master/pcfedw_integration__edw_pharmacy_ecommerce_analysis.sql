with vw_bwar_curr_exch_dim as(
    select * from {{ ref('pcfedw_integration__vw_bwar_curr_exch_dim') }}
),
edw_perenso_prod_probeid_dim as (
    select * from {{ ref('pcfedw_integration__edw_perenso_prod_probeid_dim') }}
),
edw_gch_producthierarchy as (
    select * from {{ ref('aspedw_integration__edw_gch_producthierarchy') }}
),
edw_au_pharm_ecomm_fact as (
    select * from {{ ref('pcfedw_integration__edw_au_pharm_ecomm_fact') }}
),
edw_time_dim as (
    select * from {{ source('pcfedw_integration', 'edw_time_dim') }}
),
eppd as(
    select * from edw_perenso_prod_probeid_dim
        where
            not product_probe_id in (
            select distinct
                product_probe_id
            from edw_perenso_prod_probeid_dim
            group by
                1
            having
                count(*) > 1
            )
),
gch as(
    select
    materialnumber,
    gcph_franchise,
    gcph_brand,
    gcph_subbrand,
    gcph_variant,
    gcph_needstate,
    gcph_category,
    gcph_subcategory,
    gcph_segment,
    gcph_subsegment
  from edw_gch_producthierarchy
  where
    ltrim(materialnumber, 0) <> '' and "region" = 'APAC'
),
bwar as (
    select * from vw_bwar_curr_exch_dim
    where to_ccy = 'USD'
),
transformed as(
select
  pef.week_end_dt,
  etd.jj_year::number(18,0) as jj_year,
  etd.jj_qrtr::number(18,0) as jj_qrtr,
  etd.jj_mnth::number(18,0) as jj_mnth,
  etd.jj_wk::number(18,0) as jj_wk,
  etd.jj_mnth_id::number(18,0) as jj_mnth_id,
  etd.jj_mnth_tot::number(18,0) as jj_mnth_tot,
  etd.jj_mnth_day::number(18,0) as jj_mnth_day,
  etd.jj_mnth_shrt::varchar(3) as jj_mnth_shrt,
  etd.jj_mnth_long::varchar(10) as jj_mnth_long,
  etd.cal_year::number(18,0) as cal_year,
  etd.cal_qrtr::number(18,0) as cal_qrtr,
  etd.cal_mnth::number(18,0) as cal_mnth,
  etd.cal_mnth_id::number(18,0) as cal_mnth_id,
  etd.cal_mnth_nm::varchar(10) as cal_mnth_nm,
  eppd.prod_key::number(10,0) as prod_key,
  pef.product_probe_id::varchar(20) as product_probe_id,
  eppd.prod_desc::varchar(100) as prod_desc,
  eppd.prod_id::varchar(50) as prod_sapbw_code,
  eppd.prod_ean::varchar(50) as prod_ean,
  eppd.prod_jj_franchise::varchar(100) as prod_jj_franchise,
  eppd.prod_jj_category::varchar(100) as prod_jj_category,
  pef.category::varchar(50) as iqvia_prod_category,
  eppd.prod_jj_brand::varchar(100) as prod_jj_brand,
  eppd.prod_sap_franchise::varchar(100) as prod_sap_franchise,
  eppd.prod_sap_profit_centre::varchar(100) as prod_sap_profit_centre,
  eppd.prod_sap_product_major::varchar(100) as prod_sap_product_major,
  eppd.prod_grocery_franchise::varchar(100) as prod_grocery_franchise,
  eppd.prod_grocery_category::varchar(100) as prod_grocery_category,
  eppd.prod_grocery_brand::varchar(100) as prod_grocery_brand,
  eppd.prod_pbs::varchar(100) as prod_pbs,
  eppd.prod_ims_brand::varchar(100) as prod_ims_brand,
  eppd.prod_nz_code::varchar(100) as prod_nz_code,
  gch.gcph_franchise::varchar(30) as gcph_franchise,
  gch.gcph_brand::varchar(30) as gcph_brand,
  gch.gcph_subbrand::varchar(100) as gcph_subbrand,
  gch.gcph_variant::varchar(100) as gcph_variant,
  gch.gcph_needstate::varchar(50) as gcph_needstate,
  gch.gcph_category::varchar(50) as gcph_category,
  gch.gcph_subcategory::varchar(50) as gcph_subcategory,
  gch.gcph_segment::varchar(50) as gcph_segment,
  gch.gcph_subsegment::varchar(100) as gcph_subsegment,
  pef.cust_group::varchar(10) as cust_group,
  CASE
    WHEN UPPER(PEF.CUST_GROUP) = 'OTHER AURX'
    THEN 'Other AU Pharmacy'
    WHEN UPPER(PEF.CUST_GROUP) = 'CHW'
    THEN 'Chemist Warehouse'
    else pef.cust_group
  end::varchar(17) as ecomm_cust,
  pef.owner::varchar(20) as owner,
  pef.manufacturer::varchar(50) as manufacturer,
  pef.sales_qty::number(10,2) as unit_online,
  pef.sales_value::number(10,2) as aud_sales_online,
  bwar.exch_rate::number(15,5) as exch_rate_to_usd,
  (pef.sales_value * bwar.exch_rate)::number(26,7) as usd_sales_online
from edw_au_pharm_ecomm_fact as pef
left join edw_time_dim as etd
  on pef.week_end_dt::date = etd.cal_date::date
/* negelecting the duplicate probe id from dim table */
left join eppd
  on pef.product_probe_id = eppd.product_probe_id
/* used same query from sales_reporting view as per requirement */
left join gch
  on ltrim(eppd.prod_id, 0) = ltrim(gch.materialnumber, 0)
left join bwar
  on etd.jj_mnth_id = bwar.jj_mnth_id and pef.crncy = bwar.from_ccy
where
  datediff(month, pef.week_end_dt, current_date) <= 24
)
select * from transformed