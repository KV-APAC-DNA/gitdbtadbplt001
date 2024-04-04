with vw_bwar_curr_exch_dim as(
    select * from DEV_DNA_CORE.PCFEDW_INTEGRATION.VW_BWAR_CURR_EXCH_DIM
),
edw_perenso_prod_probeid_dim as (
    select * from DEV_DNA_CORE.SNAPPCFEDW_INTEGRATION.EDW_PERENSO_PROD_PROBEID_DIM
),
edw_gch_producthierarchy as (
    select * from DEV_DNA_CORE.SNAPASPEDW_INTEGRATION.EDW_GCH_PRODUCTHIERARCHY
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
  etd.jj_year,
  etd.jj_qrtr,
  etd.jj_mnth,
  etd.jj_wk,
  etd.jj_mnth_id,
  etd.jj_mnth_tot,
  etd.jj_mnth_day,
  etd.jj_mnth_shrt,
  etd.jj_mnth_long,
  etd.cal_year,
  etd.cal_qrtr,
  etd.cal_mnth,
  etd.cal_mnth_id,
  etd.cal_mnth_nm,
  eppd.prod_key,
  pef.product_probe_id,
  eppd.prod_desc,
  eppd.prod_id as prod_sapbw_code,
  eppd.prod_ean,
  eppd.prod_jj_franchise,
  eppd.prod_jj_category,
  pef.category as iqvia_prod_category,
  eppd.prod_jj_brand,
  eppd.prod_sap_franchise,
  eppd.prod_sap_profit_centre,
  eppd.prod_sap_product_major,
  eppd.prod_grocery_franchise,
  eppd.prod_grocery_category,
  eppd.prod_grocery_brand,
  eppd.prod_pbs,
  eppd.prod_ims_brand,
  eppd.prod_nz_code,
  gch.gcph_franchise,
  gch.gcph_brand,
  gch.gcph_subbrand,
  gch.gcph_variant,
  gch.gcph_needstate,
  gch.gcph_category,
  gch.gcph_subcategory,
  gch.gcph_segment,
  gch.gcph_subsegment,
  pef.cust_group,
  CASE
    WHEN UPPER(PEF.CUST_GROUP) = 'OTHER AURX'
    THEN 'Other AU Pharmacy'
    WHEN UPPER(PEF.CUST_GROUP) = 'CHW'
    THEN 'Chemist Warehouse'
    else pef.cust_group
  end as ecomm_cust,
  pef.owner,
  pef.manufacturer,
  pef.sales_qty as unit_online,
  pef.sales_value as aud_sales_online,
  bwar.exch_rate as exch_rate_to_usd,
  pef.sales_value * bwar.exch_rate as usd_sales_online
from edw_au_pharm_ecomm_fact as pef
left join edw_time_dim as etd
  on pef.week_end_dt = trunc(etd.cal_date)
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