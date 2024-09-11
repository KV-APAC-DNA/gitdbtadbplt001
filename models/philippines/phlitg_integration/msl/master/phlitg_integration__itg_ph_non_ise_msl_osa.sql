{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "{% if is_incremental() %}
        delete from {{this}} where ( coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) ) in (
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_sm') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_ds') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_sm') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_ds') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_puregold') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_rustans') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_shm') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_super_8') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_svi_smc') }} WHERE encoded_report = '1'
          union 
          select distinct coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') as branch_code,UPPER(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_waltermart') }} WHERE encoded_report = '1'
         );{% endif %}"
    )
}}


with wks_ph_non_ise_landmark_sm as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_sm') }}
    qualify rnk = 1
),
wks_ph_non_ise_landmark_ds as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_ds') }}
    qualify rnk = 1
),
wks_ph_non_ise_puregold as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_puregold') }}
    qualify rnk = 1
),
wks_ph_non_ise_robinsons_ds as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_ds') }}
    qualify rnk = 1
),
wks_ph_non_ise_robinsons_sm as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_sm') }}
    qualify rnk = 1
),
wks_ph_non_ise_rustans as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_rustans') }}
    qualify rnk = 1
),
wks_ph_non_ise_shm as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_shm') }}
    qualify rnk = 1
),
wks_ph_non_ise_super_8 as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_super_8') }}
    qualify rnk = 1
),
wks_ph_non_ise_svi_smc as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_svi_smc') }}
    qualify rnk = 1
),
wks_ph_non_ise_waltermart as (
    select *, dense_rank() over (partition by coalesce(sku_code, 'NA'),coalesce(osa_check_date,'9999-12-31'),coalesce(upper(branch_code),'NA') ,UPPER(retailer_name) order by filename) as rnk
    from {{ ref('phlwks_integration__wks_ph_non_ise_waltermart') }}
    qualify rnk = 1
), 
landmark_ds as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,
       
       null as msl_large,

       null as msl_small,

       null as msl_premium,

       msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_landmark_ds

WHERE encoded_report = '1'
),
landmark_sm as (

SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,
       
       null as msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       
       sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_landmark_sm

WHERE encoded_report = '1'
),
puregold as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       null as msl_premium,

       msl_grocery,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       null as store_location,
       diser_name,
       msl_reseller,
       msl_priceclub

FROM wks_ph_non_ise_puregold

WHERE encoded_report = '1'
),
robinsons_ds as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,
       null as msl_large,
       null as msl_small,
       null as msl_premium,

       msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_robinsons_ds

WHERE encoded_report = '1'
),
robinsons_sm as (
SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,
       null as msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_robinsons_sm

WHERE encoded_report = '1'
),
rustans as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,

       null as msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_rustans

WHERE encoded_report = '1'
),
shm as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,

       null as msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_shm

WHERE encoded_report = '1'
),
super_8 as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       NULL as msl_large,
       null as msl_small,
       null as msl_premium,

       msl_sup_hybrid,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_super_8

WHERE encoded_report = '1'
),
svi_smc as (
SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,
       null as msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       sub_channel,
       store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_svi_smc

WHERE encoded_report = '1'
),
waltermart as (
    SELECT ret_nm_prefix,

       sku_code,

       brand,

       barcode,

       item_description,

       msl_large,

       msl_small,

       msl_premium,
       null as msl_dept,

       month,

       week,

       reason,

       acct_deliv_date,

       osa_check_date,

       encoded_report,

       team_leader,

       branch_code,

       branch_code_original,

       branch_classification,

       branch_name,

       osa_flag,

       retailer_name,

       filename,

       run_id,

       crtd_dttm,
       null as sub_channel,
       null as store_location,
       null as diser_name,
       null as msl_reseller,
       null as msl_priceclub

FROM wks_ph_non_ise_waltermart

WHERE encoded_report = '1'
),
transformed as (
select * from landmark_ds
union all
select * from landmark_sm
union all
select * from robinsons_ds
union all
select * from robinsons_sm
union all
select * from rustans
union all
select * from puregold
union all
select * from shm
union all
select * from waltermart
union all
select * from svi_smc
union all
select * from super_8
),
final as (
select
    ret_nm_prefix::varchar(50) as ret_nm_prefix,
    sku_code::varchar(50) as sku_code,
    brand::varchar(500) as brand,
    barcode::varchar(100) as barcode,
    item_description::varchar(500) as item_description,
    msl_large::varchar(10) as msl_large,
    msl_small::varchar(10) as msl_small,
    msl_premium::varchar(10) as msl_premium,
    msl_dept::varchar(10) as msl_dept,
    month::varchar(10) as month,
    week::varchar(10) as week,
    reason::varchar(500) as reason,
    acct_deliv_date::varchar(100) as acct_deliv_date,
    osa_check_date::date as osa_check_date,
    encoded_report::varchar(500) as encoded_report,
    team_leader::varchar(100) as team_leader,
    branch_code::varchar(50) as branch_code,
    branch_code_original::varchar(50) as branch_code_original,
    branch_classification::varchar(100) as branch_classification,
    branch_name::varchar(100) as branch_name,
    osa_flag::varchar(10) as osa_flag,
    retailer_name::varchar(50) as retailer_name,
    filename::varchar(100) as filename,
    run_id::number(14,0) as run_id,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    sub_channel::varchar(200) as sub_channel,
    store_location::varchar(200) as store_location,
    diser_name::varchar(200) as diser_name,
    msl_reseller::varchar(10) as msl_reseller,
    msl_priceclub::varchar(10) as msl_priceclub
from transformed
)
select * from final