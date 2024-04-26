{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where ( sku_code,osa_check_date,UPPER(branch_code) ,UPPER(retailer_name) ) in (
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_landmark_sm') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_landmark_ds') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_robinsons_sm') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_robinsons_ds') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_puregold') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_rustans') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_shm') }} WHERE encoded_report = '1'
          union 
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_super_8') }} WHERE encoded_report = '1'
          union all
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_svi_smc') }} WHERE encoded_report = '1'
          union all
          select distinct sku_code,osa_check_date,UPPER(branch_code) as branch_code,UPPER(retailer_name) as retailer_name from {{ source('phlsdl_raw', 'sdl_ph_non_ise_waltermart') }} WHERE encoded_report = '1'
         );"
    )
}}





with sdl_ph_non_ise_landmark_sm as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_landmark_sm') }}
),
sdl_ph_non_ise_landmark_ds as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_landmark_ds') }}
),
sdl_ph_non_ise_puregold as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_puregold') }}
),
sdl_ph_non_ise_robinsons_ds as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_robinsons_ds') }}
),
sdl_ph_non_ise_robinsons_sm as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_robinsons_sm') }}
),
sdl_ph_non_ise_rustans as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_rustans') }}
),
sdl_ph_non_ise_shm as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_shm') }}
),
sdl_ph_non_ise_super_8 as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_super_8') }}
),
sdl_ph_non_ise_svi_smc as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_svi_smc') }}
),
sdl_ph_non_ise_waltermart as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_non_ise_waltermart') }}
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

       crtd_dttm

FROM sdl_ph_non_ise_landmark_ds

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

       crtd_dttm

FROM sdl_ph_non_ise_landmark_sm

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

       crtd_dttm

FROM sdl_ph_non_ise_puregold

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

       crtd_dttm

FROM sdl_ph_non_ise_robinsons_ds

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

       crtd_dttm

FROM sdl_ph_non_ise_robinsons_sm

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

       crtd_dttm

FROM sdl_ph_non_ise_rustans

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

       crtd_dttm

FROM sdl_ph_non_ise_shm

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

       crtd_dttm

FROM sdl_ph_non_ise_super_8

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

       crtd_dttm

FROM sdl_ph_non_ise_svi_smc

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

       crtd_dttm

FROM sdl_ph_non_ise_waltermart

WHERE encoded_report = '1'
),
final as (
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
)
select * from final