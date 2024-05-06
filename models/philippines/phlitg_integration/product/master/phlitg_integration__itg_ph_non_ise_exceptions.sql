{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook= "delete from {{this}} where ( coalesce(sku_code, 'NA'), coalesce(upper(month), 'NA'), coalesce(upper(week), 'NA'), coalesce(upper(encoded_report), 'NA'), coalesce(upper(branch_code), 'NA'), upper(retailer_name) ) in (
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_sm') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_ds') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_sm') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_ds') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_puregold') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_rustans') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_shm') }} WHERE encoded_report <> '1'
          union all 
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_super_8') }} WHERE encoded_report <> '1'
          union all
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_svi_smc') }} WHERE encoded_report <> '1'
          union all
          select distinct coalesce(sku_code, 'NA') as sku_code, coalesce(upper(month), 'NA') as month, coalesce(upper(week), 'NA') as week, coalesce(upper(encoded_report), 'NA') as encoded_report, coalesce(upper(branch_code), 'NA') as branch_code, upper(retailer_name) as retailer_name from {{ ref('phlwks_integration__wks_ph_non_ise_waltermart') }} WHERE encoded_report <> '1'
         );"
    )
}}

with wks_ph_non_ise_landmark_sm as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_sm') }}
),
wks_ph_non_ise_landmark_ds as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_landmark_ds') }}
),
wks_ph_non_ise_puregold as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_puregold') }}
),
wks_ph_non_ise_robinsons_ds as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_ds') }}
),
wks_ph_non_ise_robinsons_sm as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_robinsons_sm') }}
),
wks_ph_non_ise_rustans as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_rustans') }}
),
wks_ph_non_ise_shm as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_shm') }}
),
wks_ph_non_ise_super_8 as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_super_8') }}
),
wks_ph_non_ise_svi_smc as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_svi_smc') }}
),
wks_ph_non_ise_waltermart as (
    select * from {{ ref('phlwks_integration__wks_ph_non_ise_waltermart') }}
), 
landmark_ds as(
    select distinct 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        null::varchar(10) as msl_large,
        null::varchar(10) as msl_small,
        null::varchar(10) as msl_premium,
        msl_dept::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_landmark_ds
    where encoded_report <> '1'
),
landmark_sm as(
    select 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        msl_large::varchar(10) as msl_large,
        msl_small::varchar(10) as msl_small,
        msl_premium::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_landmark_sm
    WHERE encoded_report <> '1'       
),
puregold as(
    select distinct 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        msl_large::varchar(10) as msl_large,
        msl_small::varchar(10) as msl_small,
        msl_grocery::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_puregold
    where encoded_report <> '1'
),
ROBINSONS_DS as(
     select distinct 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        null::varchar(10) as msl_large,
        null::varchar(10) as msl_small,
        null::varchar(10) as msl_premium,
        msl_dept::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_robinsons_ds
    where encoded_report <> '1'
),
ROBINSONS_SM as(
     select distinct 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
    	sku_code::varchar(50) as sku_code,
    	brand::varchar(500) as brand,
    	barcode::varchar(100) as barcode,
    	item_description::varchar(500) as item_description,
    	msl_large::varchar(10) as msl_large,
    	msl_small::varchar(10) as msl_small,
    	msl_premium::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
    	month::varchar(10) as month,
    	week::varchar(10) as week,
    	reason::varchar(500) as reason,
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
    	crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_robinsons_sm
    where encoded_report <> '1'

),
rustans as(
    select distinct 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        msl_large::varchar(10) as msl_large,
        msl_small::varchar(10) as msl_small,
        msl_premium::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_rustans
    where encoded_report <> '1'
),
shm as(
    select distinct 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        msl_large::varchar(10) as msl_large,
        msl_small::varchar(10) as msl_small,
        msl_premium::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    from wks_ph_non_ise_shm
    where encoded_report <> '1'

),
super_8 as(
    select 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        null::varchar(10) as msl_large,
        null::varchar(10) as msl_small,
        null::varchar(10) as msl_premium,
        msl_sup_hybrid::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    FROM wks_ph_non_ise_super_8
    WHERE encoded_report <> '1' 
),
svi_smc as(
    SELECT DISTINCT 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        msl_large::varchar(10) as msl_large,
        msl_small::varchar(10) as msl_small,
        msl_premium::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    FROM wks_ph_non_ise_svi_smc
    WHERE encoded_report <> '1'
),
waltermart as (
    SELECT DISTINCT 
        ret_nm_prefix::varchar(50) as ret_nm_prefix,
        sku_code::varchar(50) as sku_code,
        brand::varchar(500) as brand,
        barcode::varchar(100) as barcode,
        item_description::varchar(500) as item_description,
        msl_large::varchar(10) as msl_large,
        msl_small::varchar(10) as msl_small,
        msl_premium::varchar(10) as msl_premium,
        null::varchar(10) as msl_dept,
        month::varchar(10) as month,
        week::varchar(10) as week,
        reason::varchar(500) as reason,
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
        crtd_dttm::timestamp_ntz(9) as crtd_dttm
    FROM wks_ph_non_ise_waltermart
    WHERE encoded_report <> '1'

),
transformed as(
    select * from landmark_sm
    union all
    select * from landmark_ds
    union all
    select * from puregold
    union all
    select * from robinsons_ds
    union all
    select * from robinsons_sm
    union all
    select * from rustans
    union all
    select * from shm
    union all
    select * from super_8
    union all
    select * from svi_smc
    union all
    select * from waltermart
)
select * from transformed