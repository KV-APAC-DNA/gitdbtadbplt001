
{{
    config
    (
        materialized ='incremental',
        incremental_strategy = 'append',
        pre_hook = "{% if is_incremental() %}
        delete from {{this}} where file_rec_dt = to_date(convert_timezone('UTC', current_timestamp())) and ctry_cd = 'HK' and dstr_cd = '110256';
        {% endif %}"
    )
}}


with source as
(
    select * from {{ source ( 'ntasdl_raw', 'sdl_mds_hk_wingkeung_gt_msl_items')}}
),

union1 as
(
        SELECT 
            'HK' as ctry_cd,
            '110256' as dstr_cd,
            brand as brand,
            wk_code as dstr_prod_cd,
            sap_code as sap_matl_cd,
            prod_desc_eng as prod_desc_eng,
            prod_desc_chnse as prod_desc_chnse,
            'A' as store_class,
            store_class_a as msl_flg,
            cast(strt_yr_mnth as integer) as strt_yr_mnth,
            cast(nullif(end_yr_mnth, '') as integer) as end_yr_mnth,
            to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt
        FROM source
),

union2 as
(
        SELECT 
            'HK' as ctry_cd,
            '110256' as dstr_cd,
            brand as brand,
            wk_code as dstr_prod_cd,
            sap_code as sap_matl_cd,
            prod_desc_eng as prod_desc_eng,
            prod_desc_chnse as prod_desc_chnse,
            'B' as store_class,
            store_class_b as msl_flg,
            cast(strt_yr_mnth as integer) as strt_yr_mnth,
            cast(nullif(end_yr_mnth, '') as integer) as end_yr_mnth,
            to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt
        FROM source
),

union3 as
(
         SELECT 
            'HK' as ctry_cd,
            '110256' as dstr_cd,
            brand as brand,
            wk_code as dstr_prod_cd,
            sap_code as sap_matl_cd,
            prod_desc_eng as prod_desc_eng,
            prod_desc_chnse as prod_desc_chnse,
            'C' as store_class,
            store_class_c as msl_flg,
            cast(strt_yr_mnth as integer) as strt_yr_mnth,
            cast(nullif(end_yr_mnth, '') as integer) as end_yr_mnth,
            to_date(convert_timezone('UTC', current_timestamp())) as file_rec_dt
        FROM source
),

transformed as
(
    select * from union1
    union all
    select * from union2
    union all
    select * from union3
)

select * from transformed
