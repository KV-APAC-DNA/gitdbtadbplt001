{{
    config(
        materialized="incremental",
        incremental_strategy="append"
)}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_kr_pos_emart_ssg') }} 
     where filename not in (
            select distinct file_name from {{ source('ntawks_integration', 'TRATBL_sdl_kr_pos_emart_ssg__lookup_test') }}
     )
),
final as (
    select
        str_nm,
        str_cd,
        team_nm,
        lrg_classification_nm,
        mid_classification_nm,
        sub_classified_nm,
        offline_ean,
        ean,
        prod_nm,
        pos_dt,
        sellout_qty,
        sellout_amt,
        suppliers,
        product_type,
        crtd_dttm,
        filename 
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.crtd_dttm > (select max(crtd_dttm) from {{ this }}) 
    {% endif %}
)
select * from final