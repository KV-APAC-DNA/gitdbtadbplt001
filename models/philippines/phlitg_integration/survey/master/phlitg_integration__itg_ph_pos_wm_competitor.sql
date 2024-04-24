{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        unique_key=["vendor_cd","jj_mnth_id","brnch_cd","item_cd"],
        pre_hook="delete from {{this}} where ltrim(vendor_cd, '0') || jj_mnth_id || ltrim(brnch_cd, '0') || ltrim(item_cd, '0') in (select distinct ltrim(vendor_cd, '0') || jj_mnth_id || ltrim(store_cd, '0') || ltrim(pos_prod_cd, '0')from {{ source('phlsdl_raw', 'sdl_ph_pos_waltermart') }});"

    )
}}
with source as (
    select * from {{ source('phlsdl_raw', 'sdl_ph_pos_waltermart') }}
),
final as (
    select
        jj_mnth_id::varchar(30) as jj_mnth_id,
        store_cd::varchar(50) as brnch_cd,
        store_nm::varchar(100) as brnch_nm,
        sclass::varchar(30) as brand_ctgry_cd,
        category_nm::varchar(100) as brand_ctgry_nm,
        vendor_cd::varchar(30) as vendor_cd,
        vendor_nm::varchar(100) as vendor_nm,
        pos_prod_cd::varchar(50) as item_cd,
        pos_prod_nm::varchar(100) as item_nm,
        cast(qty as numeric(20, 4)) as pos_qty,
        cast(amt as numeric(20, 4)) as pos_gts,
        null::varchar(150) as file_nm,
        cdl_dttm::varchar(50) as cdl_dttm,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source
    where rnk = 1
)
select * from final

