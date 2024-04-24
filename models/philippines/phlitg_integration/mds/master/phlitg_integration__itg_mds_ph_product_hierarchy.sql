{{
    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="delete from {{this}} where 0 != (select count(*) from {{source('phlsdl_raw','sdl_mds_ph_product_hierarchy')}})"
    )
}}
with source as
(
    select * from {{source('phlsdl_raw','sdl_mds_ph_product_hierarchy')}}
),
final as
(
    select 
        hierarchy::varchar(100) as hierarchy,
        root::varchar(10) as root,
        franchisecd_id::varchar(10) as franchise_id,
        franchisecd_code::varchar(50) as franchise_cd,
        franchisecd_name::varchar(255) as franchise_nm,
        brandcd_id::varchar(10) as brand_id,
        brandcd_code::varchar(50) as brand_cd,
        brandcd_name::varchar(255) as brand_nm,
        variantcd_id::varchar(10) as variant_id,
        variantcd_code::varchar(50) as variant_cd,
        variantcd_name::varchar(255) as variant_nm,
        ph_ref_repvarputup_id::varchar(10) as ph_ref_repvarputup_id,
        ph_ref_repvarputup_code::varchar(50) as ph_ref_repvarputup_cd,
        ph_ref_repvarputup_name::varchar(255) as ph_ref_repvarputup_nm,
        current_timestamp::timestamp_ntz(9) as crtd_dttm,
        current_timestamp::timestamp_ntz(9) as updt_dttm
    from source  
)
select * from final