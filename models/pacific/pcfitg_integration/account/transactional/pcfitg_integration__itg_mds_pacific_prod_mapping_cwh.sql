{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        unique_key=  ['cwh_prodid','matl_num'],
        pre_hook= "delete from {{this}} where (cwh_prodid||matl_num)
in (Select distinct code||jnj_sap_code from {{ source('pcfsdl_raw', 'sdl_mds_pacific_prod_mapping_cwh') }});"
    )
}}

with source as 
(
    select * from {{ source('pcfsdl_raw', 'sdl_mds_pacific_prod_mapping_cwh') }}
),
final as 
(
    select distinct
        code::varchar(500) as cwh_prodid,
        jnj_sap_code::number(28,0) as matl_num,
        description::varchar(200) as matl_desc
    from source
)
select * from final