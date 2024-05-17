{{
    config(
        materialized="incremental",
        incremental_strategy = "merge",
        unique_key=["c_pk"],
        merge_exclude_columns = ["crt_dttm"]
    )
}}

with source as(
    select * from DEV_DNA_CORE.SNAPNTAWKS_INTEGRATION.WKS_itg_tw_ims_dstr_prod_map
),
final as(
    select
        MD5(
                CONCAT
                    (
                       COALESCE(dstr_product_code,'#'),
                       COALESCE(dstr_product_name,'#'),
                       dstr_code
                    )       
                ) as c_pk,
        dstr_code::varchar(10) as dstr_cd,
        dstr_name::varchar(20) as dstr_nm,
        dstr_product_code::varchar(20) as dstr_prod_cd,
        dstr_product_name::varchar(100) as dstr_prod_nm,
        ean_code::varchar(20) as ean_cd,
        current_timestamp()::timestamp_ntz(9) crt_dttm,
        current_timestamp()::timestamp_ntz(9) AS updt_dttm 
    from source
)
select * from final
