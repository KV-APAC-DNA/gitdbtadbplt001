{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['trans_dt','brnch_no','ctgry_cd']
    )
}}

with source as(
    select * from {{ source('thasdl_raw','sdl_th_tesco_transdata') }}
),
final as 
(
    select
        cast(ir_date as timestamp_ntz(9)) as trans_dt,
        article_id::float as tpn,
        warehouse::varchar(50) as brnch_no,
        stock::float as invt_qty,
        sales::float as sls_qty,
        sales_amount::float as sls_baht,
        eansku::varchar(50) as upc,
        spn::varchar(50) as sap_cd,
        supplier_id::varchar(20) as ctgry_cd,
        replace(ir_date,'-','')::varchar(20) as dt_cd,
        file_name::varchar(300) as file_nm,
        folder_name::varchar(100) as folder_nm,
        current_timestamp()::timestamp_ntz(9) as input_dt,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
)

select * from final