{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook="delete from {{this}} where (to_date(trans_dt),ltrim(brnch_no,0),ltrim(ctgry_cd,0)) 
                    IN (select to_date(ir_date),ltrim(warehouse,0),ltrim(supplier_id,0) 
                    FROM {{ source('thasdl_raw', 'sdl_th_tesco_transdata') }})"
    )
}}

with source as(
    select *, dense_rank() over(partition by to_date(ir_date), ltrim(warehouse,0), ltrim(supplier_id,0) order by file_name desc) as rnk from {{ source('thasdl_raw','sdl_th_tesco_transdata') }}
),
final as 
(
    select
        cast(ir_date as date) as trans_dt,
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
    where rnk=1
)

select * from final