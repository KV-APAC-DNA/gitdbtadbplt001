{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['filename']
    )
}}
with source as
(
    select * from {{ source('vnmsdl_raw', 'sdl_vn_mt_sellout_bhx') }}
),
transformed as
(
    SELECT 
        SUBSTRING(RIGHT (filename, 11), 4, 4) AS year,
        case
            when substring(right (filename, 11), 1, 3) like '%Jan%' then '01'
            when substring(right (filename, 11), 1, 3) like '%Feb%' then '02'
            when substring(right (filename, 11), 1, 3) like '%Mar%' then '03'
            when substring(right (filename, 11), 1, 3) like '%Apr%' then '04'
            when substring(right (filename, 11), 1, 3) like '%May%' then '05'
            when substring(right (filename, 11), 1, 3) like '%Jun%' then '06'
            when substring(right (filename, 11), 1, 3) like '%Jul%' then '07'
            when substring(right (filename, 11), 1, 3) like '%Aug%' then '08'
            when substring(right (filename, 11), 1, 3) like '%Sep%' then '09'
            when substring(right (filename, 11), 1, 3) like '%Oct%' then '10'
            when substring(right (filename, 11), 1, 3) like '%Nov%' then '11'
            when substring(right (filename, 11), 1, 3) like '%Dec%' then '12'
        END AS month,
    pro_code as pro_code,
    pro_name as pro_name,
    cust_code as cust_code,
    cust_name as cust_name,
    cat_store as cat_store,
    quantity as quantity,
    amount as amount,
    filename as filename,
    run_id as run_id,
    crtd_dttm as crtd_dttm,
    current_timestamp() as updt_dttm
    from source
),
final as
(
    select 
    year::varchar(20) as year,
    month::varchar(20) as month,
    pro_code::varchar(50) as pro_code,
    pro_name::varchar(200) as pro_name,
    cust_code::varchar(20) as cust_code,
    cust_name::varchar(200) as cust_name,
    cat_store::varchar(60) as cat_store,
    quantity::number(18,0) as quantity,
    amount::number(20,5) as amount,
    filename::varchar(100) as filename,
    run_id::number(14,0) as run_id,
    crtd_dttm::timestamp_ntz(9) as crtd_dttm,
    updt_dttm::timestamp_ntz(9) as updt_dttm
    from transformed
)
select * from final
