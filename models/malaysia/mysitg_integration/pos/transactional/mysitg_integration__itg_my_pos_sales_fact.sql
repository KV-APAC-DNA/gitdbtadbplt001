{{
    config(
        materialized="incremental",
        incremental_strategy= "delete+insert",
        unique_key=  ['cust_id','jj_mnth_id'],
        sql_header="USE WAREHOUSE "+ env_var("DBT_ENV_CORE_DB_MEDIUM_WH")+ ";"
    )
}}

--import CTE
with source as (
    select * from {{ source('myssdl_raw','sdl_my_pos_sales_fact') }}
),

final as (
    select 
        coalesce(cust_id,'')::varchar(50) as cust_id,
        cust_nm::varchar(255) as cust_nm,
        store_cd::varchar(50) as store_cd,
        store_nm::varchar(255) as store_nm,
        dept_cd::varchar(50) as dept_cd,
        dept_nm::varchar(255) as dept_nm,
        mt_item_cd::varchar(50) as mt_item_cd,
        mt_item_desc::varchar(255) as mt_item_desc,
        coalesce(replace(jj_mnth_id, '/', ''),'')::varchar(10) as jj_mnth_id,
        jj_yr_week_no::varchar(10) as jj_yr_week_no,
        cast(qty as decimal(15, 6)) as qty,
        cast(so_val as decimal(15, 6)) as so_val,
        sap_matl_num::varchar(50) as sap_matl_num,
        file_nm::varchar(50) as file_nm,
        cdl_dttm::varchar(50) as cdl_dttm,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        current_timestamp()::timestamp_ntz(9) as updt_dttm
    from source
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
        where source.curr_dt > (select max(crtd_dttm) from {{ this }}) 
        and source.file_nm not in (select distinct file_nm from {{ this }})
    {% endif %}
)

select * from final