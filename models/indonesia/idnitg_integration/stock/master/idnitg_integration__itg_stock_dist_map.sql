{{    config
    (
        materialized="incremental",
        incremental_strategy="append",
        pre_hook="
        {% if is_incremental() %}
            delete from {{this}} where (TRIM(jj_mnth_id),dstrbtr_cd) IN (
                                        SELECT DISTINCT TRIM(jj_mnth_id),dstrbtr_cd
                                        FROM {{ source('idnsdl_raw', 'sdl_stock_dist_map') }}
                                    );
        {% endif %}"
    )
}}

with source as (
    select * from {{ source('idnsdl_raw', 'sdl_stock_dist_map') }}
),
final as (
    select 
        dstrbtr_cd::varchar(20) as dstrbtr_cd,
        stock_dt::timestamp_ntz(9) as stock_dt,
        dstrbtr_id::varchar(20) as dstrbtr_id,
        dstrbtr_prod_id::varchar(255) as dstrbtr_prod_id,
        git::number(18,4) as git,
        tot_stock::number(18,4) as tot_stock,
        stock_key::varchar(100) as stock_key,
        stock_val::number(18,4) as stock_val,
        stock_niv::number(18,4) as stock_niv,
        jj_sap_prod_id::varchar(25) as jj_sap_prod_id,
        jj_sap_dstrbtr_id::varchar(20) as jj_sap_dstrbtr_id,
        quarter::varchar(10) as quarter,
        jj_wk::varchar(4) as jj_wk,
        jj_mnth_id::varchar(10) as jj_mnth_id,
        jj_year::varchar(10) as jj_year,
        yearmonth::varchar(10) as yearmonth,
        week_2::varchar(4) as week_2,
        filename::varchar(255) as file_name,
        run_id::number(14,0) as run_id,
        current_timestamp()::timestamp_ntz(9) as crtd_dttm,
        saleable_stock_qty::number(18,4) as saleable_stock_qty,
        saleable_stock_value::number(18,4) as saleable_stock_value,
        non_saleable_stock_qty::number(18,4) as non_saleable_stock_qty,
        non_saleable_stock_value::number(18,4) as non_saleable_stock_value
from source
)

select * from final