{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with 
source as
(
    select * from {{ ref('ntaitg_integration__sdl_tw_ims_dstr_std_customer') }} 
),

final as
(
    select
        distributor_code,
        distributor_cusotmer_code,
        distributor_customer_name,
        distributor_address,
        distributor_telephone,
        distributor_contact,
        distributor_sales_area,
        crt_dttm,
        updt_dttm as upd_dttm,
        null as filename,
        null as run_id
    from source
    {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where crt_dttm > (select max(crt_dttm) from {{ this }}) 
{% endif %} 
)

select * from final