{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

with source as (
     select * from {{ source('ntasdl_raw','sdl_tsi_target_data') }}
),     
final as (
    select
        date,
        ec,
        offtake,
        customer_code,
        customer_name,
        customer_cname,
        customer_sname,
        nts,
        "offtake(sell_out)",
        gts,
        pre_sales,
        prs_code_01,
        prs_code_02,
        prs_code_03,
        prs_code_04,
        prs_code_05,
        null as filename,
        null as crt_dttm
    from source
 {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where source.crt_dttm > (select max(crt_dttm) from {{ this }}) 
 {% endif %}
)

select * from final