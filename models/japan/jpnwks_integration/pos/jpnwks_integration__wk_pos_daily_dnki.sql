{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook=" {% if is_incremental() %}
                delete from {{this}} where TO_CHAR(TO_DATE(upload_dt, 'MM-DD-YYYY'), 'YYYY-MM-DD') < TO_CHAR(current_timestamp(), 'YYYY-MM-DD');
                {% endif %}"    )
}}

with jp_pos_daily_dnki as(
    select * from {{ source('jpnsdl_raw', 'jp_pos_daily_dnki') }}
),
final as(
    select distinct
        store_key_1,
        store_key_2,
        jan_code,
        product_name,
        accounting_date,
        quantity,
        amount,
        account_key,
        source_file_date,
        upload_dt,
        upload_time
    from jp_pos_daily_dnki
    {% if is_incremental() %}
        -- this filter will only be applied on an incremental run
    where TO_DATE(jp_pos_daily_dnki.accounting_date, 'YYYYMMDD') > (select max(TO_DATE(accounting_date, 'YYYYMMDD')) from {{this}}) 
    {% endif %}
)
select * from final
