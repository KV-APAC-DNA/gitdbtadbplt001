{{
    config(
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook=" {% if is_incremental() %}
                delete from {{this}} where TO_CHAR(TO_DATE(upload_dt, 'MM-DD-YYYY'), 'YYYY-MM-DD') < TO_CHAR(current_timestamp(), 'YYYY-MM-DD');
                {% endif %}"    )
}}

with jp_pos_daily_dnki as(
    select *, dense_rank() over (partition by upload_dt order by source_file_date desc) rn
    from {{ source('jpnsdl_raw', 'jp_pos_daily_dnki') }} qualify rn=1
),
final as(
    select
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
)
select * from final