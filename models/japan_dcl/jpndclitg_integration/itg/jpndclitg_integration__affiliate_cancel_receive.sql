{{ config(materialized="incremental", incremental_strategy="append") }}

with
    source as (select * from dev_dna_load.snapjpdclsdl_raw.affiliate_cancel_receive),

    transformed as (
        select
            achievement,
            click_dt,
            accrual_dt,
            asp,
            unique_id,
            media_name,
            asp_control_no,
            sale_num,
            amount_including_tax,
            amount_excluded_tax,
            orderdate,
            webid,
            current_timestamp() as inserted_date,
            current_timestamp() as updated_date
        from source
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            where source.inserted_date > (select max(inserted_date) from {{ this }})
        {% endif %}
    )

select *
from transformed
