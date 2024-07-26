{{ config(materialized="incremental", incremental_strategy="append") }}


with
    ocl_in_userlist_v as (
        select * from dev_dna_core.snapjpdcledw_integration.ocl_in_userlist_v
    ),

    transformed as (
        select file_id, filename, current_timestamp() as insertdate,

        -- CONVERT_TIMEZONE('Asia/Tokyo', current_timestamp()) AS insertdate
        from ocl_in_userlist_v
        {% if is_incremental() %}
            -- this filter will only be applied on an incremental run
            where
                ocl_in_userlist_v.insertdate > (select max(insertdate) from {{ this }})
        {% endif %}
    ),

    final as (select * from transformed)

select *
from final
