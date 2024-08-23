{{
    config(
        materialized="incremental",
        incremental_strategy="append"
    )}}

WITH source
AS (
    SELECT *
    FROM {{ source('thasdl_raw', 'sdl_th_dms_customer_dim') }} source
    WHERE file_name NOT IN (
            SELECT DISTINCT file_name
            FROM {{ source('thawks_integration', 'TRATBL_sdl_th_dms_customer_dim__duplicate_test') }}
            )
    ),
    
final as(
    select * from source
     {% if is_incremental() %}
    -- this filter will only be applied on an incremental run
    where curr_date > (select max(curr_date) from {{ this }}) 
 {% endif %}
)
select * from final