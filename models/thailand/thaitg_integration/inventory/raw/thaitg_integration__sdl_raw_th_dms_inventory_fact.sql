
{{ 
    config(
        materialized = "incremental", 
        incremental_strategy = "append"
        ) 
    }}
WITH source AS (
        SELECT *
        FROM {{ source('thasdl_raw', 'sdl_th_dms_inventory_fact') }} source
        WHERE SOURCE_FILE_NAME NOT IN (
                SELECT DISTINCT file_name
                FROM {{ source('thawks_integration', 'TRATBL_sdl_th_dms_inventory_fact__test_date_format') }}
                )
        ),
    final AS (
        SELECT *
        FROM source { % IF is_incremental() % }
            -- this filter will only be applied on an incremental run
            WHERE curr_date > (
                SELECT max(curr_date)
                FROM {{ this }}
                ) { % endif % } )


         SELECT *
            FROM final
    
