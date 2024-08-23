
{{ 
    config(
        materialized = "incremental", 
        incremental_strategy = "append"
        ) 
    }}
WITH source AS (
        SELECT *,
            dense_rank() OVER (
                PARTITION BY dstrbtr_grp_cd,
                invoice_dt ORDER BY file_name DESC
                ) AS rnk
        FROM {{ source('thasdl_raw', 'sdl_th_dms_inventory_fact') }} source
        WHERE file_name NOT IN (
                SELECT DISTINCT file_name
                FROM {{ source('thawks_integration', 'TRATBL_sdl_th_dms_inventory_fact__test_date_format') }}
                ) qualify rnk = 1
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
    
