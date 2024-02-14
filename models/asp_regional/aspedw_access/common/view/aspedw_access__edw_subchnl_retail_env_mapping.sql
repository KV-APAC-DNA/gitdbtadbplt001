with source as(
    select * from {{ source('aspedw_integration', 'edw_subchnl_retail_env_mapping') }}
),
final as(
  select 
        sub_channel as "sub_channel",
        retail_env as "retail_env"
        from source
)

select * from final