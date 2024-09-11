--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_mds_mother_code') }}
),

--Logical CTE
itg_mds_master_mother_code_mapping as  
(
    select market_code as market,
       code as sku_unique_identifier, 
       unique_sku_identifer_mapping_code,
       brand,
       description,
       mother_description,
       mother_code,
       msl_flag_code,
       msl_flag_name
     FROM source
),

--Final CTE
final as 
(
    select
    market :: varchar(500) as market,
       sku_unique_identifier :: varchar(500) as sku_unique_identifier,
       unique_sku_identifer_mapping_code :: varchar(500) as unique_sku_identifer_mapping_code,
       brand :: varchar(200) as brand,
       description::varchar(200) as description,
       mother_description::varchar(200) as mother_description,
       mother_code ::varchar(200) as mother_code,
       msl_flag_code :: varchar(500) as msl_flag_code,
       msl_flag_name :: varchar(500) as msl_flag_name
    from itg_mds_master_mother_code_mapping
)

--Final select
select * from final 