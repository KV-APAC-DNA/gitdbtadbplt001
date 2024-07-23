--Import CTE
with source as (
    select * from {{ source('aspsdl_raw', 'sdl_mds_mds_reds_market_msl') }}
),

--Logical CTE
itg_re_msl_input_definition as  
(
    SELECT TO_CHAR ((start_ddmmyyyy :: DATE),'DD/MM/YYYY') AS start_date,
       TO_CHAR ((end_ddmmyyyy :: DATE),'DD/MM/YYYY') AS end_date,
       market,
       region,
       zone,
       retail_environment_code AS retail_environment,
       channel,
       sub_channel,
       customer,
       store_grade_code AS store_grade,
       unique_identifier_mapping_code AS unique_identifier_mapping,
       sku_unique_identifier,
       sku_description,
       sku_code,
       product_key,
	   msl_final,
	   active_status_code,
	   sourceexistenceflag_code
    FROM source
),

--Final CTE
final as 
(
    select
    start_date :: varchar(50) as start_date,
    end_date :: varchar(50) as end_date,
    market :: varchar(50) as market,
    region :: varchar(20) as region,
    zone :: varchar(20) as zone,
    retail_environment :: varchar(50) as retail_environment,
    channel :: varchar(50) as channel,
    sub_channel :: varchar(50) as sub_channel,
    customer :: varchar(50) as customer,
    store_grade :: varchar(20) as store_grade,
    unique_identifier_mapping :: varchar(50) as unique_identifier_mapping,
    sku_unique_identifier :: varchar(100) as sku_unique_identifier,
    sku_description :: varchar(500) as sku_description,
    sku_code :: varchar(50) as sku_code,
    product_key :: varchar(500) as product_key,
    msl_final :: varchar(200) as msl_final,
    active_status_code :: varchar(20) as active_status_code,
    sourceexistenceflag_code :: varchar(20) as sourceexistenceflag_code
    from itg_re_msl_input_definition
)

--Final select
select * from final 