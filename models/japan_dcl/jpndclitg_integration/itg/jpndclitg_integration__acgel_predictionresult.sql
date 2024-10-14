{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["customer_id"]
       
    )
}}

with acgel_predictionresult as (
  
    select * from {{source('jpdclsdl_raw','acgel_predictionresult')}}
),
transformed as (
SELECT NVL(LPAD(customer_id, 10, '0'), '0000000000') as customer_id,
    acgelpropensity,
    source_file_date as source_file_date
FROM acgel_predictionresult   
),
final as (
select
customer_id::varchar(60) as customer_id,
acgelpropensity::varchar(60) as acgelpropensity,
source_file_date::varchar(30) as source_file_date,
current_timestamp()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
null::timestamp_ntz(9) as updated_date,
null::varchar(9) as updated_by
from transformed
)
select * from final
