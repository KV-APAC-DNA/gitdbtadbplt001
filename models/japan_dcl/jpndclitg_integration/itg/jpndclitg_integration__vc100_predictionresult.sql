{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        unique_key= ["customer_id"]
       
    )
}}

with vc100_predictionresult as (
    select * from {{source('jpdclsdl_raw','vc100_predictionresult')}}
),
transformed as (
SELECT nvl(lpad(customer_id, 10, '0'), '0000000000') as customer_id,
    vc100propensity,
    'Src_File_Dt' as source_file_date
FROM vc100_predictionresult   
),
final as (
select
customer_id::varchar(60) as customer_id,
vc100propensity::varchar(60) as vc100propensity,
source_file_date::varchar(30) as source_file_date,
sysdate()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
null::timestamp_ntz(9) as updated_date,
null::varchar(9) as updated_by
from transformed
)
select * from final
