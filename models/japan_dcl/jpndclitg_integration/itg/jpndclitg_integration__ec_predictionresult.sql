{{
    config
    (
        materialized="incremental",
        incremental_strategy= "append",
        pre_hook ="UPDATE {{ ref('jpndclitg_integration__ec_predictionresult_1') }}
                SET customer_id = substring(customer_id, 1, length(customer_id) - 2)
                WHERE customer_id LIKE '%99'"     
                    )
                }}

with ec_predictionresult as (
    select * from {{ ref('jpndclitg_integration__ec_predictionresult_1') }}
),
transformed as (
SELECT
    customer_id,
    ECPropensity,
    'Src_File_Dt' as source_file_date
FROM ec_predictionresult 
),
final as (
select
customer_id::varchar(60) as customer_id,
ECPropensity::varchar(60) as ECPropensity,
source_file_date::varchar(30) as source_file_date,
sysdate()::timestamp_ntz(9) as inserted_date,
null::varchar(100) as inserted_by,
null::timestamp_ntz(9) as updated_date,
null::varchar(9) as updated_by
from transformed
)
select * from final